from datetime import timedelta
import socket
from enum import Enum
from secrets import token_hex
from threading import Condition
from time import monotonic

from . import commands, constants
from .codec import parse_cmd
from .data_types import RespArray, RespBulkString
from .database import Database
from .logs import logger
from .singleton_meta import SingletonMeta


class ReplicaHandler(metaclass=SingletonMeta):
    class ReplicaHandshakeState(Enum):
        """Current state of the replica master handshake"""

        READY = 0
        """Ready to begin handshake"""
        PING = 1
        REPLCONF_PORT = 2
        REPLCONF_CAPA_PSYNC = 3
        PSYNC = 4
        DONE = 5

    def __init__(
        self,
        is_master: bool,
        ip: str,
        port: int,
        replica_of: tuple[str, int] | None,
        db: Database,
    ):
        self.is_master = is_master
        self.ack_count = 0
        self.ack_cv = Condition()
        # self.id = str(uuid.uuid4())
        self.ip = ip
        self.port = port
        if replica_of:
            self.master_ip = replica_of[0]
            self.master_port = replica_of[1]
        self.slaves: list[socket.socket] = []
        self.connected_slaves = 0
        self.role = "master" if is_master else "slave"
        self.master_replid = token_hex(20) if is_master else "?"
        self.master_repl_offset = 0
        self.handshake_state = ReplicaHandler.ReplicaHandshakeState.READY
        self.db = db

    def incr_ack_count(self):
        with self.ack_cv:
            logger.info(f"incrementing {self.ack_count=} to {self.ack_count + 1}")
            self.ack_count += 1
            self.ack_cv.notify_all()

    def wait(self, replica_count: int, timeout: timedelta):
        now = monotonic()
        end = now + timeout.total_seconds()
        with self.ack_cv:
            self.ack_count = 0
        self.propogate(
            RespArray(
                [
                    RespBulkString(b"REPLCONF"),
                    RespBulkString(b"GETACK"),
                    RespBulkString(b"*"),
                ]
            ).encode()
        )
        logger.info("finished sending to all slaves")
        with self.ack_cv:
            while self.ack_count < replica_count and monotonic() < end:
                self.ack_cv.wait(timeout=max(0, end - monotonic()))

        logger.info(f"{self.ack_count=}, {monotonic() - end=} (should be positive)")
        return self.ack_count

    def master_recv_loop(self):
        while self.handshake_state != ReplicaHandler.ReplicaHandshakeState.DONE:
            self.connect_to_master()
        while True:
            logger.info("replica waiting for master...")
            data = self.master_conn.recv(constants.BUFFER_SIZE)
            logger.info(f"replica from master: raw {data=}")
            if not data:
                logger.info("EOF/no data received, replica breaking")
                break
            cmds = parse_cmd(data)
            logger.info(f"replica {cmds=}")
            self._execute_cmds(cmds)

    def connect_to_master(self):
        """Performs the master slave handshake. Has the side effect of executing any commands that come directly after the handshake (there should be none)."""
        # if we get more cases, can use a table instead of hardcoding cases
        match self.handshake_state:
            case ReplicaHandler.ReplicaHandshakeState.READY:
                master_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                master_conn.settimeout(constants.CONN_TIMEOUT)
                master_conn.connect((self.master_ip, int(self.master_port)))
                self.master_conn = master_conn
                self.handshake_state = ReplicaHandler.ReplicaHandshakeState.PING
            case ReplicaHandler.ReplicaHandshakeState.PING:
                self.master_conn.sendall(commands.craft_command("PING").encode())
                self._expect_handshake(b"+PONG\r\n")
                self.handshake_state = (
                    ReplicaHandler.ReplicaHandshakeState.REPLCONF_PORT
                )
            case ReplicaHandler.ReplicaHandshakeState.REPLCONF_PORT:
                self.master_conn.sendall(
                    commands.craft_command(
                        "REPLCONF", "listening-port", str(self.port)
                    ).encode()
                )
                self._expect_handshake(constants.OK_SIMPLE_RESP_STRING.encode())
                self.handshake_state = (
                    ReplicaHandler.ReplicaHandshakeState.REPLCONF_CAPA_PSYNC
                )
            case ReplicaHandler.ReplicaHandshakeState.REPLCONF_CAPA_PSYNC:
                self.master_conn.sendall(
                    commands.craft_command("REPLCONF", "capa", "psync2").encode()
                )
                self._expect_handshake(constants.OK_SIMPLE_RESP_STRING.encode())
                self.handshake_state = ReplicaHandler.ReplicaHandshakeState.PSYNC
            case ReplicaHandler.ReplicaHandshakeState.PSYNC:
                self.master_conn.sendall(
                    commands.craft_command(
                        "PSYNC", self.master_replid, str(-1)
                    ).encode()
                )
                # expect any rdb file
                data = self.master_conn.recv(constants.BUFFER_SIZE)
                cmds = parse_cmd(data)
                logger.info(f"handshake {cmds=}")

                # receive the FULLRESYNC command
                if not cmds or not isinstance(cmds[0], commands.FullResyncCommand):
                    raise Exception(f"replica expected FullResyncCommand, got {data}")
                # until we write a parser with a buffer, hack it
                if len(cmds) >= 2:
                    if not isinstance(cmds[1], commands.RdbFileCommand):
                        raise Exception(f"replica expected RDB file, got {data}")
                    rdb_cmd = cmds[1]
                    rest_cmds = cmds[2:]
                else:
                    data = self.master_conn.recv(constants.BUFFER_SIZE)
                    cmds = parse_cmd(data)
                    if not cmds or not isinstance(cmds[0], commands.RdbFileCommand):
                        raise Exception(f"replica expected RDB file, got {data}")
                    rdb_cmd = cmds[0]
                    rest_cmds = cmds[1:]
                # receive the RDB file, then leave the rest to the main loop
                # initialise DB with the RDB file
                rdb_cmd.execute(self.db, self, self.master_conn)
                self._execute_cmds(rest_cmds)
                logger.info("connect_to_master_sm complete")
                self.handshake_state = ReplicaHandler.ReplicaHandshakeState.DONE

    def _execute_cmds(self, cmds: list[commands.Command]):
        for cmd in cmds:
            executed = cmd.execute(self.db, self, self.master_conn)
            if isinstance(cmd, commands.ReplConfGetAckCommand):
                for resp in executed:
                    logger.info(f"responding to master: {resp}")
                    self.master_conn.sendall(resp)
            # need to update offset based on cmd in list, not based on full data
            self.master_repl_offset += len(cmd.raw_cmd)

    def _expect_handshake(self, expected: bytes):
        """Receives data from master and asserts that the response is expected, based on self.handshake_state"""
        data = self.master_conn.recv(len(expected))
        if data != expected:
            raise Exception(f"replica expected {expected}, got {data}")
        return data

    def add_slave(self, slave: socket.socket):
        self.slaves.append(slave)
        self.connected_slaves += 1

    def propogate(self, raw_cmd: bytes):
        for slave in self.slaves:
            slave.sendall(raw_cmd)
        self.master_repl_offset += len(raw_cmd)

    def get_info(self) -> list[bytes]:
        """Encodes each kv as a RespBulkString"""
        info = {
            "role": self.role,
            "connected_slaves": self.connected_slaves,
            "master_replid": self.master_replid,
            "master_repl_offset": self.master_repl_offset,
        }
        return RespBulkString(
            b"".join(
                map(
                    lambda item: RespBulkString(
                        f"{item[0]}:{item[1]}".encode()
                    ).encode(),
                    info.items(),
                )
            )
        ).encode_to_list()
