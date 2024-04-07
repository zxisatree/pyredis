import threading
import uuid
import socket

import codec
import commands
import constants
import database
import data_types
from logs import logger
import singleton_meta


class ReplicaHandler(metaclass=singleton_meta.SingletonMeta):
    def __init__(
        self,
        is_master: bool,
        ip: str,
        port: int,
        replica_of: tuple[str, int] | None,
        db: database.Database,
    ):
        self.is_master = is_master
        self.ack_count = 0
        self.id = str(uuid.uuid4())
        self.ip = ip
        self.port = port
        if replica_of is not None:
            self.master_ip = replica_of[0]
            self.master_port = replica_of[1]
        self.slaves: list[socket.socket] = []
        self.connected_slaves = 0
        self.role = "master" if is_master else "slave"
        self.master_replid = self.id if is_master else "?"
        self.master_repl_offset = 0
        # attempt to connect to master
        if not is_master:
            threading.Thread(target=self.connect_to_master, args=(db,)).start()

    def connect_to_master(self, db: database.Database):
        self.master_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_conn.settimeout(constants.CONN_TIMEOUT)
        self.master_conn.connect((self.master_ip, int(self.master_port)))

        self.master_conn.sendall(commands.craft_command("PING").encode())
        data = self.master_conn.recv(constants.BUFFER_SIZE)
        logger.info(f"Replica sent ping, got {data=}")
        # check if we get PONG
        if data != commands.PingCommand(b"").execute(None, None, None):
            logger.info("Failed to connect to master")
            return

        self.master_conn.sendall(
            commands.craft_command(
                "REPLCONF", "listening-port", str(self.port)
            ).encode()
        )
        data = self.master_conn.recv(constants.BUFFER_SIZE)
        logger.info(f"Replica sent REPLCONF 1, got {data=}")
        # check if we get OK
        if data != constants.OK_SIMPLE_RESP_STRING.encode():
            logger.info("Failed to connect to master")
            return

        self.master_conn.sendall(
            commands.craft_command("REPLCONF", "capa", "psync2").encode()
        )
        data = self.master_conn.recv(constants.BUFFER_SIZE)
        logger.info(f"Replica sent REPLCONF 2, got {data=}")
        # check if we get OK
        if data != constants.OK_SIMPLE_RESP_STRING.encode():
            logger.info("Failed to connect to master")
            return

        self.master_conn.sendall(
            commands.craft_command("PSYNC", self.master_replid, str(-1)).encode()
        )
        logger.info(f"Replica sent PSYNC")
        handshake_step = 0

        while True:
            logger.info("Replica waiting for master...")
            data = self.master_conn.recv(constants.BUFFER_SIZE)
            logger.info(f"Replica from master: raw {data=}")
            if not data:
                logger.info("Replica breaking")
                break

            cmds = codec.parse_cmd(data)
            logger.info(f"Replica {cmds=}")
            for cmd in cmds:
                self.respond_to_master(cmd, db)
                if handshake_step != 2:
                    handshake_step = self.handle_handshake_psync(handshake_step, cmd)
                else:
                    # need to update offset based on cmd in list, not based on full data
                    self.master_repl_offset += len(cmd.raw_cmd)

    def handle_handshake_psync(
        self, handshake_step: int, cmd: "commands.Command"
    ) -> int:
        # check if we get FULLRESYNC and RDB file
        if handshake_step == 0 and isinstance(cmd, commands.FullResyncCommand):
            logger.info("Replica got FULLRESYNC")
            return 1
        elif handshake_step == 1 and isinstance(cmd, commands.RdbFileCommand):
            logger.info("Replica got RDB file")
            return 2
        else:
            return handshake_step

    def respond_to_master(self, cmd: "commands.Command", db: database.Database):
        executed = cmd.execute(db, self, self.master_conn)
        if isinstance(cmd, commands.ReplConfGetAckCommand):
            if isinstance(executed, bytes):  # impossible to get list here
                self.master_conn.sendall(executed)

    def add_slave(self, slave: socket.socket):
        self.slaves.append(slave)
        self.connected_slaves += 1

    def propogate(self, raw_cmd: bytes):
        for slave in self.slaves:
            slave.sendall(raw_cmd)

    def get_info(self) -> bytes:
        # encode each kv as a RespBulkString
        info = {
            "role": self.role,
            "connected_slaves": self.connected_slaves,
            "master_replid": self.master_replid,
            "master_repl_offset": self.master_repl_offset,
        }
        return data_types.RespBulkString(
            b"".join(
                map(
                    lambda item: data_types.RespBulkString(
                        f"{item[0]}:{item[1]}".encode()
                    ).encode(),
                    info.items(),
                )
            )
        ).encode()
