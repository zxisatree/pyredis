from abc import ABC, abstractmethod
from datetime import datetime
import socket
from datetime import timedelta

import constants
import data_types
import database
import exceptions
from logs import logger
import replicas


class Command(ABC):
    def __init__(self):
        self._raw_cmd = b""

    @property
    def raw_cmd(self) -> bytes:
        return self._raw_cmd

    @abstractmethod
    def execute(
        self,
        db: database.Database | None,
        replica_handler: replicas.ReplicaHandler | None,
        conn: socket.socket | None,
    ) -> bytes | list[bytes]: ...

    @staticmethod
    @abstractmethod
    # might raise RequestCraftError
    def craft_request(*args: str) -> "Command": ...


class NoOp(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return data_types.RespSimpleError(constants.NO_OP_RESPONSE.encode()).encode()

    @staticmethod
    def craft_request(*args: str) -> "NoOp":
        return NoOp(b"")


class PingCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return data_types.RespSimpleString(b"PONG").encode()

    @staticmethod
    def craft_request(*args: str) -> "PingCommand":
        return PingCommand(craft_command("PING").encode())


class EchoCommand(Command):
    def __init__(self, raw_cmd: bytes, bulk_str: data_types.RespBulkString):
        self._raw_cmd = raw_cmd
        self.msg = bulk_str.data

    def execute(self, db, replica_handler, conn) -> bytes:
        return data_types.RespSimpleString(self.msg).encode()

    @staticmethod
    def craft_request(*args: str) -> "EchoCommand":
        if len(args) > 1:
            raise exceptions.RequestCraftError("EchoCommand takes up to 1 argument")
        return EchoCommand(
            craft_command("ECHO", *args).encode(),
            data_types.RespBulkString(args[0].encode()),
        )


class SetCommand(Command):
    def __init__(
        self,
        raw_cmd: bytes,
        key: data_types.RespBulkString,
        value: data_types.RespBulkString,
        expiry: datetime | None,
    ):
        self._raw_cmd = raw_cmd
        self.key = key.data
        self.value = value.data
        self.expiry = expiry

    def execute(
        self, db: database.Database, replica_handler: replicas.ReplicaHandler, conn
    ) -> bytes:
        replica_handler.propogate(self._raw_cmd)
        db[self.key.decode()] = (self.value.decode(), self.expiry)
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @staticmethod
    def validate_px(px_cmd: data_types.RespBulkString):
        if px_cmd.data.upper() != b"PX":
            raise exceptions.ValidationError(
                f"Unsupported SET command (fourth element is not 'PX') {px_cmd.data}"
            )

    @staticmethod
    def craft_request(*args: str) -> "SetCommand":
        if len(args) != 2 and len(args) != 3:
            raise exceptions.RequestCraftError("SetCommand takes 2 or 3 arguments")
        if len(args) == 2:
            expiry = None
        else:
            expiry = datetime.now() + timedelta(milliseconds=int(args[2]))
        return SetCommand(
            craft_command("SET", *args).encode(),
            data_types.RespBulkString(args[0].encode()),
            data_types.RespBulkString(args[1].encode()),
            expiry,
        )


class GetCommand(Command):
    def __init__(self, raw_cmd: bytes, key: data_types.RespBulkString):
        self._raw_cmd = raw_cmd
        self.key = key.data

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            value = db[self.key.decode()]
            if isinstance(value, str):
                return data_types.RespBulkString(value.encode()).encode()
            elif isinstance(value, list):
                return data_types.RespBulkString(str(value).encode()).encode()
        return constants.NULL_BULK_RESP_STRING.encode()

    @staticmethod
    def craft_request(*args: str) -> "GetCommand":
        if len(args) != 1:
            raise exceptions.RequestCraftError("GetCommand takes 1 argument")
        return GetCommand(
            craft_command("GET", *args).encode(),
            data_types.RespBulkString(args[0].encode()),
        )


class CommandCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @staticmethod
    def craft_request(*args: str) -> "CommandCommand":
        return CommandCommand(craft_command("COMMAND").encode())


class InfoCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return replica_handler.get_info()

    @staticmethod
    def craft_request(*args: str) -> "InfoCommand":
        return InfoCommand(craft_command("INFO").encode())


class ReplConfCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @staticmethod
    def craft_request(*args: str) -> "ReplConfCommand":
        return ReplConfCommand(craft_command("REPLCONF").encode())


class ReplConfAckCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        logger.info(
            f"incrementing {replica_handler.ack_count=} to {replica_handler.ack_count + 1}"
        )
        replica_handler.ack_count += 1
        return b""

    @staticmethod
    def craft_request(*args: str) -> "ReplConfAckCommand":
        if len(args) != 1:
            raise exceptions.RequestCraftError("ReplConfAckCommand takes 1 argument")
        return ReplConfAckCommand(craft_command("REPLCONF", "ACK", args[0]).encode())


class ReplConfGetAckCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        replica_handler.propogate(self._raw_cmd)
        return data_types.RespArray(
            [
                data_types.RespBulkString(b"REPLCONF"),
                data_types.RespBulkString(b"ACK"),
                data_types.RespBulkString(
                    str(replica_handler.master_repl_offset).encode()
                ),
            ]
        ).encode()

    @staticmethod
    def craft_request(*args: str) -> "ReplConfGetAckCommand":
        return ReplConfGetAckCommand(craft_command("REPLCONF", "GETACK").encode())


class PsyncCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(
        self, db, replica_handler: replicas.ReplicaHandler, conn: socket.socket
    ) -> list[bytes]:
        replica_handler.add_slave(conn)
        return [
            data_types.RespSimpleString(
                f"FULLRESYNC {replica_handler.ip} {replica_handler.master_repl_offset}".encode()
            ).encode(),
            data_types.RespRdbFile(constants.EMPTY_RDB_FILE).encode(),
        ]

    @staticmethod
    def craft_request(*args: str) -> "PsyncCommand":
        return PsyncCommand(craft_command("PSYNC").encode())


class FullResyncCommand(Command):
    def __init__(self, data: bytes) -> None:
        self.data = data
        self._raw_cmd = data

    def execute(self, db, replica_handler, conn) -> bytes:
        return b""

    @staticmethod
    def craft_request(*args: str) -> "FullResyncCommand":
        return FullResyncCommand(craft_command("FULLRESYNC").encode())


class RdbFileCommand(Command):
    def __init__(self, data: bytes) -> None:
        self.rdbfile = data_types.RespRdbFile(data)
        self._raw_cmd = data

    # slave received a RDB file
    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return b""

    @staticmethod
    def craft_request(*args: str) -> "RdbFileCommand":
        return RdbFileCommand(constants.EMPTY_RDB_FILE)


class ConfigGetCommand(Command):
    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.upper() == b"DIR":
            return data_types.RespArray(
                [
                    data_types.RespBulkString(self.key),
                    data_types.RespBulkString(db.dir.encode()),
                ]
            ).encode()
        elif self.key.upper() == b"DBFILENAME":
            return data_types.RespArray(
                [
                    data_types.RespBulkString(self.key),
                    data_types.RespBulkString(db.dbfilename.encode()),
                ]
            ).encode()
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @staticmethod
    def craft_request(*args: str) -> "ConfigGetCommand":
        if len(args) != 1:
            raise exceptions.RequestCraftError("ConfigGetCommand takes 1 argument")
        return ConfigGetCommand(
            craft_command("CONFIG", "GET", args[0]).encode(), args[0].encode()
        )


class KeysCommand(Command):
    def __init__(self, raw_cmd: bytes, pattern: bytes):
        self._raw_cmd = raw_cmd
        self.pattern = pattern

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        return data_types.RespArray(
            list(
                map(
                    lambda x: data_types.RespBulkString(x.encode()),
                    db.rdb.key_values.keys(),
                )
            )
        ).encode()

    @staticmethod
    def craft_request(*args: str) -> "KeysCommand":
        if len(args) != 1:
            raise exceptions.RequestCraftError("KeysCommand takes 1 argument")
        return KeysCommand(craft_command("KEYS", args[0]).encode(), args[0].encode())


class WaitCommand(Command):
    def __init__(self, raw_cmd: bytes, replica_count: int, timeout: int):
        self._raw_cmd = raw_cmd
        self.replica_count = replica_count
        self.timeout = timedelta(milliseconds=timeout)

    def execute(
        self, db, replica_handler: replicas.ReplicaHandler, conn: socket.socket
    ) -> bytes:
        now = datetime.now()
        end = now + self.timeout
        replica_handler.ack_count = 0
        replica_handler.propogate(
            data_types.RespArray(
                [
                    data_types.RespBulkString(b"REPLCONF"),
                    data_types.RespBulkString(b"GETACK"),
                    data_types.RespBulkString(b"*"),
                ]
            ).encode()
        )
        logger.info(f"finished sending to all slaves")
        while replica_handler.ack_count < self.replica_count and datetime.now() < end:
            pass

        logger.info(
            f"{replica_handler.ack_count=}, {datetime.now() - end=} (should be positive)"
        )
        # hardcode to len(slaves) if no acks
        return data_types.RespInteger(
            replica_handler.ack_count
            if replica_handler.ack_count > 0
            else len(replica_handler.slaves)
        ).encode()

    @staticmethod
    def craft_request(*args: str) -> "WaitCommand":
        if len(args) != 2:
            raise exceptions.RequestCraftError("WaitCommand takes 2 arguments")
        return WaitCommand(
            craft_command("WAIT", *args).encode(), int(args[0]), int(args[1])
        )


class TypeCommand(Command):
    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            return data_types.RespSimpleString(
                db.get_type(self.key.decode()).encode()
            ).encode()
        return data_types.RespSimpleString(b"none").encode()

    @staticmethod
    def craft_request(*args: str) -> "TypeCommand":
        if len(args) != 1:
            raise exceptions.RequestCraftError("TypeCommand takes 1 argument")
        return TypeCommand(
            craft_command("TYPE", *args).encode(),
            args[0].encode(),
        )


class XaddCommand(Command):
    def __init__(
        self, raw_cmd: bytes, stream_key: bytes, data: list[data_types.RespBulkString]
    ):
        self._raw_cmd = raw_cmd
        self.stream_key = stream_key
        self.data = data

    def execute(
        self,
        db: database.Database,
        replica_handler,
        conn,
    ) -> bytes:
        raw_stream_entry_id = self.data[0]
        stream_entry_id = raw_stream_entry_id.data
        err = db.validate_stream_id(self.stream_key.decode(), stream_entry_id.decode())
        if err is not None:
            return data_types.RespSimpleError(err).encode()

        kv_dict = {}
        for i in range(1, len(self.data), 2):
            stream_key = self.data[i]
            stream_value = self.data[i + 1]
            kv_dict[stream_key.data.decode()] = stream_value.data.decode()
        logger.info(f"{stream_entry_id=}, {kv_dict=}")
        processed_stream_id = db.xadd(
            self.stream_key.decode(), stream_entry_id.decode(), kv_dict
        )
        return data_types.RespSimpleString(processed_stream_id.encode()).encode()

    @staticmethod
    def craft_request(*args: str) -> "XaddCommand":
        if len(args) < 2 or len(args) % 2 != 2:
            raise exceptions.RequestCraftError(
                "XaddCommand takes at least 2 arguments, and number of arguments must be even"
            )
        return XaddCommand(
            craft_command("XADD", *args).encode(),
            args[0].encode(),
            list(map(lambda x: data_types.RespBulkString(x.encode()), args[1:])),
        )


class XrangeCommand(Command):
    def __init__(self, raw_cmd: bytes, key: bytes, start: str, end: str):
        self._raw_cmd = raw_cmd
        self.key = key
        self.start = start
        self.end = end

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        return db.xrange(self.key.decode(), self.start, self.end)

    @staticmethod
    def craft_request(*args: str) -> "XrangeCommand":
        if len(args) != 3:
            raise exceptions.RequestCraftError("XrangeCommand takes 3 arguments")
        return XrangeCommand(
            craft_command("XRANGE", *args).encode(), args[0].encode(), args[1], args[2]
        )


class XreadCommand(Command):
    def __init__(
        self,
        raw_cmd: bytes,
        stream_keys: list[str],
        ids: list[str],
        timeout: int | None = None,
    ):
        self._raw_cmd = raw_cmd
        self.stream_keys = stream_keys
        self.ids = ids
        self.timeout = timeout

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        res = db.xread(self.stream_keys, self.ids, self.timeout)
        return res

    @staticmethod
    def craft_request(*args: str) -> "XreadCommand":
        if len(args) < 2:
            raise exceptions.RequestCraftError(
                "XreadCommand takes at least 2 arguments"
            )
        if args[0].upper() == "BLOCK":
            if len(args) < 4:
                raise exceptions.RequestCraftError(
                    "XreadCommand with BLOCK takes at least 4 arguments"
                )
            return XreadCommand(
                craft_command("XREAD", *args).encode(),
                list(args[2:]),
                list(args[1:2]),
                int(args[3]),
            )
        return XreadCommand(
            craft_command("XREAD", *args).encode(), list(args[1:]), list(args[0])
        )


def craft_command(*args: str) -> data_types.RespArray:
    return data_types.RespArray(
        list(map(lambda x: data_types.RespBulkString(x.encode()), args))
    )
