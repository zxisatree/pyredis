from datetime import datetime, timedelta
from typing import Iterable, cast

import constants
from data_types import (
    RespSimpleString,
    RespBulkString,
    RespArray,
    RespInteger,
    RespPlainString,
    RespSimpleError,
    RespRdbFile,
)
import exceptions
from interfaces import Command
from logs import logger
from utils import encode_score, construct_conn_id, transform_to_execute_output


class NoOp(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"NOOP"

    def execute(self, db, replica_handler, conn):
        return transform_to_execute_output(constants.NO_OP_ERROR)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return NoOp(b"")


class PingCommand(Command):
    expected_arg_count = [0]
    allowed_in_subscribed_mode = True

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"PING"

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        if db.in_subscribed_mode(conn_id):
            return RespArray(
                [RespSimpleString(b"pong"), RespBulkString(b"")]
            ).encode_to_list()
        else:
            return RespSimpleString(b"PONG").encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return PingCommand(craft_command("PING").encode())


class EchoCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, bulk_str: RespBulkString):
        self._raw_cmd = raw_cmd
        self.msg = bulk_str.data
        self._keyword = b"ECHO"

    def execute(self, db, replica_handler, conn):
        return RespSimpleString(self.msg).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return EchoCommand(
            craft_command("ECHO", *args).encode(),
            RespBulkString(args[0].encode()),
        )


class SetCommand(Command):
    expected_arg_count = [2, 3]
    propogated_to_replicas = True

    def __init__(
        self,
        raw_cmd: bytes,
        key: RespBulkString,
        value: RespBulkString,
        expiry: RespBulkString | None,
    ):
        self._raw_cmd = raw_cmd
        self.key = key.data
        self.value = value.data
        self.expiry = (
            (datetime.now() + timedelta(milliseconds=int(expiry.data)))
            if expiry
            else None
        )
        self._keyword = b"SET"

    def execute(self, db, replica_handler, conn):
        db.set_string_value(self.key, (self.value.decode(), self.expiry))
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    @staticmethod
    def validate_px(px_cmd: RespBulkString):
        if px_cmd.data.upper() != b"PX":
            raise exceptions.UnsupportedOperationError(
                f"Unsupported SET command (fourth element is not 'PX') {px_cmd.data}"
            )

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return SetCommand(
            craft_command("SET", *args).encode(),
            RespBulkString(args[0].encode()),
            RespBulkString(args[1].encode()),
            RespBulkString(args[2].encode()) if len(args) > 2 else None,
        )


class IncrCommand(Command):
    expected_arg_count = [1]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"INCR"

    def execute(self, db, replica_handler, conn):
        value_type = db.get_type(self.key)
        if value_type not in [db.ValType.NONE, db.ValType.STRING]:
            raise exceptions.UnsupportedOperationError(
                "INCR command is unsupported for stream values"
            )

        old_value = db[self.key]
        value = cast(str, old_value)
        if old_value:
            try:
                new_value = str(int(value) + 1)
            except ValueError:
                return RespSimpleError(
                    b"ERR value is not an integer or out of range"
                ).encode_to_list()
            expiry = db.get_expiry(self.key)
        else:
            new_value = str(1)
            expiry = None

        db.set_string_value(self.key, (new_value, expiry))
        return RespInteger(int(new_value)).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return IncrCommand(craft_command("INCR", *args).encode(), args[0].encode())


class GetCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"GET"

    def execute(self, db, replica_handler, conn):
        if self.key in db:
            value = db[self.key]
            if isinstance(value, str):
                return RespBulkString(value.encode()).encode_to_list()
            elif isinstance(value, list):
                return RespBulkString(str(value).encode()).encode_to_list()
        return transform_to_execute_output(constants.NULL_BULK_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return GetCommand(
            craft_command("GET", *args).encode(),
            args[0].encode(),
        )


class CommandCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"COMMAND"

    def execute(self, db, replica_handler, conn):
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return CommandCommand(craft_command("COMMAND").encode())


class InfoCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"INFO"

    def execute(self, db, replica_handler, conn):
        return replica_handler.get_info()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return InfoCommand(craft_command("INFO").encode())


class ReplConfCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"REPLCONF"

    def execute(self, db, replica_handler, conn):
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ReplConfCommand(craft_command("REPLCONF").encode())


class ReplConfAckCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"REPLCONF"

    def execute(self, db, replica_handler, conn):
        logger.info(
            f"incrementing {replica_handler.ack_count=} to {replica_handler.ack_count + 1}"
        )
        replica_handler.ack_count += 1
        return []

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ReplConfAckCommand(craft_command("REPLCONF", "ACK", args[0]).encode())


class ReplConfGetAckCommand(Command):
    expected_arg_count = [0]
    propogated_to_replicas = True

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"REPLCONF"

    def execute(self, db, replica_handler, conn):
        return RespArray(
            [
                RespBulkString(b"REPLCONF"),
                RespBulkString(b"ACK"),
                RespBulkString(str(replica_handler.master_repl_offset).encode()),
            ]
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ReplConfGetAckCommand(craft_command("REPLCONF", "GETACK").encode())


class PsyncCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"FULLRESYNC"

    def execute(self, db, replica_handler, conn):
        replica_handler.add_slave(conn)
        return [
            RespSimpleString(
                f"FULLRESYNC {replica_handler.ip} {replica_handler.master_repl_offset}".encode()
            ).encode(),
            RespRdbFile(constants.EMPTY_RDB_FILE).encode(),
        ]

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return PsyncCommand(craft_command("PSYNC").encode())


class FullResyncCommand(Command):
    expected_arg_count = [1]

    def __init__(self, data: bytes) -> None:
        self.data = data
        self._raw_cmd = data
        self._keyword = b"FULLRESYNC"

    def execute(self, db, replica_handler, conn):
        return []

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return FullResyncCommand(craft_command("FULLRESYNC").encode())


class RdbFileCommand(Command):
    expected_arg_count = [0]

    def __init__(self, data: bytes) -> None:
        self.rdbfile = RespRdbFile(data)
        self._raw_cmd = data
        self._keyword = b""

    # slave received a RDB file
    def execute(self, db, replica_handler, conn):
        db.init_from_rdb(self.rdbfile.data)
        return []

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return RdbFileCommand(constants.EMPTY_RDB_FILE)


class ConfigGetCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"CONFIG"

    def execute(self, db, replica_handler, conn):
        if self.key.upper() == b"DIR":
            return RespArray(
                [
                    RespBulkString(self.key),
                    RespBulkString(db.dir.encode()),
                ]
            ).encode_to_list()
        elif self.key.upper() == b"DBFILENAME":
            return RespArray(
                [
                    RespBulkString(self.key),
                    RespBulkString(db.dbfilename.encode()),
                ]
            ).encode_to_list()
        else:
            return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ConfigGetCommand(
            craft_command("CONFIG", "GET", args[0]).encode(), args[0].encode()
        )


class KeysCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, pattern: bytes):
        self._raw_cmd = raw_cmd
        self.pattern = pattern
        self._keyword = b"KEYS"

    def execute(self, db, replica_handler, conn):
        return RespArray(
            list(
                map(
                    RespBulkString,
                    db.rdb.key_values.keys(),
                )
            )
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return KeysCommand(craft_command("KEYS", args[0]).encode(), args[0].encode())


class WaitCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, replica_count: int, timeout: int):
        self._raw_cmd = raw_cmd
        self.replica_count = replica_count
        self.timeout = timedelta(milliseconds=timeout)
        self._keyword = b"WAIT"

    def execute(self, db, replica_handler, conn):
        now = datetime.now()
        end = now + self.timeout
        replica_handler.ack_count = 0
        replica_handler.propogate(
            RespArray(
                [
                    RespBulkString(b"REPLCONF"),
                    RespBulkString(b"GETACK"),
                    RespBulkString(b"*"),
                ]
            ).encode()
        )
        logger.info("finished sending to all slaves")
        while replica_handler.ack_count < self.replica_count and datetime.now() < end:
            pass

        logger.info(
            f"{replica_handler.ack_count=}, {datetime.now() - end=} (should be positive)"
        )
        # hardcode to len(slaves) if no acks
        return RespInteger(
            replica_handler.ack_count
            if replica_handler.ack_count > 0
            else len(replica_handler.slaves)
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return WaitCommand(
            craft_command("WAIT", *args).encode(), int(args[0]), int(args[1])
        )


class TypeCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"TYPE"

    def execute(self, db, replica_handler, conn):
        if self.key in db:
            return RespSimpleString(
                str(db.get_type(self.key)).encode()
            ).encode_to_list()
        return RespSimpleString(b"none").encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return TypeCommand(
            craft_command("TYPE", *args).encode(),
            args[0].encode(),
        )


class MultiCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"MULTI"

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        db.start_xact(conn_id)
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return MultiCommand(craft_command("MULTI", *args).encode())


class ExecCommand(Command):
    expected_arg_count = [0]
    allowed_in_xact = True

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"EXEC"

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        if not db.xact_exists(conn_id):
            return RespSimpleError(b"ERR EXEC without MULTI").encode_to_list()
        cmds = db.exec_xact(conn_id)
        responses = [cmd.execute(db, replica_handler, conn) for cmd in cmds]
        flattened = []
        for response in responses:
            if isinstance(response, list):
                for inner_response in response:
                    flattened.append(RespPlainString(inner_response))
            else:
                flattened.append(RespPlainString(response))
        return RespArray(flattened).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ExecCommand(craft_command("EXEC", *args).encode())


class DiscardCommand(Command):
    expected_arg_count = [0]
    allowed_in_xact = True

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"DISCARD"

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        if not db.xact_exists(conn_id):
            return RespSimpleError(b"ERR DISCARD without MULTI").encode_to_list()
        db.exec_xact(conn_id)
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return DiscardCommand(craft_command("DISCARD", *args).encode())


class RpushCommand(Command):
    expected_arg_count = [2]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
        values: list[bytes],
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self.values = values
        self._keyword = b"RPUSH"

    def execute(self, db, replica_handler, conn):
        length = db.rpush(self.key, self.values)
        return RespInteger(length).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return RpushCommand(
            craft_command("RPUSH", *args).encode(),
            args[0].encode(),
            [arg.encode() for arg in args],
        )


class LpushCommand(Command):
    expected_arg_count = [2]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
        values: list[bytes],
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self.values = values
        self._keyword = b"LPUSH"

    def execute(self, db, replica_handler, conn):
        length = db.lpush(self.key, self.values[::-1])
        return RespInteger(length).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LpushCommand(
            craft_command("LPUSH", *args).encode(),
            args[0].encode(),
            [arg.encode() for arg in args],
        )


class LpopCommand(Command):
    expected_arg_count = [1, 2]

    def __init__(self, raw_cmd: bytes, key: bytes, count: int):
        self._raw_cmd = raw_cmd
        self.key = key
        self.count = count
        self._keyword = b"LPOP"

    def execute(self, db, replica_handler, conn):
        if self.count == 1:
            value = db.lpop(self.key)
            return RespBulkString(value).encode_to_list()
        else:
            values = db.lpop_multiple(self.key, self.count)
            return RespArray(
                [RespBulkString(value) for value in values]
            ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LpopCommand(
            craft_command("LPOP", *args).encode(), args[0].encode(), int(args[1])
        )


class BlpopCommand(Command):
    expected_arg_count = [1, 2]

    def __init__(self, raw_cmd: bytes, key: bytes, timeout: float):
        self._raw_cmd = raw_cmd
        self.key = key
        self.timeout = timeout
        self._keyword = b"BLPOP"

    def execute(self, db, replica_handler, conn):
        value = db.blpop_timeout(self.key, None if self.timeout == 0 else self.timeout)
        if value:
            return RespArray(
                [RespBulkString(self.key), RespBulkString(value)]
            ).encode_to_list()
        else:
            # timed out
            return transform_to_execute_output(constants.NULL_ARRAY_RESP_STRING)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return BlpopCommand(
            craft_command("LPOP", *args).encode(),
            args[0].encode(),
            int(args[1]) if len(args) > 1 else 0,
        )


class LlenCommand(Command):
    expected_arg_count = [1]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"LLEN"

    def execute(self, db, replica_handler, conn):
        if not db.key_exists(self.key):
            length = 0
        else:
            length = len(db.get_list(self.key))
        return RespInteger(length).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LlenCommand(
            craft_command("LLEN", *args).encode(),
            args[0].encode(),
        )


class LrangeCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, start: int, stop: int):
        self._raw_cmd = raw_cmd
        self.key = key
        self.start = start
        self.stop = stop
        self._keyword = b"LRANGE"

    def execute(self, db, replica_handler, conn):
        if not db.key_exists(self.key) or (self.stop >= 0 and self.start > self.stop):
            return transform_to_execute_output(constants.EMPTY_RESP_ARRAY)
        retrieved_list = db.get_list(self.key)
        if self.start >= len(retrieved_list):
            return transform_to_execute_output(constants.EMPTY_RESP_ARRAY)
        # automatically handles stop being larger than array
        if self.stop == -1:
            adjusted_stop = len(retrieved_list)
        else:
            adjusted_stop = self.stop + 1
        return RespArray(
            [RespBulkString(val) for val in retrieved_list[self.start : adjusted_stop]]
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LrangeCommand(
            craft_command("LRANGE", *args).encode(),
            args[0].encode(),
            int(args[1]),
            int(args[2]),
        )


class XaddCommand(Command):
    def __init__(self, raw_cmd: bytes, stream_key: bytes, data: list[RespBulkString]):
        self._raw_cmd = raw_cmd
        self.stream_key = stream_key
        self.data = data
        self._keyword = b"XADD"

    def execute(
        self,
        db,
        replica_handler,
        conn,
    ):
        raw_stream_entry_id = self.data[0]
        stream_entry_id = raw_stream_entry_id.data
        err = db.validate_stream_id(self.stream_key, stream_entry_id.decode())
        if err is not None:
            return RespSimpleError(err).encode_to_list()

        kv_dict = {}
        for i in range(1, len(self.data), 2):
            stream_key = self.data[i]
            stream_value = self.data[i + 1]
            kv_dict[stream_key.data.decode()] = stream_value.data.decode()
        logger.info(f"{stream_entry_id=}, {kv_dict=}")
        processed_stream_id = db.xadd(
            self.stream_key, stream_entry_id.decode(), kv_dict
        )
        return RespBulkString(processed_stream_id.encode()).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        args_len = len(args)
        if args_len < 2 or args_len % 2 != 0:
            error_msg = f"{cls.__name__} takes an even number of argument(s), but {args_len} {'was' if args_len == 1 else 'were'} provided"
            raise exceptions.RequestCraftError(error_msg)

        return XaddCommand(
            craft_command("XADD", *args).encode(),
            args[0].encode(),
            list(map(lambda x: RespBulkString(x.encode()), args[1:])),
        )


class XrangeCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, start: str, end: str):
        self._raw_cmd = raw_cmd
        self.key = key
        self.start = start
        self.end = end
        self._keyword = b"XRANGE"

    def execute(self, db, replica_handler, conn):
        return db.xrange(self.key, self.start, self.end)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return XrangeCommand(
            craft_command("XRANGE", *args).encode(), args[0].encode(), args[1], args[2]
        )


class XreadCommand(Command):
    expected_arg_count = [2]

    def __init__(
        self,
        raw_cmd: bytes,
        stream_keys: list[bytes],
        ids: list[str],
        timeout: int | None = None,
    ):
        self._raw_cmd = raw_cmd
        self.stream_keys = stream_keys
        self.ids = ids
        self.timeout = timeout
        self._keyword = b"XREAD"

    def execute(self, db, replica_handler, conn):
        return db.xread(self.stream_keys, self.ids, self.timeout)

    @classmethod
    def craft_request(cls, *args: str):
        if args[0].upper() == "BLOCK":
            verify_arg_count(cls.__name__, [4], len(args))
            return XreadCommand(
                craft_command("XREAD", *args).encode(),
                list(map(lambda x: x.encode(), args[2:])),
                list(args[1:2]),
                int(args[3]),
            )
        else:
            verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
            return XreadCommand(
                craft_command("XREAD", *args).encode(),
                list(map(lambda x: x.encode(), args[1:])),
                list(args[0]),
            )


class SubscribeCommand(Command):
    expected_arg_count = [1]
    allowed_in_subscribed_mode = True

    def __init__(self, raw_cmd: bytes, channel_name: bytes):
        self._raw_cmd = raw_cmd
        self.channel_name = channel_name
        self._keyword = b"SUBSCRIBE"

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        channel_count = db.subscribe(self.channel_name, conn, conn_id)
        return RespArray(
            [
                RespBulkString(b"subscribe"),
                RespBulkString(self.channel_name),
                RespInteger(channel_count),
            ]
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return SubscribeCommand(
            craft_command("SUBSCRIBE", *args).encode(), args[0].encode()
        )


class UnsubscribeCommand(Command):
    expected_arg_count = [1]
    allowed_in_subscribed_mode = True

    def __init__(self, raw_cmd: bytes, channel_name: bytes):
        self._raw_cmd = raw_cmd
        self.channel_name = channel_name
        self._keyword = b"UNSUBSCRIBE"

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        channel_count = db.unsubscribe(self.channel_name, conn, conn_id)
        return RespArray(
            [
                RespBulkString(b"unsubscribe"),
                RespBulkString(self.channel_name),
                RespInteger(channel_count),
            ]
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return UnsubscribeCommand(
            craft_command("UNSUBSCRIBE", *args).encode(), args[0].encode()
        )


class PublishCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, channel_name: bytes, msg: bytes):
        self._raw_cmd = raw_cmd
        self.channel_name = channel_name
        self.msg = msg
        self._keyword = b"PUBLISH"

    def execute(self, db, replica_handler, conn):
        publish_msg = RespArray(
            [
                RespBulkString(b"message"),
                RespBulkString(self.channel_name),
                RespBulkString(self.msg),
            ]
        ).encode()
        for _, subscribed_conn in db.get_subscribers(self.channel_name):
            subscribed_conn.sendall(publish_msg)
        return RespInteger(len(db.get_subscribers(self.channel_name))).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return PublishCommand(
            craft_command("PUBLISH", *args).encode(), args[0].encode(), args[1].encode()
        )


class ZaddCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, score: float, name: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"ZADD"
        self.key = key
        self.score = score
        self.name = name

    def execute(self, db, replica_handler, conn):
        return RespInteger(
            int(db.zadd(self.key, self.score, self.name))
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ZaddCommand(
            craft_command("ZADD", *args).encode(),
            args[0].encode(),
            float(args[1]),
            args[2].encode(),
        )


class ZrankCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, key: bytes, name: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"ZRANK"
        self.key = key
        self.name = name

    def execute(self, db, replica_handler, conn):
        result = db.zrank(self.key, self.name)
        if result == -1:
            return transform_to_execute_output(constants.NULL_BULK_RESP_STRING)
        else:
            return RespInteger(result).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ZrankCommand(
            craft_command("ZRANK", *args).encode(),
            args[0].encode(),
            args[1].encode(),
        )


class ZrangeCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, start: int, end: int):
        self._raw_cmd = raw_cmd
        self._keyword = b"ZRANGE"
        self.key = key
        self.start = start
        self.end = end

    def execute(self, db, replica_handler, conn):
        result = db.zrange(self.key, self.start, self.end)
        if len(result) == 0:
            return transform_to_execute_output(constants.EMPTY_RESP_ARRAY)
        else:
            return RespArray(
                [RespBulkString(item.name) for item in result]
            ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ZrangeCommand(
            craft_command("ZRANGE", *args).encode(),
            args[0].encode(),
            int(args[1]),
            int(args[2]),
        )


class ZcardCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"ZCARD"
        self.key = key

    def execute(self, db, replica_handler, conn):
        result = db.zcard(self.key)
        return RespInteger(result).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ZcardCommand(
            craft_command("ZCARD", *args).encode(),
            args[0].encode(),
        )


class ZscoreCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, key: bytes, name: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"ZSCORE"
        self.key = key
        self.name = name

    def execute(self, db, replica_handler, conn):
        result = db.zscore(self.key, self.name)
        if result == -1:
            return transform_to_execute_output(constants.NULL_BULK_RESP_STRING)
        else:
            return RespBulkString(str(result).encode()).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ZscoreCommand(
            craft_command("ZSCORE", *args).encode(),
            args[0].encode(),
            args[1].encode(),
        )


class ZremCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, key: bytes, name: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"ZREM"
        self.key = key
        self.name = name

    def execute(self, db, replica_handler, conn):
        result = db.zrem(self.key, self.name)
        return RespInteger(result).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ZremCommand(
            craft_command("ZREM", *args).encode(),
            args[0].encode(),
            args[1].encode(),
        )


class GeoaddCommand(Command):
    expected_arg_count = [4]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
        longitude: float,
        latitude: float,
        member: bytes,
    ):
        self._raw_cmd = raw_cmd
        self._keyword = b"GEOADD"
        self.key = key
        self.longitude = longitude
        self.latitude = latitude
        self.member = member

    def execute(self, db, replica_handler, conn):
        if not (-180 <= self.longitude <= 180):
            return RespSimpleError(b"ERR invalid longitude").encode_to_list()
        if not (-85.05112878 <= self.latitude <= +85.05112878):
            return RespSimpleError(b"ERR invalid latitude").encode_to_list()
        score = encode_score(self.longitude, self.latitude)
        return RespInteger(int(db.zadd(self.key, score, self.member))).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return GeoaddCommand(
            craft_command("GEOADD", *args).encode(),
            args[0].encode(),
            float(args[1].encode()),
            float(args[2].encode()),
            args[3].encode(),
        )


class GeoposCommand(Command):
    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
        members: list[bytes],
    ):
        self._raw_cmd = raw_cmd
        self._keyword = b"GEOPOS"
        self.key = key
        self.members = members

    def execute(self, db, replica_handler, conn):
        positions = db.geopos(self.key, self.members)
        return RespArray(
            [
                RespArray(
                    [
                        RespBulkString(str(position[0]).encode()),
                        RespBulkString(str(position[1]).encode()),
                    ]
                )
                if position is not None
                else RespArray(None)
                for position in positions
            ]
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        args_len = len(args)
        if args_len < 3:
            error_msg = f"{cls.__name__} takes at least 3 arguments, but {args_len} {'was' if args_len == 1 else 'were'} provided"
            raise exceptions.RequestCraftError(error_msg)
        return GeoposCommand(
            craft_command("GEOPOS", *args).encode(),
            args[0].encode(),
            [arg.encode() for arg in args[1:]],
        )


class GeodistCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, place1: bytes, place2: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"GEODIST"
        self.key = key
        self.place1 = place1
        self.place2 = place2

    def execute(self, db, replica_handler, conn):
        return RespBulkString(
            str(db.geodist(self.key, self.place1, self.place2)).encode()
        ).encode_to_list()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return GeodistCommand(
            craft_command("GEODIST", *args).encode(),
            args[0].encode(),
            args[1].encode(),
            args[2].encode(),
        )


def verify_arg_count(
    command_name: str, expected_arg_count: Iterable[int], args_len: int
):
    """Raises a RequestCraftError if arg count does not match"""
    if args_len not in expected_arg_count:
        error_msg = f"{command_name} takes {'/'.join(str(i) for i in expected_arg_count)} argument(s), but {args_len} {'was' if args_len == 1 else 'were'} provided"
        raise exceptions.RequestCraftError(error_msg)


def craft_command(*args: str) -> RespArray:
    return RespArray(list(map(lambda x: RespBulkString(x.encode()), args)))
