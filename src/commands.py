from datetime import datetime, timedelta
from typing import cast

from . import constants, exceptions
from .data_types import (
    RespArray,
    RespBulkString,
    RespInteger,
    RespPlainString,
    RespRdbFile,
    RespSimpleError,
    RespSimpleString,
)
from .database import Database
from .interfaces import Command, XactBehaviour
from .logs import logger
from .utils import construct_conn_id, encode_score, transform_to_execute_output


class NoOpCommand(Command):
    def execute(self, db, replica_handler, conn):
        return transform_to_execute_output(constants.NO_OP_ERROR)


class PingCommand(Command):
    allowed_in_subscribed_mode = True

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        if db.in_subscribed_mode(conn_id):
            return RespArray(
                [RespBulkString(b"pong"), RespBulkString(b"")]
            ).encode_to_list()
        else:
            return RespSimpleString(b"PONG").encode_to_list()


class EchoCommand(Command):
    def __init__(self, msg: bytes):
        self.msg = msg

    def execute(self, db, replica_handler, conn):
        return RespBulkString(self.msg).encode_to_list()


class SetCommand(Command):
    should_propogate_to_replicas = True
    should_write_to_aof = True

    def __init__(
        self,
        key: bytes,
        value: bytes,
        expiry: bytes | None,
    ):
        self.key = key
        self.value = value
        self.expiry = (
            (datetime.now() + timedelta(milliseconds=int(expiry))) if expiry else None
        )

    def execute(self, db, replica_handler, conn):
        db.set_string_value(self.key, (self.value.decode(), self.expiry))
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)

    def execute_for_aof(self, db: Database) -> list[bytes]:
        db.set_string_value(self.key, (self.value.decode(), self.expiry))
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


class IncrCommand(Command):
    def __init__(
        self,
        key: bytes,
    ):
        self.key = key

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


class GetCommand(Command):
    def __init__(self, key: bytes):
        self.key = key

    def execute(self, db, replica_handler, conn):
        if self.key in db:
            value = db[self.key]
            if isinstance(value, str):
                return RespBulkString(value.encode()).encode_to_list()
            elif isinstance(value, list):
                return RespBulkString(str(value).encode()).encode_to_list()
        return transform_to_execute_output(constants.NULL_BULK_RESP_STRING)


class CommandCommand(Command):
    def execute(self, db, replica_handler, conn):
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


class InfoCommand(Command):
    def execute(self, db, replica_handler, conn):
        return replica_handler.get_info()


class ReplConfCommand(Command):
    def execute(self, db, replica_handler, conn):
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


class ReplConfAckCommand(Command):
    keyword = "REPLCONF"

    def execute(self, db, replica_handler, conn):
        replica_handler.incr_ack_count()
        return []


class ReplConfGetAckCommand(Command):
    keyword = "REPLCONF"
    should_propogate_to_replicas = True

    def execute(self, db, replica_handler, conn):
        return RespArray(
            [
                RespBulkString(b"REPLCONF"),
                RespBulkString(b"ACK"),
                RespBulkString(str(replica_handler.master_repl_offset).encode()),
            ]
        ).encode_to_list()


class PsyncCommand(Command):
    def execute(self, db, replica_handler, conn):
        replica_handler.add_slave(conn)
        return [
            RespSimpleString(
                f"FULLRESYNC {replica_handler.master_replid} {replica_handler.master_repl_offset}".encode()
            ).encode(),
            RespRdbFile(constants.EMPTY_RDB_FILE).encode(),
        ]


class FullResyncCommand(Command):
    def __init__(self, data: bytes) -> None:
        self.data = data

    def execute(self, db, replica_handler, conn):
        return []


class RdbFileCommand(Command):
    keyword = ""

    def __init__(self, data: bytes) -> None:
        self.rdbfile = RespRdbFile(data)

    # slave received a RDB file
    def execute(self, db, replica_handler, conn):
        db.init_from_rdb(self.rdbfile.key_values)
        return []


class ConfigGetCommand(Command):
    keyword = "CONFIG"

    def __init__(self, key: bytes):
        self.key = key

    def execute(self, db, replica_handler, conn):
        config_value = db.get_config(self.key)
        if config_value is None:
            return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)
        else:
            return RespArray(
                [
                    RespBulkString(self.key),
                    RespBulkString(config_value.encode()),
                ]
            ).encode_to_list()


class KeysCommand(Command):
    def __init__(self, pattern: bytes):
        self.pattern = pattern

    def execute(self, db, replica_handler, conn):
        return RespArray(
            list(
                map(
                    RespBulkString,
                    db.store.keys(),
                )
            )
        ).encode_to_list()


class WaitCommand(Command):
    def __init__(self, replica_count: int, timeout: int):
        self.replica_count = replica_count
        self.timeout = timedelta(milliseconds=timeout)

    def execute(self, db, replica_handler, conn):
        # if no writes since server start, return number of connected replicas
        if replica_handler.master_repl_offset == 0:
            return RespInteger(len(replica_handler.slaves)).encode_to_list()
        ack_count = replica_handler.wait(self.replica_count, self.timeout)
        return RespInteger(ack_count).encode_to_list()


class TypeCommand(Command):
    def __init__(self, key: bytes):
        self.key = key

    def execute(self, db, replica_handler, conn):
        if self.key in db:
            return RespSimpleString(
                str(db.get_type(self.key)).encode()
            ).encode_to_list()
        return RespSimpleString(b"none").encode_to_list()


class MultiCommand(Command):
    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        db.start_xact(conn_id)
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


class ExecCommand(Command):
    xact_behaviour = XactBehaviour.EXECUTE

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        if not db.xact_exists(conn_id):
            return RespSimpleError(b"ERR EXEC without MULTI").encode_to_list()
        has_any_version_changed, cmds = db.pop_xact_for_exec(conn_id)
        if has_any_version_changed:
            return RespArray(None).encode_to_list()

        responses = [cmd.execute(db, replica_handler, conn) for cmd in cmds]
        flattened = []
        for response in responses:
            if isinstance(response, list):
                for inner_response in response:
                    flattened.append(RespPlainString(inner_response))
            else:
                flattened.append(RespPlainString(response))
        return RespArray(flattened).encode_to_list()


class DiscardCommand(Command):
    xact_behaviour = XactBehaviour.EXECUTE

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        if not db.xact_exists(conn_id):
            return RespSimpleError(b"ERR DISCARD without MULTI").encode_to_list()
        db.pop_xact_for_exec(conn_id)
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


class RpushCommand(Command):
    def __init__(
        self,
        key: bytes,
        values: list[bytes],
    ):
        self.key = key
        self.values = values

    def execute(self, db, replica_handler, conn):
        length = db.rpush(self.key, self.values)
        return RespInteger(length).encode_to_list()


class LpushCommand(Command):
    def __init__(
        self,
        key: bytes,
        values: list[bytes],
    ):
        self.key = key
        self.values = values

    def execute(self, db, replica_handler, conn):
        length = db.lpush(self.key, self.values[::-1])
        return RespInteger(length).encode_to_list()


class LpopCommand(Command):
    def __init__(self, key: bytes, count: int):
        self.key = key
        self.count = count

    def execute(self, db, replica_handler, conn):
        if self.count == 1:
            value = db.lpop(self.key)
            return RespBulkString(value).encode_to_list()
        else:
            values = db.lpop_multiple(self.key, self.count)
            return RespArray(
                [RespBulkString(value) for value in values]
            ).encode_to_list()


class BlpopCommand(Command):
    def __init__(self, key: bytes, timeout: float):
        self.key = key
        self.timeout = timeout

    def execute(self, db, replica_handler, conn):
        value = db.blpop_timeout(self.key, None if self.timeout == 0 else self.timeout)
        if value:
            return RespArray(
                [RespBulkString(self.key), RespBulkString(value)]
            ).encode_to_list()
        else:
            # timed out
            return transform_to_execute_output(constants.NULL_ARRAY_RESP_STRING)


class LlenCommand(Command):
    def __init__(
        self,
        key: bytes,
    ):
        self.key = key

    def execute(self, db, replica_handler, conn):
        if not db.key_exists(self.key):
            length = 0
        else:
            length = len(db.get_list(self.key))
        return RespInteger(length).encode_to_list()


class LrangeCommand(Command):
    def __init__(self, key: bytes, start: int, stop: int):
        self.key = key
        self.start = start
        self.stop = stop

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


class XaddCommand(Command):
    def __init__(self, stream_key: bytes, values: list[bytes]):
        self.stream_key = stream_key
        self.values = values

    def execute(
        self,
        db,
        replica_handler,
        conn,
    ):
        stream_entry_id = self.values[0]
        err = db.validate_stream_id(self.stream_key, stream_entry_id.decode())
        if err is not None:
            return RespSimpleError(err).encode_to_list()

        kv_dict = {}
        for i in range(1, len(self.values), 2):
            stream_key = self.values[i]
            stream_value = self.values[i + 1]
            kv_dict[stream_key.decode()] = stream_value.decode()
        logger.info(f"{stream_entry_id=}, {kv_dict=}")
        processed_stream_id = db.xadd(
            self.stream_key, stream_entry_id.decode(), kv_dict
        )
        return RespBulkString(processed_stream_id.encode()).encode_to_list()


class XrangeCommand(Command):
    def __init__(self, key: bytes, start: str, end: str):
        self.key = key
        self.start = start
        self.end = end

    def execute(self, db, replica_handler, conn):
        return db.xrange(self.key, self.start, self.end)


class XreadCommand(Command):
    def __init__(
        self,
        stream_keys: list[bytes],
        ids: list[str],
        timeout: int | None = None,
    ):
        self.stream_keys = stream_keys
        self.ids = ids
        self.timeout = timeout

    def execute(self, db, replica_handler, conn):
        return db.xread(self.stream_keys, self.ids, self.timeout)


class SubscribeCommand(Command):
    allowed_in_subscribed_mode = True

    def __init__(self, channel_name: bytes):
        self.channel_name = channel_name

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


class UnsubscribeCommand(Command):
    allowed_in_subscribed_mode = True

    def __init__(self, channel_name: bytes):
        self.channel_name = channel_name

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


class PublishCommand(Command):
    def __init__(self, channel_name: bytes, msg: bytes):
        self.channel_name = channel_name
        self.msg = msg

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


class ZaddCommand(Command):
    def __init__(self, key: bytes, score: float, name: bytes):
        self.key = key
        self.score = score
        self.name = name

    def execute(self, db, replica_handler, conn):
        return RespInteger(
            int(db.zadd(self.key, self.score, self.name))
        ).encode_to_list()


class ZrankCommand(Command):
    def __init__(self, key: bytes, name: bytes):
        self.key = key
        self.name = name

    def execute(self, db, replica_handler, conn):
        result = db.zrank(self.key, self.name)
        if result == -1:
            return transform_to_execute_output(constants.NULL_BULK_RESP_STRING)
        else:
            return RespInteger(result).encode_to_list()


class ZrangeCommand(Command):
    def __init__(self, key: bytes, start: int, end: int):
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


class ZcardCommand(Command):
    def __init__(self, key: bytes):
        self.key = key

    def execute(self, db, replica_handler, conn):
        result = db.zcard(self.key)
        return RespInteger(result).encode_to_list()


class ZscoreCommand(Command):
    def __init__(self, key: bytes, name: bytes):
        self.key = key
        self.name = name

    def execute(self, db, replica_handler, conn):
        result = db.zscore(self.key, self.name)
        if result is None:
            return transform_to_execute_output(constants.NULL_BULK_RESP_STRING)
        else:
            return RespBulkString(str(result).encode()).encode_to_list()


class ZremCommand(Command):
    def __init__(self, key: bytes, name: bytes):
        self.key = key
        self.name = name

    def execute(self, db, replica_handler, conn):
        result = db.zrem(self.key, self.name)
        return RespInteger(result).encode_to_list()


class GeoaddCommand(Command):
    def __init__(
        self,
        key: bytes,
        longitude: float,
        latitude: float,
        member: bytes,
    ):
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


class GeoposCommand(Command):
    def __init__(
        self,
        key: bytes,
        members: list[bytes],
    ):
        self.key = key
        self.members = members

    def execute(self, db, replica_handler, conn):
        positions = db.geopos(self.key, self.members)
        return RespArray(
            [
                (
                    RespArray(
                        [
                            RespBulkString(str(position[0]).encode()),
                            RespBulkString(str(position[1]).encode()),
                        ]
                    )
                    if position is not None
                    else RespArray(None)
                )
                for position in positions
            ]
        ).encode_to_list()


class GeodistCommand(Command):
    def __init__(self, key: bytes, place1: bytes, place2: bytes):
        self.key = key
        self.place1 = place1
        self.place2 = place2

    def execute(self, db, replica_handler, conn):
        geodist = db.geodist(self.key, self.place1, self.place2)
        if geodist is None:
            return constants.NULL_BULK_RESP_STRING
        return RespBulkString(str(geodist).encode()).encode_to_list()


class GeosearchCommand(Command):
    def __init__(
        self,
        key: bytes,
        mode: bytes,
        longitude: float,
        latitude: float,
        byradius: bytes,
        radius: float,
        unit: bytes,
    ):
        if mode != b"FROMLONLAT" or byradius != b"BYRADIUS" or unit != b"m":
            raise NotImplementedError()
        self.key = key
        self.mode = mode
        self.longitude = longitude
        self.latitude = latitude
        self.byradius = byradius
        self.radius = radius
        self.unit = unit

    def execute(self, db, replica_handler, conn):
        results = db.geosearch(
            self.key,
            self.longitude,
            self.latitude,
            self.radius,
        )
        return RespArray([RespBulkString(name) for name in results]).encode_to_list()


class AclWhoamiCommand(Command):
    keyword = "ACL"

    def execute(self, db, replica_handler, conn):
        return RespBulkString(b"default").encode_to_list()


class AclGetuserCommand(Command):
    keyword = "ACL"

    def __init__(self, user: bytes):
        self.user = user

    def execute(self, db, replica_handler, conn):
        passwords = db.get_passwords(self.user)
        properties = []
        if len(passwords) == 0:
            properties.append(RespBulkString(b"nopass"))
        return RespArray(
            [
                RespBulkString(b"flags"),
                RespArray(properties),
                RespBulkString(b"passwords"),
                RespArray([RespBulkString(password) for password in passwords]),
            ]
        ).encode_to_list()


class AclSetuserCommand(Command):
    keyword = "ACL"

    def __init__(self, user: bytes, property: bytes):
        self.user = user
        self.property = property

    def execute(self, db, replica_handler, conn):
        if self.property.startswith(b">"):
            conn_id = construct_conn_id(conn)
            db.set_password(self.user, self.property[1:], conn_id)
            return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)
        else:
            raise exceptions.UnsupportedOperationError(
                "SETUSER is only allowed for setting passwords with >"
            )


class AuthCommand(Command):
    allowed_while_unauthenticated = True

    def __init__(self, user: bytes, password: bytes):
        self.user = user
        self.password = password

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        is_authenticated = db.authenticate(self.user, self.password, conn_id)
        if is_authenticated:
            return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)
        else:
            return RespSimpleError(
                b"WRONGPASS invalid username-password pair or user is disabled."
            ).encode_to_list()


class WatchCommand(Command):
    xact_behaviour = XactBehaviour.ERROR

    def __init__(self, keys: list[bytes]):
        self.keys = keys

    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        for key in self.keys:
            db.watch_key(conn_id, key)
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


class UnwatchCommand(Command):
    def execute(self, db, replica_handler, conn):
        conn_id = construct_conn_id(conn)
        db.clear_watched_keys(conn_id)
        return transform_to_execute_output(constants.OK_SIMPLE_RESP_STRING)


def craft_command(*args: str) -> RespArray:
    return RespArray(list(map(lambda x: RespBulkString(x.encode()), args)))
