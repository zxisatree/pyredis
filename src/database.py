import bisect
import hashlib
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from threading import Condition, Lock, Semaphore
from typing import cast

from . import constants
from .aof import AofHandler
from .data_types import RespArray, RespBulkString, RespDataType
from .interfaces import Command, ListVal, StreamId, StreamVal, StrVal
from .logs import logger
from .singleton_meta import SingletonMeta
from .utils import (
    ConnId,
    ThreadsafeDefaultdict,
    decode_score,
    haversines,
    transform_to_execute_output,
)


class SortedSet:
    """Naive implementation, uses a list to maintain sorted order"""

    @dataclass(order=True)
    class Item:
        score: float
        name: bytes

    def __init__(self) -> None:
        # constant time lookup of identifiers
        self.names: set[bytes] = set()
        # counterintuitive, but the list acts as the main data source
        self.set: list[SortedSet.Item] = list()

    def __len__(self):
        return len(self.set)

    def __contains__(self, key: bytes) -> bool:
        return key in self.names

    def get_slice(self, start: int, stop: int):
        """exclusive of stop"""
        return self.set[start:stop]

    def rank(self, name: bytes):
        for idx, item in enumerate(self.set):
            if item.name == name:
                return idx
        return -1

    def score(self, name: bytes) -> float:
        for item in self.set:
            if item.name == name:
                return item.score
        return -1

    def upsert_item(self, item: Item) -> int:
        if item.name in self.names:
            for stored_item in self.set:
                if stored_item.name == item.name:
                    stored_item.score = item.score
            return 0
        else:
            bisect.insort(self.set, item)
            self.names.add(item.name)
            return 1

    def add(self, name: bytes, score: float) -> int:
        return self.upsert_item(SortedSet.Item(score, name))

    def remove(self, name: bytes) -> int:
        for idx, item in enumerate(self.set):
            if item.name == name:
                self.set.pop(idx)
                return 1
        return 0


class Database(metaclass=SingletonMeta):
    class ValType(Enum):
        NONE = 0
        STRING = 1
        STREAM = 2
        LIST = 3
        SET = 4

        def __str__(self) -> str:
            match self.value:
                case 1:
                    return "string"
                case 2:
                    return "stream"
                case 3:
                    return "list"
                case 4:
                    return "set"
            return "none"

    def __init__(
        self,
        dir: str,
        dbfilename: str,
        rdb_key_values: dict[bytes, StrVal],
        aof_handler: AofHandler,
    ):
        self.store: dict[bytes, StrVal | StreamVal | ListVal | SortedSet] = {}
        self.key_types: dict[bytes, Database.ValType] = {}
        # map of keys of streams to threads waiting for new elements
        self.stream_waitlist: ThreadsafeDefaultdict[
            bytes, tuple[Lock, set[Semaphore]]
        ] = ThreadsafeDefaultdict(lambda: (Lock(), set()))
        # map of keys of lists to threads waiting for new elements
        self.blpop_waitlist: ThreadsafeDefaultdict[bytes, Condition] = (
            ThreadsafeDefaultdict(Condition)
        )
        # xacts can only be started explicitly through a single function, so we avoid the overhead of a defaultdict here
        self.xacts: dict[ConnId, list[Command]] = {}
        self.channels: ThreadsafeDefaultdict[ConnId, set[bytes]] = (
            ThreadsafeDefaultdict(set)
        )
        self.subscribers: ThreadsafeDefaultdict[
            bytes, set[tuple[ConnId, socket.socket]]
        ] = ThreadsafeDefaultdict(set)
        self.passwords: ThreadsafeDefaultdict[bytes, list[bytes]] = (
            ThreadsafeDefaultdict(list)
        )
        self.authenticated_sessions: ThreadsafeDefaultdict[
            ConnId, tuple[bytes, bytes]
        ] = ThreadsafeDefaultdict(lambda: (b"default", b""))
        self.watched_keys: ThreadsafeDefaultdict[ConnId, dict[bytes, int]] = (
            ThreadsafeDefaultdict(dict)
        )
        self.key_versions: ThreadsafeDefaultdict[bytes, int] = ThreadsafeDefaultdict(
            int
        )

        self.dir = dir
        self.dbfilename = dbfilename
        self.init_from_rdb(rdb_key_values)
        self.aof_handler = aof_handler
        logger.info(f"db initialised with {self.store=}")

    def __len__(self) -> int:
        return len(self.store)

    def __getitem__(self, key: bytes) -> str | StreamVal | SortedSet | None:
        """Only returns the value, not the expiry"""
        if key not in self.store:
            return None
        key_type = self.key_types[key]
        value = self.store[key]
        match key_type:
            case Database.ValType.STRING:
                str_val, expiry = cast(StrVal, value)
                if expiry and self.expire_one(key):
                    return None
                return str_val
            case Database.ValType.LIST:
                list_val = cast(StreamVal, value)
                return list_val
            case Database.ValType.STREAM:
                stream_val = cast(StreamVal, value)
                return stream_val
            case Database.ValType.SET:
                set_val = cast(SortedSet, value)
                return set_val

    def __delitem__(self, key: bytes):
        del self.store[key]

    def __contains__(self, key: bytes) -> bool:
        return key in self.store

    def __str__(self) -> str:
        return str(self.store)

    def __repr__(self) -> str:
        return f"Database({repr(self.store)})"

    def init_from_rdb(self, key_values: dict[bytes, StrVal]):
        for key, value in key_values.items():
            self.store[key] = value
            self.key_types[key] = Database.ValType.STRING  # only support strings in RDB

    def get_config(self, key: bytes) -> str | None:
        uppercase_key = key.upper()
        if uppercase_key == b"DIR":
            return self.dir
        elif uppercase_key == b"DBFILENAME":
            return self.dbfilename
        elif uppercase_key == b"APPENDONLY":
            return "yes" if self.aof_handler.append_only else "no"
        elif uppercase_key == b"APPENDDIRNAME":
            return self.aof_handler.append_dirname
        elif uppercase_key == b"APPENDFILENAME":
            return self.aof_handler.append_filename
        elif uppercase_key == b"APPENDFSYNC":
            return self.aof_handler.append_fsync.value
        else:
            return None

    def is_conn_authenticated(self, conn_id: ConnId):
        user, provided_password = self.authenticated_sessions[conn_id]
        passwords = self.passwords[user]
        return len(passwords) == 0 or provided_password in passwords

    def set_string_value(self, key: bytes, value: StrVal):
        self.key_types[key] = Database.ValType.STRING
        self.store[key] = value
        self.key_versions[key] += 1

    def get_type(self, key: bytes) -> ValType:
        if key not in self.store:
            return Database.ValType.NONE
        return self.key_types[key]

    def get_expiry(self, key: bytes) -> datetime | None:
        if key not in self.store:
            return None
        value = self.store[key]
        # get_expiry only works for string values
        key_type = self.key_types[key]
        if key_type == Database.ValType.STRING:
            string_val = cast(StrVal, value)
            return string_val[1]
        else:
            raise Exception(f"Called get_expiry on a non string key {key=}")

    def expire_one(self, key: bytes) -> bool:
        # returns True if key was expired
        value = self.store[key]
        key_type = self.key_types[key]
        if key_type != Database.ValType.STRING:
            return False
        _, expiry = cast(StrVal, value)
        if expiry and expiry < datetime.now():
            del self.store[key]
            return True
        return False

    def start_xact(self, conn_id: ConnId):
        self.xacts[conn_id] = []

    def xact_exists(self, conn_id: ConnId) -> bool:
        return conn_id in self.xacts

    def queue_xact_cmd(self, conn_id: ConnId, cmd: Command):
        self.xacts[conn_id].append(cmd)

    def pop_xact_for_exec(self, conn_id: ConnId) -> tuple[bool, list[Command]]:
        """Returns (if any key versions have changed, list of commands)"""
        has_any_key_version_changed = self.check_watched_keys(conn_id)
        if conn_id in self.watched_keys:
            self.clear_watched_keys(conn_id)
        return has_any_key_version_changed, self.xacts.pop(conn_id)

    def watch_key(self, conn_id: ConnId, key: bytes):
        self.watched_keys[conn_id][key] = self.key_versions[key]

    def clear_watched_keys(self, conn_id: ConnId):
        del self.watched_keys[conn_id]

    def check_watched_keys(self, conn_id: ConnId) -> bool:
        watched_versions = self.watched_keys.get(conn_id, {})
        for key, version in watched_versions.items():
            if self.key_versions.get(key, 0) != version:
                return True
        return False

    def in_subscribed_mode(self, conn_id: ConnId) -> bool:
        return conn_id in self.channels

    def subscribe(
        self, channel_name: bytes, conn: socket.socket, conn_id: ConnId
    ) -> int:
        """Subscribe to a channel, and return the number of channels the client is subscribed to"""
        self.channels[conn_id].add(channel_name)
        self.subscribers[channel_name].add((conn_id, conn))
        return len(self.channels[conn_id])

    def unsubscribe(
        self, channel_name: bytes, conn: socket.socket, conn_id: ConnId
    ) -> int:
        """Unubscribe from a channel, and return the number of channels the client is subscribed to"""
        if channel_name in self.channels[conn_id]:
            self.channels[conn_id].remove(channel_name)
            self.subscribers[channel_name].remove((conn_id, conn))
        return len(self.channels[conn_id])

    def get_subscribers(self, channel_name: bytes) -> set[tuple[ConnId, socket.socket]]:
        return self.subscribers[channel_name]

    def rpush(self, key: bytes, values: ListVal) -> int:
        if key not in self.store:
            self.store[key] = []
        elif key in self.store and self.key_types[key] != Database.ValType.LIST:
            raise Exception(f"Called rpush on a non list key {key=}")
        self.key_types[key] = Database.ValType.LIST
        cast(ListVal, self.store[key]).extend(values)
        self.list_notify_queue(key)
        return len(self.store[key])

    def lpush(self, key: bytes, values: ListVal) -> int:
        if key not in self.store:
            self.store[key] = []
        elif key in self.store and self.key_types[key] != Database.ValType.LIST:
            raise Exception(f"Called lpush on a non list key {key=}")
        self.key_types[key] = Database.ValType.LIST
        self.store[key] = values + cast(ListVal, self.store[key])
        self.list_notify_queue(key)
        return len(self.store[key])

    def list_notify_queue(self, key: bytes):
        cv = self.blpop_waitlist[key]
        with cv:
            cv.notify()

    def lpop(self, key: bytes) -> bytes:
        return self.get_list(key).pop(0)

    def lpop_multiple(self, key: bytes, count: int) -> ListVal:
        values = [self.lpop(key) for _ in range(count)]
        return values

    def blpop_timeout(self, key: bytes, timeout: float | None) -> bytes | None:
        """Blocks until the list is nonempty or timeout is reached"""
        if key in self.store and len(self.store[key]) != 0:
            return self.lpop(key)
        cv = self.blpop_waitlist[key]
        end_time = None if timeout is None else time.monotonic() + timeout
        with cv:
            while key not in self.store or len(self.store[key]) == 0:
                remaining = None if end_time is None else end_time - time.monotonic()
                if remaining is not None and remaining < 0:
                    # timed out
                    return None
                was_notified = cv.wait(remaining)
                if was_notified:
                    return self.lpop(key)

    def get_list(self, key: bytes) -> ListVal:
        if self.key_types[key] != Database.ValType.LIST:
            raise Exception("Called get_list on a non list key {key=}")
        return cast(ListVal, self.store[key])

    def key_exists(self, key: bytes) -> bool:
        return key in self.store or key in self.store

    def validate_stream_id(self, key: bytes, id: str) -> bytes | None:
        """Returns the error when validating the stream ID, if it exists"""
        if key not in self.store:
            return None
        key_type = self.key_types[key]
        value = self.store[key]
        if key_type != Database.ValType.STREAM:
            # should change this error message
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        value = cast(StreamVal, value)

        if id == "*":
            return None
        splitted = id.split("-")
        if len(splitted) != 2:
            # should change this one too
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        _, seq_no = splitted
        seq_no_is_star = seq_no == "*"
        stream_id = StreamId(id)
        if seq_no_is_star:
            return None
        is_0_0 = stream_id.milliseconds_time == "0" and stream_id.seq_no == "0"
        if is_0_0:
            return constants.STREAM_ID_TOO_SMALL_ERROR.encode()

        if not value:
            return None
        last_stream_id = value[-1][0]
        if stream_id <= last_stream_id:
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        return None

    def xadd(self, key: bytes, id: str, value: dict) -> str:
        # stream key has already been validated
        if key in self.key_types and self.key_types[key] != Database.ValType.STREAM:
            raise Exception(f"key {key} is not a stream")
        self.key_types[key] = Database.ValType.STREAM
        if key not in self.store:
            self.store[key] = []
        cur_value = cast(StreamVal, self.store[key])
        processed_id = StreamId.generate_stream_id(
            id, cur_value[-1][0] if cur_value else None
        )
        cur_value.append((processed_id, value))
        waitlist_lock, waitlist = self.stream_waitlist[key]
        with waitlist_lock:
            for sws in waitlist:
                sws.release()
        return str(processed_id)

    def xrange(self, key: bytes, start: str, end: str) -> list[bytes]:
        value = self.store[key]
        key_type = self.key_types[key]
        if key_type != Database.ValType.STREAM:
            return transform_to_execute_output(constants.XOP_ON_NON_STREAM_ERROR)
        value = cast(StreamVal, value)

        # support - and + queries
        if start == "-":
            start_stream_id = StreamId("0-1")
        elif "-" not in start:
            start_stream_id = StreamId(f"{start}-0")
        else:
            start_stream_id = StreamId(start)
        if end == "+":
            end_stream_id = (
                value[-1][0]
                if value
                else StreamId(
                    f"{constants.MAX_STREAM_ID_SEQ_NO}-{constants.MAX_STREAM_ID_SEQ_NO}"
                )
            )
        elif "-" not in end:
            end_stream_id = StreamId(f"{end}-{constants.MAX_STREAM_ID_SEQ_NO}")
        else:
            end_stream_id = StreamId(end)

        lo = bisect.bisect_right(value, start_stream_id, key=lambda x: x[0])
        if lo >= len(value):
            return transform_to_execute_output(constants.EMPTY_RESP_ARRAY)
        hi = bisect.bisect_right(value, end_stream_id, key=lambda x: x[0])
        if hi >= len(value):
            hi = len(value)

        res = []
        for i in range(lo - 1 if lo != 0 else 0, hi):
            flattened_kvs = [
                RespBulkString(item.encode())
                for items in value[i][1].items()
                for item in items
            ]
            res.append(
                RespArray(
                    [
                        RespBulkString(str(value[i][0]).encode()),
                        RespArray(flattened_kvs),
                    ]
                )
            )
        return RespArray(res).encode_to_list()

    def xread(
        self, stream_keys: list[bytes], ids: list[str], timeout: int | None
    ) -> list[bytes]:
        if timeout is not None:
            original_lens = [len(self.store[stream_key]) for stream_key in stream_keys]
            logger.info(f"{original_lens=}")
            if timeout != 0:
                time.sleep(timeout / 1e3)
            else:
                # wait until there is a new element
                # just need a map of stream_keys to threads to wake up
                new_lens = [len(self.store[stream_key]) for stream_key in stream_keys]
                any_new_elements = any(
                    original_lens[i] != new_lens[i] for i in range(len(original_lens))
                )
                if not any_new_elements:
                    sem = Semaphore(value=0)
                    for stream_key in stream_keys:
                        waitlist_lock, waitlist = self.stream_waitlist[stream_key]
                        with waitlist_lock:
                            waitlist.add(sem)
                    sem.acquire()
                logger.info(f"{new_lens=}")

        res = []
        for i in range(len(stream_keys)):
            stream_key = stream_keys[i]
            id = ids[i]
            value = self.store[stream_key]
            key_type = self.key_types[stream_key]
            if key_type != Database.ValType.STREAM:
                return transform_to_execute_output(constants.XOP_ON_NON_STREAM_ERROR)
            value = cast(StreamVal, value)
            if id == "$":
                logger.info(f"{original_lens[i]=}")
                id = str(value[original_lens[i] - 1][0]) if value else "0-0"
            stream_id = StreamId(id)

            lo = bisect.bisect_right(value, stream_id, key=lambda x: x[0])
            if lo >= len(value):
                return transform_to_execute_output(constants.NULL_ARRAY_RESP_STRING)

            inter: list[RespDataType] = []
            for i in range(lo, len(value)):
                flattened_kvs = []
                for k, v in value[i][1].items():
                    flattened_kvs.append(RespBulkString(k.encode()))
                    flattened_kvs.append(RespBulkString(v.encode()))
                inter.append(
                    RespArray(
                        [
                            RespBulkString(str(value[i][0]).encode()),
                            RespArray(flattened_kvs),
                        ]
                    )
                )
            res.append(
                RespArray(
                    [
                        RespBulkString(stream_key),
                        RespArray(inter),
                    ]
                )
            )
        return RespArray(res).encode_to_list()

    def zadd(self, key: bytes, score: float, name: bytes) -> int:
        if key not in self.store:
            self.store[key] = SortedSet()
            self.key_types[key] = Database.ValType.SET
        value = self.store[key]
        # zadd only works for set values
        key_type = self.key_types[key]
        if key_type == Database.ValType.SET:
            set_val = cast(SortedSet, value)
            return set_val.add(name, score)
        else:
            logger.error("tried to Database.zadd with non SET key")
            return 0

    def zrank(self, key: bytes, name: bytes) -> int:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return -1
        value = self.store[key]
        set_val = cast(SortedSet, value)
        return set_val.rank(name)

    def zrange(self, key: bytes, start: int, end: int) -> list[SortedSet.Item]:
        """[start:end+1], inclusive of end"""
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return []
        value = self.store[key]
        set_val = cast(SortedSet, value)
        if end == -1:
            end = len(set_val)
        else:
            end += 1
        return set_val.get_slice(start, end)

    def zcard(self, key: bytes) -> int:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return 0
        return len(self.store[key])

    def zscore(self, key: bytes, name: bytes) -> float:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return 0
        value = self.store[key]
        set_val = cast(SortedSet, value)
        return set_val.score(name)

    def zrem(self, key: bytes, name: bytes) -> int:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return 0
        value = self.store[key]
        set_val = cast(SortedSet, value)
        return set_val.remove(name)

    def geopos(
        self, key: bytes, members: list[bytes]
    ) -> list[tuple[float, float] | None]:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return [None for _ in members]
        value = self.store[key]
        set_val = cast(SortedSet, value)
        return [
            decode_score(int(set_val.score(member))) if member in set_val else None
            for member in members
        ]

    def geodist(self, key: bytes, place1: bytes, place2: bytes) -> float:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return 0
        value = self.store[key]
        set_val = cast(SortedSet, value)
        lon1, lat1 = decode_score(int(set_val.score(place1)))
        lon2, lat2 = decode_score(int(set_val.score(place2)))
        return haversines(lon1, lat1, lon2, lat2)

    def geosearch(
        self,
        key: bytes,
        longitude: float,
        latitude: float,
        radius: float,
    ) -> list[bytes]:
        if key not in self.store or self.key_types[key] != Database.ValType.SET:
            return []
        value = self.store[key]
        set_val = cast(SortedSet, value)
        result = []
        for value in set_val.set:
            value_lon, value_lat = decode_score(int(value.score))
            if haversines(longitude, latitude, value_lon, value_lat) <= radius:
                result.append(value.name)
        return result

    def get_passwords(self, user: bytes) -> list[bytes]:
        return self.passwords[user]

    def set_password(self, user: bytes, password: bytes, conn_id: ConnId):
        """Keeps you logged in after changing password"""
        hashed_password = hashlib.sha256(password).hexdigest().encode()
        self.passwords[user].append(hashed_password)
        retrieved_user, _ = self.authenticated_sessions[conn_id]
        # TODO: can we set passwords for other users? probably not
        # TODO: is this 'stay logged in while setting pw' only for default?
        if user == retrieved_user:
            self.authenticated_sessions[conn_id] = (user, hashed_password)

    def authenticate(self, user: bytes, password: bytes, conn_id: ConnId) -> bool:
        hashed_password = hashlib.sha256(password).hexdigest().encode()
        retrieved_passwords = self.get_passwords(user)
        is_authenticated = any(
            password == hashed_password for password in retrieved_passwords
        )
        if is_authenticated:
            self.authenticated_sessions[conn_id] = (user, hashed_password)
        return is_authenticated
