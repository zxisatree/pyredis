import bisect
from datetime import datetime
from enum import Enum
import functools
import os
import socket
from threading import Condition, Lock, Semaphore
import time
from typing import cast
from dataclasses import dataclass

import interfaces
import constants
from data_types import RespArray, RespBulkString, RespDataType
from logs import logger
import rdb
import singleton_meta
from utils import (
    ConnId,
    ThreadsafeDefaultdict,
    ThreadsafeDict,
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


class Database(metaclass=singleton_meta.SingletonMeta):
    StrVal = tuple[str, datetime | None]
    StreamVal = list[tuple["StreamId", dict[str, str]]]
    ListVal = list[bytes]

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

    def __init__(self, dir: str, dbfilename: str):
        self.store: ThreadsafeDict[
            bytes, Database.StrVal | Database.StreamVal | Database.ListVal | SortedSet
        ] = ThreadsafeDict()
        self.key_types: ThreadsafeDict[bytes, Database.ValType] = ThreadsafeDict()
        # map of keys of streams to threads waiting for new elements
        self.stream_waitlist: ThreadsafeDefaultdict[
            bytes, tuple[Lock, set[Semaphore]]
        ] = ThreadsafeDefaultdict(lambda: (Lock(), set()))
        # map of keys of lists to threads waiting for new elements
        self.blpop_waitlist: ThreadsafeDefaultdict[bytes, Condition] = (
            ThreadsafeDefaultdict(Condition)
        )
        # xacts can only be started explicitly through a single function, so we avoid the overhead of a defaultdict here
        self.xacts: ThreadsafeDict[ConnId, list[interfaces.Command]] = ThreadsafeDict()
        self.channels: ThreadsafeDefaultdict[ConnId, set[bytes]] = (
            ThreadsafeDefaultdict(set)
        )
        self.subscribers: ThreadsafeDefaultdict[
            bytes, set[tuple[ConnId, socket.socket]]
        ] = ThreadsafeDefaultdict(set)

        self.dir = dir
        self.dbfilename = dbfilename
        file_path = os.path.join(self.dir, self.dbfilename)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                self.rdb = rdb.RdbFile(f.read())
        else:
            self.rdb = rdb.RdbFile(constants.EMPTY_RDB_FILE)
        self.init_from_rdb(self.rdb)
        logger.info(f"db initialised with {self.store=}")

    def init_from_rdb(self, rdb_file: rdb.RdbFile):
        for key, value in rdb_file.key_values.items():
            self.store[key] = value
            self.key_types[key] = Database.ValType.STRING  # only support strings in RDB

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
                (str_val, expiry) = cast(Database.StrVal, value)
                if expiry and self.expire_one(key):
                    return None
                return str_val
            case Database.ValType.LIST:
                list_val = cast(Database.StreamVal, value)
                return list_val
            case Database.ValType.STREAM:
                stream_val = cast(Database.StreamVal, value)
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

    def set_string_value(self, key: bytes, value: StrVal):
        self.key_types[key] = Database.ValType.STRING
        self.store[key] = value

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
            string_val = cast(Database.StrVal, value)
            return string_val[1]
        else:
            raise Exception(f"Called get_expiry on a non string key {key=}")

    def expire_one(self, key: bytes) -> bool:
        # returns True if key was expired
        value = self.store[key]
        key_type = self.key_types[key]
        if key_type != Database.ValType.STRING:
            return False
        _, expiry = cast(Database.StrVal, value)
        if expiry and expiry < datetime.now():
            del self.store[key]
            return True
        return False

    def start_xact(self, conn_id: ConnId):
        self.xacts[conn_id] = []

    def xact_exists(self, conn_id: ConnId) -> bool:
        return conn_id in self.xacts

    def queue_xact_cmd(self, conn_id: ConnId, cmd: interfaces.Command):
        self.xacts[conn_id].append(cmd)

    def exec_xact(self, conn_id: ConnId) -> list[interfaces.Command]:
        res = self.xacts.pop(conn_id)
        return res

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
        cast(Database.ListVal, self.store[key]).extend(values)
        self.list_notify_queue(key)
        return len(self.store[key])

    def lpush(self, key: bytes, values: ListVal) -> int:
        if key not in self.store:
            self.store[key] = []
        elif key in self.store and self.key_types[key] != Database.ValType.LIST:
            raise Exception(f"Called lpush on a non list key {key=}")
        self.key_types[key] = Database.ValType.LIST
        self.store[key] = values + cast(Database.ListVal, self.store[key])
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
        return cast(Database.ListVal, self.store[key])

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
        value = cast(Database.StreamVal, value)

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
        cur_value = cast(Database.StreamVal, self.store[key])
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
        value = cast(Database.StreamVal, value)

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
            value = cast(Database.StreamVal, value)
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


@functools.total_ordering
class StreamId:
    """ID of a stream entry"""

    def __init__(self, id_str: str):
        milliseconds_time, seq_no = id_str.split("-")
        self.milliseconds_time = milliseconds_time
        self.seq_no = seq_no

    def __repr__(self) -> str:
        return f"StreamId({self.milliseconds_time}-{self.seq_no})"

    def __str__(self) -> str:
        return f"{self.milliseconds_time}-{self.seq_no}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, StreamId):
            return False
        return (
            self.milliseconds_time == other.milliseconds_time
            and self.seq_no == other.seq_no
        )

    def __lt__(self, other: "StreamId"):
        if self.milliseconds_time != other.milliseconds_time:
            return self.milliseconds_time < other.milliseconds_time
        return self.seq_no < other.seq_no

    @staticmethod
    def generate_stream_id(id: str, last_id: "StreamId | None") -> "StreamId":
        if id == "*":
            # milliseconds_time should be current time in milliseconds
            milliseconds_time = str(int(datetime.now().timestamp() * 1000))
            if not last_id:
                return StreamId(f"{milliseconds_time}-0")
            if last_id.milliseconds_time == milliseconds_time:
                return last_id.next_seq_id()
            return StreamId(f"{milliseconds_time}-0")

        splitted = id.split("-")
        if len(splitted) != 2:
            raise Exception(f"Invalid stream id {id}")
        milliseconds_time, seq_no = splitted
        if not last_id:
            if seq_no == "*":
                seq_no = "1" if milliseconds_time == "0" else "0"
            return StreamId(f"{milliseconds_time}-{seq_no}")

        if seq_no == "*":
            if milliseconds_time == last_id.milliseconds_time:
                seq_no = str(int(last_id.seq_no) + 1)
            else:
                seq_no = "1" if milliseconds_time == "0" else "0"
        return StreamId(f"{milliseconds_time}-{seq_no}")

    def next_seq_id(self) -> "StreamId":
        return StreamId(f"{self.milliseconds_time}-{int(self.seq_no) + 1}")
