import bisect
from datetime import datetime
import functools
from threading import RLock
import time
import os

import constants
import data_types
from logs import logger
import rdb
import singleton_meta


class Database(metaclass=singleton_meta.SingletonMeta):
    lock = RLock()
    store_str_val_type = tuple[str, datetime | None]
    store_stream_val_type = list[tuple["StreamId", dict[str, str]]]
    store: dict[str, store_str_val_type | store_stream_val_type] = {}

    def __init__(self, dir: str, dbfilename: str):
        self.dir = dir
        self.dbfilename = dbfilename
        file_path = os.path.join(self.dir, self.dbfilename)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                self.rdb = rdb.RdbFile(f.read())
        else:
            self.rdb = rdb.RdbFile(constants.EMPTY_RDB_FILE)
        for key, value in self.rdb.key_values.items():
            self.store[key] = value
        logger.info(f"db initialised with {self.store=}")

    def __len__(self) -> int:
        with self.lock:
            return len(self.store)

    def __getitem__(self, key: str) -> str | store_stream_val_type | None:
        with self.lock:
            if key not in self.store:
                return None
            value = self.store[key]
            if isinstance(value, list):
                return value
            if value[1] and self.expire_one(key):
                return None
            return value[0]

    def __setitem__(self, key: str, value: store_str_val_type):
        with self.lock:
            self.store[key] = value

    def __delitem__(self, key: str):
        with self.lock:
            del self.store[key]

    def __contains__(self, key: str) -> bool:
        with self.lock:
            return key in map(lambda x: x, self.store.keys())

    def __str__(self) -> str:
        with self.lock:
            return str(self.store)

    def __repr__(self) -> str:
        with self.lock:
            return f"Store({repr(self.store)})"

    def get_type(self, key: str) -> str:
        with self.lock:
            if key not in self.store:
                return "none"
            value = self.store[key]
            if isinstance(value, list):
                return "stream"
            return "string"

    def expire(self):
        with self.lock:
            for key, value in self.store.items():
                if not isinstance(value, list):
                    _, expiry = value
                    if expiry and expiry < datetime.now():
                        del self.store[key]

    def expire_one(self, key: str) -> bool:
        # returns True if key was expired
        with self.lock:
            value = self.store[key]
            if isinstance(value, list):
                return False
            expiry = value[1]
            if expiry and expiry < datetime.now():
                del self.store[key]
                return True
            return False

    def validate_stream_id(self, key: str, id: str) -> bytes | None:
        with self.lock:
            if key not in self.store:
                return None
            cur_value = self.store[key]
            if not isinstance(cur_value, list):
                return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
            if id == "*":
                return None
            milliseconds_time, seq_no = id.split("-")
            seq_no_is_star = seq_no == "*"

            is_0_0 = milliseconds_time == "0" and seq_no == "0"
            if is_0_0:
                return constants.STREAM_ID_TOO_SMALL_ERROR.encode()
            if not cur_value:
                return None
            last_mst, last_seq_no = str(cur_value[-1][0]).split("-")
            if int(milliseconds_time) < int(last_mst):
                return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
            elif seq_no_is_star:
                return None
            elif int(milliseconds_time) == int(last_mst) and int(seq_no) <= int(
                last_seq_no
            ):
                return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
            return None

    def xadd(self, key: str, id: str, value: dict) -> str:
        # stream key has already been validated
        with self.lock:
            if key not in self.store:
                self.store[key] = []
            cur_value = self.store[key]
            if not isinstance(cur_value, list):
                raise Exception(f"key {key} is not a stream")
            processed_id = StreamId.generate_stream_id(
                id, str(cur_value[-1][0]) if cur_value else None
            )
            cur_value.append((processed_id, value))
            return str(processed_id)

    def xrange(self, key: str, start: str, end: str) -> bytes:
        with self.lock:
            value = self.store[key]
            if not isinstance(value, list):
                return constants.XOP_ON_NON_STREAM_ERROR.encode()
            if start == "-":
                start = "0-1"
            elif "-" not in start:
                start = f"{start}-0"
            if end == "+":
                end = (
                    str(value[-1][0])
                    if value
                    else f"{constants.MAX_STREAM_ID_SEQ_NO}-{constants.MAX_STREAM_ID_SEQ_NO}"
                )
            elif "-" not in end:
                end = f"{end}-{constants.MAX_STREAM_ID_SEQ_NO}"
            start_stream_id = StreamId(start)
            end_stream_id = StreamId(end)

            lo = bisect.bisect_right(value, start_stream_id, key=lambda x: x[0])
            if lo >= len(value):
                return data_types.RespArray([]).encode()
            hi = bisect.bisect_right(value, end_stream_id, key=lambda x: x[0])
            if hi >= len(value):
                hi = len(value)

            res = []
            for i in range(lo - 1 if lo != 0 else 0, hi):
                flattened_kvs = []
                for k, v in value[i][1].items():
                    flattened_kvs.append(data_types.RespBulkString(k.encode()))
                    flattened_kvs.append(data_types.RespBulkString(v.encode()))
                res.append(
                    data_types.RespArray(
                        [
                            data_types.RespBulkString(str(value[i][0]).encode()),
                            data_types.RespArray(flattened_kvs),
                        ]
                    )
                )
            return data_types.RespArray(res).encode()

    def xread(
        self, stream_keys: list[str], ids: list[str], timeout: int | None
    ) -> bytes:
        if timeout is not None:
            with self.lock:
                original_lens = [
                    len(self.store[stream_key]) for stream_key in stream_keys
                ]
            logger.info(f"{original_lens=}")
            if timeout != 0:
                time.sleep(timeout / 1e3)
            else:
                while True:
                    time.sleep(0.5)
                    with self.lock:
                        new_lens = [
                            len(self.store[stream_key]) for stream_key in stream_keys
                        ]
                        to_break = False
                        for i in range(len(original_lens)):
                            if new_lens[i] != original_lens[i]:
                                to_break = True
                        if to_break:
                            logger.info(f"{new_lens=}")
                            break

        with self.lock:
            res = []
            for i in range(len(stream_keys)):
                stream_key = stream_keys[i]
                id = ids[i]
                value = self.store[stream_key]
                if not isinstance(value, list):
                    return constants.XOP_ON_NON_STREAM_ERROR.encode()
                if id == "$":
                    logger.info(f"{original_lens[i]}")
                    id = str(value[original_lens[i] - 1][0]) if value else "0-0"
                stream_id = StreamId(id)

                lo = bisect.bisect_right(value, stream_id, key=lambda x: x[0])
                if lo >= len(value):
                    return constants.NULL_BULK_RESP_STRING.encode()

                inter: list[data_types.RespDataType] = []
                for i in range(lo, len(value)):
                    flattened_kvs = []
                    for k, v in value[i][1].items():
                        flattened_kvs.append(data_types.RespBulkString(k.encode()))
                        flattened_kvs.append(data_types.RespBulkString(v.encode()))
                    inter.append(
                        data_types.RespArray(
                            [
                                data_types.RespBulkString(str(value[i][0]).encode()),
                                data_types.RespArray(flattened_kvs),
                            ]
                        )
                    )
                res.append(
                    data_types.RespArray(
                        [
                            data_types.RespBulkString(stream_key.encode()),
                            data_types.RespArray(inter),
                        ]
                    )
                )
            return data_types.RespArray(res).encode()


@functools.total_ordering
class StreamId:
    def __init__(self, id_str: str):
        milliseconds_time, seq_no = id_str.split("-")
        self.validate(milliseconds_time, seq_no)
        self.milliseconds_time = milliseconds_time
        self.seq_no = seq_no

    def validate(self, milliseconds_time: str, seq_no: str) -> bool:
        if milliseconds_time == "0" and seq_no == "0":
            logger.info(f"Invalid stream id {milliseconds_time}-{seq_no}")
            return False
        return True

    @staticmethod
    def generate_stream_id(id: str, last_id: str | None) -> "StreamId":
        if id == "*":
            # milliseconds_time should be current time in milliseconds
            milliseconds_time = str(int(datetime.now().timestamp() * 1000))
            if not last_id:
                return StreamId(f"{milliseconds_time}-0")
            splitted_last = last_id.split("-")
            if splitted_last[0] == milliseconds_time:
                return StreamId(f"{milliseconds_time}-{int(splitted_last[1]) + 1}")
            return StreamId(f"{milliseconds_time}-0")

        splitted = id.split("-")
        if len(splitted) != 2:
            raise Exception(f"Invalid stream id {id}")
        milliseconds_time, seq_no = splitted
        if not last_id:
            if seq_no == "*":
                seq_no = "1" if milliseconds_time == "0" else "0"
            return StreamId(f"{milliseconds_time}-{seq_no}")

        splitted_last = last_id.split("-")
        last_milliseconds_time, last_seq_no = splitted_last
        if seq_no == "*":
            if milliseconds_time == last_milliseconds_time:
                seq_no = str(int(last_seq_no) + 1)
            else:
                seq_no = "1" if milliseconds_time == "0" else "0"
        return StreamId(f"{milliseconds_time}-{seq_no}")

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

    def next_seq_id(self) -> "StreamId":
        return StreamId(f"{self.milliseconds_time}-{int(self.seq_no) + 1}")
