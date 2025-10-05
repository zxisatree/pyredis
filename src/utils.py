from collections import defaultdict
import socket
from threading import Lock, RLock

import constants

ConnId = tuple[int, str]


def construct_conn_id(conn: socket.socket) -> ConnId:
    return (conn.fileno(), conn.getsockname())


def transform_to_execute_output(single_result: str) -> list[bytes]:
    return [single_result.encode()]


def calculate_score(latitude: float, longitude: float) -> float:
    normalized_latitude = int(
        2**26 * (latitude - constants.MIN_LATITUDE) / constants.LATITUDE_RANGE
    )
    normalized_longitude = int(
        2**26 * (longitude - constants.MIN_LONGITUDE) / constants.LONGITUDE_RANGE
    )
    interleaved_latitude = interleave64(normalized_latitude)
    interleaved_longitude = interleave64(normalized_longitude)
    longitude_shifted = interleaved_longitude << 1
    return interleaved_latitude | longitude_shifted


# taken from Redis
def interleave64(v: int) -> int:
    v = v & 0xFFFFFFFF
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555
    return v


class ThreadsafeDict[KT, VT](dict):
    """Coarse locking wrapper over a dict"""

    def __init__(self, *args, **kwargs):
        self.lock = Lock()
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: KT) -> VT:
        with self.lock:
            return super().__getitem__(key)

    def __setitem__(self, key: KT, value: VT):
        with self.lock:
            return super().__setitem__(key, value)

    def __contains__(self, key: KT) -> bool:
        with self.lock:
            return super().__contains__(key)

    def __len__(self) -> int:
        with self.lock:
            return super().__len__()

    def __delitem__(self, key: KT):
        with self.lock:
            return super().__delitem__(key)

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return f"ThreadsafeDict{super().__repr__()}"


class ThreadsafeDefaultdict[KT, VT](defaultdict):
    """Coarse locking wrapper over a defaultdict"""

    def __init__(self, *args, **kwargs):
        # defaultdict might call __setitem__ in __getitem__
        self.lock = RLock()
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: KT) -> VT:
        with self.lock:
            return super().__getitem__(key)

    def __setitem__(self, key: KT, value: VT):
        with self.lock:
            return super().__setitem__(key, value)

    def __contains__(self, key: KT) -> bool:
        with self.lock:
            return super().__contains__(key)

    def __len__(self) -> int:
        with self.lock:
            return super().__len__()

    def __delitem__(self, key: KT):
        with self.lock:
            return super().__delitem__(key)

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return f"ThreadsafeDefaultdict{super().__repr__()}"
