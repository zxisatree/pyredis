from collections import defaultdict
from math import radians, sin, cos, sqrt, asin
import socket
from threading import Lock, RLock
from typing import Generic, TypeVar

import constants

ConnId = tuple[int, str]


def construct_conn_id(conn: socket.socket) -> ConnId:
    return (conn.fileno(), conn.getsockname())


def transform_to_execute_output(single_result: str) -> list[bytes]:
    return [single_result.encode()]


def encode_score(longitude: float, latitude: float):
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


def decode_score(score: int) -> tuple[float, float]:
    # score is passed as an int, but used as a float here
    y = score >> 1
    x = score
    grid_latitude_number = deinterleave64(x)
    grid_longitude_number = deinterleave64(y)
    grid_latitude_min = constants.MIN_LATITUDE + constants.LATITUDE_RANGE * (
        grid_latitude_number / (2**26)
    )
    grid_latitude_max = constants.MIN_LATITUDE + constants.LATITUDE_RANGE * (
        (grid_latitude_number + 1) / (2**26)
    )
    grid_longitude_min = constants.MIN_LONGITUDE + constants.LONGITUDE_RANGE * (
        grid_longitude_number / (2**26)
    )
    grid_longitude_max = constants.MIN_LONGITUDE + constants.LONGITUDE_RANGE * (
        (grid_longitude_number + 1) / (2**26)
    )

    # Calculate the center point of the grid cell
    latitude = (grid_latitude_min + grid_latitude_max) / 2
    longitude = (grid_longitude_min + grid_longitude_max) / 2
    return (longitude, latitude)


# taken from Redis
def interleave64(v: int) -> int:
    v = v & 0xFFFFFFFF
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555
    return v


def deinterleave64(v: int) -> int:
    v = v & 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return v


def haversines(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    a = sin(dLat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dLon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return constants.EARTH_RADIUS * c


KT = TypeVar("KT")
VT = TypeVar("VT")


class ThreadsafeDict(dict, Generic[KT, VT]):
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
        # dict methods should be atomic
        return super().__contains__(key)
        # with self.lock:
        #     return super().__contains__(key)

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


class ThreadsafeDefaultdict(defaultdict, Generic[KT, VT]):
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
        # dict methods should be atomic
        return super().__contains__(key)
        # with self.lock:
        #     return super().__contains__(key)

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


# class Node:
#     def __init__(
#         self,
#         name: bytes,
#         score: float,
#         left: "Self | None",
#         right: "Self | None",
#         is_black: bool,
#     ):
#         self.name = name
#         self.score = score
#         if left is not None:
#             self.left = left
#         if right is not None:
#             self.right = right
#         self.is_black = True
#         self.parent: "Self" = self


# class SortedSet:
#     # Red black tree with unique keys
#     def __init__(self):
#         self.root = None
#         self.name_map = {}

#     def insert(self, node: Node):
#         self.name_map[node.name] = node
#         if self.root is None:
#             self.root = node
#         node.is_black = False  # new nodes are red
#         return self._insert(self.root, node)

#     def _insert(self, ref: Node, node: Node):
#         if node.score > ref.score:
#             if not ref.right:
#                 node.parent = ref
#                 ref.right = node
#                 if not ref.is_black:
#                     self._fix_violations(ref)
#             else:
#                 return self._insert(ref.right, node)
#         elif node.score < ref.score:
#             if not ref.left:
#                 node.parent = ref
#                 ref.left = node
#                 if not ref.is_black:
#                     self._fix_violations(ref)
#             else:
#                 return self._insert(ref.left, node)
#         else:
#             raise NotImplementedError("SortedSet should have unique names and scores")

#     def _fix_violations(self, node: Node):
#         pass

#     def delete(self, name: bytes):
#         pass
#         # node = self.name_map[name]
#         # parent = node.parent
#         # if parent.left == node:
#         #     # need to connect to node's children
#         #     parent.left = None
#         # if parent.right == node:
#         #     parent.right = None

#     def search(self, name: bytes):
#         if name not in self.name_map:
#             return None
#         return self.name_map[name]
