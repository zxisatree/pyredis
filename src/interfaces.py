from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from functools import total_ordering
from typing import TYPE_CHECKING

from .exceptions import ExecuteForAofError

if TYPE_CHECKING:
    import socket

    import database
    import replicas


class XactBehaviour(Enum):
    QUEUE = "queue"
    EXECUTE = "execute"
    ERROR = "error"


@total_ordering
class StreamId:
    """ID of a stream entry"""

    def __init__(self, id_str: str):
        milliseconds_time, seq_no = id_str.split("-")
        self.milliseconds_time = int(milliseconds_time)
        self.seq_no = int(seq_no)

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

    # def __lt__(self, other: "StreamId"):
    #     if self.milliseconds_time != other.milliseconds_time:
    #         return int(self.milliseconds_time) < int(other.milliseconds_time)
    #     return int(self.seq_no) < int(other.seq_no)

    @staticmethod
    def generate_stream_id(id: str, last_id: "StreamId | None") -> "StreamId":
        if id == "*":
            # milliseconds_time should be current time in milliseconds
            milliseconds_time = int(datetime.now().timestamp() * 1000)
            if not last_id:
                return StreamId(f"{milliseconds_time}-0")
            if last_id.milliseconds_time == milliseconds_time:
                return last_id.next_seq_id()
            return StreamId(f"{milliseconds_time}-0")

        splitted = id.split("-")
        if len(splitted) != 2:
            raise Exception(f"Invalid stream id {id}")
        milliseconds_time = int(splitted[0])
        seq_no_raw = splitted[1]
        # placeholder, is overwritten if seq_no_raw is indeed "*"
        seq_no = int(splitted[1]) if seq_no_raw != "*" else 0
        if not last_id:
            if seq_no_raw == "*":
                seq_no = 1 if milliseconds_time == 0 else 0
            return StreamId(f"{milliseconds_time}-{seq_no}")

        if seq_no_raw == "*":
            if milliseconds_time == last_id.milliseconds_time:
                seq_no = last_id.seq_no + 1
            else:
                seq_no = 1 if milliseconds_time == 0 else 0
        return StreamId(f"{milliseconds_time}-{seq_no}")

    def next_seq_id(self) -> "StreamId":
        return StreamId(f"{self.milliseconds_time}-{int(self.seq_no) + 1}")


StrVal = tuple[str, datetime | None]
StreamVal = list[tuple[StreamId, dict[str, str]]]
ListVal = list[bytes]


class Command(ABC):
    allowed_in_subscribed_mode = False
    xact_behaviour = XactBehaviour.QUEUE
    allowed_while_unauthenticated = False
    should_propogate_to_replicas = False
    should_write_to_aof = False

    def __init__(self):
        self._raw_cmd = b""
        # self._keyword = b""

    # replicas require this, but there's no good way to enforce properties on subclasses. It's either this with bad developer experience (need to write self._raw_cmd instead of self.raw_cmd in __init__) or runtime checks (slow, use reflection)
    @property
    def raw_cmd(self) -> bytes:
        return self._raw_cmd

    @property
    def keyword(self) -> bytes:
        return self.__class__.__name__.replace("Command", "").lower().encode()
        # return self._keyword

    @abstractmethod
    def execute(
        self,
        db: database.Database,
        replica_handler: replicas.ReplicaHandler,
        conn: socket.socket,
    ) -> list[bytes]: ...

    def execute_for_aof(self, db: database.Database) -> list[bytes]:
        """Only for commands read from AOF file which do not require the replica_handler or conn information"""
        if self.should_write_to_aof:
            raise ExecuteForAofError(
                f"For {self.keyword} commands, {self.should_write_to_aof=}, but execute_for_aof is not defined"
            )
        else:
            raise ExecuteForAofError(
                f"For {self.keyword} commands, {self.should_write_to_aof=}, execute_for_aof is not allowed"
            )

    # @classmethod
    # @abstractmethod
    # # might raise RequestCraftError
    # def craft_request(cls, *args: str) -> Self: ...
