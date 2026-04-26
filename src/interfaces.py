from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
import exceptions
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import socket

    import database
    import replicas


class XactBehaviour(Enum):
    QUEUE = "queue"
    EXECUTE = "execute"
    ERROR = "error"


class Command(ABC):
    allowed_in_subscribed_mode = False
    xact_behaviour = XactBehaviour.QUEUE
    allowed_while_unauthenticated = False
    should_propogate_to_replicas = False
    should_write_to_aof = False

    def __init__(self):
        self._raw_cmd = b""
        self._keyword = b""

    # replicas require this, but there's no good way to enforce properties on subclasses. It's either this with bad developer experience (need to write self._raw_cmd instead of self.raw_cmd in __init__) or runtime checks (slow, use reflection)
    @property
    def raw_cmd(self) -> bytes:
        return self._raw_cmd

    @property
    def keyword(self) -> bytes:
        return self._keyword

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
            raise exceptions.ExecuteForAofError(
                f"For {self.keyword} commands, {self.should_write_to_aof=}, but execute_for_aof is not defined"
            )
        else:
            raise exceptions.ExecuteForAofError(
                f"For {self.keyword} commands, {self.should_write_to_aof=}, execute_for_aof is not allowed"
            )

    # @classmethod
    # @abstractmethod
    # # might raise RequestCraftError
    # def craft_request(cls, *args: str) -> Self: ...
