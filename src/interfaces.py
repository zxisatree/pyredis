from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
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
    propogated_to_replicas = False

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

    # @classmethod
    # @abstractmethod
    # # might raise RequestCraftError
    # def craft_request(cls, *args: str) -> Self: ...
