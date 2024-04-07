from abc import ABC, abstractmethod

import codec
import constants
import exceptions
from logs import logger
import rdb


class RespDataType(ABC):
    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def encode(self) -> bytes: ...

    @staticmethod
    @abstractmethod
    # Returns the parsed object and the new pos
    def decode(data: bytes, pos: int) -> tuple["RespDataType", int]: ...

    @staticmethod
    @abstractmethod
    def validate(that) -> "RespDataType": ...


class RespSimpleString(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespSimpleString({repr(self.data)})"

    def encode(self) -> bytes:
        return b"+" + self.data + b"\r\n"

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespSimpleString", int]:
        start = pos
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP simple string, missing \\r\\n separator")
        simple_str = data[start:pos]
        pos += 2
        assert pos <= len(data)
        return (RespSimpleString(simple_str), pos)

    @staticmethod
    def validate(that) -> "RespSimpleString":
        if not isinstance(that, RespSimpleString):
            raise exceptions.ValidationError(
                f"Expected RespSimpleString, got {type(that)}"
            )
        return that


class RespArray(RespDataType):
    def __init__(self, elements: list[RespDataType]):
        self.elements = elements

    def __len__(self) -> int:
        return len(self.elements)

    def __getitem__(self, idx) -> list[RespDataType] | RespDataType:
        res = self.elements.__getitem__(idx)
        if isinstance(res, list):
            return list(res)  # enables type hinting
        else:
            return res

    def __setitem__(self, idx, value: RespDataType):
        self.elements.__setitem__(idx, value)

    def __delitem__(self, idx):
        self.elements.__delitem__(idx)

    def __str__(self) -> str:
        return str(self.elements)

    def __repr__(self) -> str:
        return f"RespArray({repr(self.elements)})"

    def encode(self) -> bytes:
        return f"*{len(self.elements)}\r\n".encode() + b"".join(
            map(lambda x: x.encode(), self.elements)
        )

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespArray", int]:
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP array, missing \\r\\n separator")
        array_len = int(data[start:pos])
        pos += 2

        elements: list[RespDataType] = []
        for _ in range(array_len):
            element, pos = codec.dispatch(data, pos)
            elements.append(element)
        assert pos <= len(data)
        return (RespArray(elements), pos)

    @staticmethod
    def validate(that) -> "RespArray":
        if not isinstance(that, RespArray):
            raise exceptions.ValidationError(f"Expected RespArray, got {type(that)}")
        return that


class RespBulkString(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespBulkString({repr(self.data)})"

    def encode(self) -> bytes:
        return (
            f"${len(self.data)}\r\n".encode() + self.data + b"\r\n"
            if self.data
            else constants.NULL_BULK_RESP_STRING.encode()
        )

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespBulkString", int]:
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP bulk string, missing \\r\\n separator")
        bulk_str_len = int(data[start:pos])
        pos += 2

        bulk_str = data[pos : pos + bulk_str_len]
        pos += bulk_str_len + 2
        assert pos <= len(data)
        return (RespBulkString(bulk_str), pos)

    @staticmethod
    def validate(that) -> "RespBulkString":
        if not isinstance(that, RespBulkString):
            raise exceptions.ValidationError(
                f"Expected RespBulkString, got {type(that)}"
            )
        return that

    @staticmethod
    def safe_validate(that) -> tuple["RespBulkString", None] | tuple[None, str]:
        if not isinstance(that, RespBulkString):
            return that, f"Expected RespBulkString, got {type(that)}"
        return that, None


class RespInteger(RespDataType):
    def __init__(self, val: int):
        self.val = val

    def __len__(self) -> int:
        return len(str(self.val))

    def __str__(self) -> str:
        return str(self.val)

    def __repr__(self) -> str:
        return f"RespInteger({repr(self.val)})"

    def encode(self) -> bytes:
        return f":{self.val}\r\n".encode()

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespInteger", int]:
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP integer, missing \\r\\n separator")
        val = int(data[start:pos])
        pos += 2
        assert pos <= len(data)
        return (RespInteger(val), pos)

    @staticmethod
    def validate(that) -> "RespInteger":
        if not isinstance(that, RespInteger):
            raise exceptions.ValidationError(f"Expected RespInteger, got {type(that)}")
        return that


class RespSimpleError(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespSimpleError({repr(self.data)})"

    def encode(self) -> bytes:
        return b"-" + self.data + b"\r\n"

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespSimpleError", int]:
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP simple error, missing \\r\\n separator")
        simple_err = data[start:pos]
        pos += 2
        assert pos <= len(data)
        return (RespSimpleError(simple_err), pos)

    @staticmethod
    def validate(that) -> "RespSimpleError":
        if not isinstance(that, RespSimpleError):
            raise exceptions.ValidationError(
                f"Expected RespSimpleError, got {type(that)}"
            )
        return that


class RespRdbFile(RespDataType):
    def __init__(self, data: bytes):
        self.data = rdb.RdbFile(data)

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RdbFile({repr(self.data)})"

    def encode(self) -> bytes:
        return f"${len(self.data)}\r\n".encode() + self.data.data

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespRdbFile", int]:
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RDB file, missing \\r\\n separator")
        bulk_str_len = int(data[start:pos])
        pos += 2

        bulk_str = data[pos : pos + bulk_str_len]
        pos += bulk_str_len
        assert pos <= len(data)
        return (RespRdbFile(bulk_str), pos)

    @staticmethod
    def validate(that) -> "RespRdbFile":
        if not isinstance(that, RespRdbFile):
            raise exceptions.ValidationError(f"Expected RdbFile, got {type(that)}")
        return that


def decode_bulk_string_or_rdb(data: bytes, pos: int) -> tuple[RespDataType, int]:
    # check if the length ends with a sep
    orig = pos
    start = pos + 1
    while pos < len(data) and not is_sep(data, pos):
        pos += 1
    if pos >= len(data):
        logger.info("Invalid bulk string/RDB file, missing \\r\\n separator")
    bulk_str_len = int(data[start:pos])
    pos += 2 + bulk_str_len
    if is_sep(data, pos):
        return RespBulkString.decode(data, orig)
    else:
        return RespRdbFile.decode(data, orig)


def is_sep(data: bytes, pos: int) -> bool:
    # using slices to index data to get bytes instead of ints
    return (
        pos + 1 < len(data)
        and data[pos : pos + 1] == b"\r"
        and data[pos + 1 : pos + 2] == b"\n"
    )
