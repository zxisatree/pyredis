from abc import ABC, abstractmethod
from typing import Self, Sequence, cast

from . import constants
from .exceptions import ParseError, ValidationError
from .logs import logger
from .rdb import RdbParser


class RespDataType(ABC):
    @abstractmethod
    def encode(self) -> bytes: ...

    def encode_to_list(self) -> list[bytes]:
        return [self.encode()]

    @classmethod
    @abstractmethod
    # Returns the parsed object and the new pos
    def decode(cls, data: bytes, pos: int) -> tuple[Self, int]: ...

    @classmethod
    @abstractmethod
    def validate(cls, that) -> Self: ...


class RespPlainString(RespDataType):
    """Not an actual RESP data type, only used to avoid double encoding elements in arrays for MULTI/EXEC"""

    def __init__(self, data: bytes):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data.decode())

    def __repr__(self):
        return f"RespPlainWrapper({repr(self.data)})"

    def encode(self):
        return self.data

    @classmethod
    def decode(cls, data, pos):
        raise NotImplementedError(
            "RespPlainWrapper cannot be decoded, is not a RESP data type"
        )

    @classmethod
    def validate(cls, that):
        raise NotImplementedError("RespPlainWrapper is not a RESP data type")


class RespSimpleString(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data.decode())

    def __repr__(self):
        return f"RespSimpleString({repr(self.data)})"

    def encode(self):
        return b"+" + self.data + b"\r\n"

    @classmethod
    def decode(cls, data, pos):
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP simple string, missing \\r\\n separator")
        simple_str = data[start:pos]
        pos += 2
        assert pos <= len(data)
        return (RespSimpleString(simple_str), pos)

    @classmethod
    def validate(cls, that):
        if not isinstance(that, RespSimpleString):
            raise ValidationError(f"Expected RespSimpleString, got {type(that)}")
        return that


class RespArray(RespDataType):
    # Sequence is covariant, list is invariant
    def __init__(self, elements: Sequence[RespDataType] | None):
        self.is_null_array = elements is None
        if not self.is_null_array:
            self.elements = cast(Sequence[RespDataType], elements)
        else:
            self.elements = []

    def __len__(self):
        if self.is_null_array:
            return -1
        return len(self.elements)

    def __getitem__(self, idx) -> list[RespDataType] | RespDataType:
        res = self.elements.__getitem__(idx)
        if isinstance(res, list):
            return list(res)  # enables type hinting
        else:
            return res

    def __str__(self):
        if self.is_null_array:
            return "NULL_ARRAY"
        return "\n".join(str(element) for element in self.elements)

    def __repr__(self):
        if self.is_null_array:
            return "RespArray(None)"
        return f"RespArray({repr(self.elements)})"

    def encode(self):
        if self.is_null_array:
            return constants.NULL_ARRAY_RESP_STRING.encode()
        return f"*{len(self.elements)}\r\n".encode() + b"".join(
            map(lambda x: x.encode(), self.elements)
        )

    @classmethod
    def decode(cls, data, pos):
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP array, missing \\r\\n separator")
        array_len = int(data[start:pos])
        pos += 2

        elements: list[RespDataType] = []
        for _ in range(array_len):
            element, pos = dispatch(data, pos)
            elements.append(element)
        assert pos <= len(data)
        return (RespArray(elements), pos)

    @classmethod
    def validate(cls, that):
        if not isinstance(that, RespArray):
            raise ValidationError(f"Expected RespArray, got {type(that)}")
        return that


class RespBulkString(RespDataType):
    def __init__(self, data: bytes | None):
        if data is None:
            self.is_nil = True
            self.data = b""
        else:
            self.is_nil = False
            self.data = cast(bytes, data)

    def __len__(self):
        return len(self.data) if self.data else 0

    def __str__(self):
        return str(self.data.decode())

    def __repr__(self):
        return f"RespBulkString({repr(self.data)})"

    def encode(self):
        return (
            f"${len(self.data)}\r\n".encode() + self.data + b"\r\n"
            if not self.is_nil
            else constants.NULL_BULK_RESP_STRING.encode()
        )

    @classmethod
    def decode(cls, data, pos):
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

    @classmethod
    def validate(cls, that):
        if not isinstance(that, RespBulkString):
            raise ValidationError(f"Expected RespBulkString, got {type(that)}")
        return that


class RespInteger(RespDataType):
    def __init__(self, val: int):
        self.val = val

    def __len__(self):
        return len(str(self.val))

    def __str__(self):
        return str(self.val)

    def __repr__(self):
        return f"RespInteger({repr(self.val)})"

    def encode(self):
        return f":{self.val}\r\n".encode()

    @classmethod
    def decode(cls, data, pos):
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP integer, missing \\r\\n separator")
        val = int(data[start:pos])
        pos += 2
        assert pos <= len(data)
        return (RespInteger(val), pos)

    @classmethod
    def validate(cls, that):
        if not isinstance(that, RespInteger):
            raise ValidationError(f"Expected RespInteger, got {type(that)}")
        return that


class RespSimpleError(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data.decode())

    def __repr__(self):
        return f"RespSimpleError({repr(self.data)})"

    def encode(self):
        return b"-" + self.data + b"\r\n"

    @classmethod
    def decode(cls, data, pos):
        start = pos + 1
        while pos < len(data) and not is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            logger.info("Invalid RESP simple error, missing \\r\\n separator")
        simple_err = data[start:pos]
        pos += 2
        assert pos <= len(data)
        return (RespSimpleError(simple_err), pos)

    @classmethod
    def validate(cls, that):
        if not isinstance(that, RespSimpleError):
            raise ValidationError(f"Expected RespSimpleError, got {type(that)}")
        return that


class RespRdbFile(RespDataType):
    def __init__(self, data: bytes):
        self.data = data
        self.key_values = RdbParser(data).parse_rdb()

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data.decode())

    def __repr__(self):
        return f"RespRdbFile({repr(self.data)})"

    def encode(self):
        return f"${len(self.data)}\r\n".encode() + self.data

    @classmethod
    def decode(cls, data, pos):
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

    @classmethod
    def validate(cls, that):
        if not isinstance(that, RespRdbFile):
            raise ValidationError(f"Expected RdbFile, got {type(that)}")
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


def dispatch(cmd: bytes, pos: int) -> tuple[RespDataType, int]:
    """Raises ParseError if input is not valid"""
    data_type = cmd[pos : pos + 1]
    if data_type == b"*":
        return RespArray.decode(cmd, pos)
    elif data_type == b"$":
        return decode_bulk_string_or_rdb(cmd, pos)
    elif data_type == b"+":
        return RespSimpleString.decode(cmd, pos)
    else:
        raise ParseError(f"Unsupported data type {data_type}")
