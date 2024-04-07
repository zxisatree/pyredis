import datetime

import constants
from logs import logger


class RdbFile:
    def __init__(self, data: bytes):
        self.data = data
        self.idx = 9  # ignore magic string and version number
        self.buffer = []
        self.key_values: dict[str, tuple[str, datetime.datetime | None]] = {}
        err = self.read_rdb()
        if err is not None:
            logger.error(f"Failed to read RDB file with error {err}, defaulting to empty file")
            self.data = constants.EMPTY_RDB_FILE
            self.read_rdb()

    def __len__(self) -> int:
        return len(self.data)

    def read_rdb(self) -> str | None:
        sanity_check = self.data[0:5]
        if sanity_check != b"REDIS":
            return f"Invalid RDB file, magic bytes are not REDIS: {sanity_check}"
        try:
            # check version number
            int.from_bytes(self.data[5:9], byteorder="little")
        except:
            return f"Invalid RDB file, got version number: {self.data[5:9]}"
        while self.idx < len(self.data):
            self.parse()

    def read(self, length: int) -> bytes:
        data = self.data[self.idx : self.idx + length]
        self.idx += length
        return data

    def read_length_encoding(self) -> tuple[int, int, int]:
        length_encoding = self.read(1)
        return (
            int.from_bytes(length_encoding) >> 7,
            (int.from_bytes(length_encoding) >> 6) & 1,
            int.from_bytes(length_encoding) & 0x3F,
        )

    def read_length_encoded_integer(self) -> tuple[int, bool]:
        le0, le1, rest = self.read_length_encoding()
        if le0 == 0 and le1 == 0:
            return rest, False
        elif le0 == 0 and le1 == 1:
            next_byte = self.read(1)
            return (rest << 8) | int.from_bytes(next_byte), False
        elif le0 == 1 and le0 == 0:
            return int.from_bytes(self.read(4)), False
        else:
            if rest == 0:
                return 1, True
            elif rest == 1:
                return 2, True
            elif rest == 2:
                return 4, True
            elif rest == 3:
                raise Exception("RdbFile can't parse LZF compressed strings")
            return 0, False

    def read_length_encoded_string(self) -> bytes:
        length, is_int = self.read_length_encoded_integer()
        val = self.read(length)
        if is_int:
            return str(int.from_bytes(val)).encode()
        else:
            return val

    def parse(self):
        logger.info(f"{self.data[self.idx:]}")
        op_code = self.read(1)
        match op_code:
            case b"\xff":
                # EOF, remaining is 8 bit crc
                self.idx = len(self.data)
                return
            case b"\xfe":
                # database selector
                db_selector = self.read_length_encoded_integer()[0]
                self.buffer.append(("db", db_selector))
            case b"\xfd":
                # expiry time in s
                expiry = datetime.datetime.fromtimestamp(
                    int.from_bytes(self.read(4), "little"), datetime.UTC
                )
                expiry = expiry.replace(tzinfo=None)
                key, value = self.parse_kv(self.read(1))
                self.key_values[key.decode()] = (value.decode(), expiry)
            case b"\xfc":
                # expiry time in ms
                expiry = datetime.datetime.fromtimestamp(
                    int.from_bytes(self.read(8), "little") / 1e3, datetime.UTC
                )
                expiry = expiry.replace(tzinfo=None)
                key, value = self.parse_kv(self.read(1))
                self.key_values[key.decode()] = (value.decode(), expiry)
            case b"\xfb":
                # resizedb
                db_hash_table_size = self.read_length_encoded_integer()[0]
                expiry_hash_table_size = self.read_length_encoded_integer()[0]
                self.buffer.append(
                    ("resizedb", db_hash_table_size, expiry_hash_table_size)
                )
            case b"\xfa":
                # aux field
                aux_key = self.read_length_encoded_string()
                aux_value = self.read_length_encoded_string()
                self.buffer.append(("aux", aux_key, aux_value))
            case _:
                # type, key, value
                key, value = self.parse_kv(op_code)
                self.key_values[key.decode()] = (value.decode(), None)
                return

    def parse_kv(self, val_type: bytes) -> tuple[bytes, bytes]:
        key = self.read_length_encoded_string()
        match val_type:
            case b"\x00":
                # string
                return key, self.read_length_encoded_string()
            case _:
                return key, b""
