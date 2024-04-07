BUFFER_SIZE = 1024
CONN_TIMEOUT = 15
MAX_STREAM_ID_SEQ_NO = 2**32 - 1

OK_SIMPLE_RESP_STRING = "+OK\r\n"
NULL_BULK_RESP_STRING = "$-1\r\n"
STREAM_ID_NOT_GREATER_ERROR = (
    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
)
STREAM_ID_TOO_SMALL_ERROR = "ERR The ID specified in XADD must be greater than 0-0"
XOP_ON_NON_STREAM_ERROR = "ERR The key provided does not refer to a stream"
NO_OP_RESPONSE = "NOOP"

from base64 import b64decode

EMPTY_RDB_FILE = b64decode(
    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
)
