# PyKvStore

_A Python implementation of a key value database_

PyKvStore is meant to be a drop in replacement for quick development on Windows, and to fix the inability to Ctrl+C to stop the server. It is not meant to be a production ready server, and is not optimized for speed or memory usage.

Run main.py to start the server on port 6379 by default. The command line arguments `--port`, `--replicaof`, `--dir` and `--dbfilename` can be used to specify the port to listen on, the master server IP and port, the directory to load the RDB file from and the name of the RDB file respectively.

Supported commands include:

-  PING
-  ECHO
-  SET
-  GET
-  INCR
-  COMMAND
-  INFO
-  REPLCONF
-  WAIT
-  PSYNC
-  CONFIG
-  KEYS
-  TYPE
-  MULTI
-  EXEC
-  DISCARD
-  RPUSH
-  LPUSH
-  LPOP
-  BLPOP
-  LLEN
-  LRANGE
-  ZADD
-  ZRANK
-  ZRANGE
-  ZCARD
-  ZSCORE
-  ZREM
-  XADD
-  XRANGE
-  XREAD
-  SUBSCRIBE
-  UNSUBSCRIBE
-  PUBLISH
-  REDIS

Also supports replication (with `--replicaof`) and limited persistence (can read RDB files but not write them).

# Why doesn't Ctrl+C work on Windows?
`socket.accept()` in particular doesn't handle KeyboardInterrupts.
Alternatives considered: wait (`select`) on both socket and stdin. But Windows cannot select on stdin
Spawn a separate process and signal it with SIGINT/SIGBREAK. Is expensive, and requires platform specific code since Windows only accepts SIGBREAK to interrupt `sleep` and `accept`.

Instead, we spawn a separate thread and communicate with it using a `socketpair()`. When we KeyboardInterrupt the main thread, we close the socket which signals the other thread to break out of its loop and clean up.

We also need to handle EOFError, because nested PTYs might send EOFErrors instead of KeyboardInterrupts. e.g. in VSCode from WSL, open bash, then open pwsh.exe and run main.py. Ctrl+C will send an EOFError.

# Why threading and not asyncio?
I'm more familiar with threading and don't expect to have a lot of concurrent connections

# Notes
To test: use WSL for everything. Run main.py to start the server. Then run `redis-cli` to start a client.
To add a command: add the class to commands.py, then add the parser case to codec.py
To add new data types to db: getitem, ValType, self.store type hint
