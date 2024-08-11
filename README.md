# PyRedis

_A Python implementation of a Redis server_

PyRedis is meant to be a drop in replacement for quick development on Windows, and to fix the inability to Ctrl+C to stop the server. It is not meant to be a production ready server, and is not optimized for speed or memory usage.

Run main.py to start the server on port 6379 by default. The command line arguments `--port`, `--replicaof`, `--dir` and `--dbfilename` can be used to specify the port to listen on, the master server IP and port, the directory to load the RDB file from and the name of the RDB file respectively.

Supported commands include:

- PING
- ECHO
- GET
- SET (with expiry)
- INFO
- TYPE
- REPLCONF
- WAIT
- TYPE
- XADD
- XRANGE
- XREAD

Also supports streams, replication (with `--replicaof`) and limited persistence (can read RDB files but not write them).

# Why are there so many if/else statements instead of try/catch or match?

Speed is not a primary concern, and we're writing in Python anyway. Also I'm lazy to rewrite everything

# Why threading and not asyncio?

I'm more familiar with threading and don't expect to have a lot of concurrent connections
