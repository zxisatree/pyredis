# PyRedis

_A Python implementation of a Redis server_

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
