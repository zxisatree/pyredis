import argparse
import select
import socket
import threading
from typing import Sequence

from sys import path
from pathlib import Path

# if app is called from parent folder, modify path to call files in parent folder
path.append(str(Path(__file__).parent))

import aof
import codec
import commands
import constants
import database
import data_types
from exceptions import ArgParseError
import interfaces
from logs import logger
from utils import construct_conn_id, transform_to_execute_output
import replicas


def main(args: Sequence[str] | None = None):
    (
        port,
        replicaof,
        rdbdir,
        dbfilename,
        appendonly,
        appenddirname,
        appendfilename,
        appendfsync,
    ) = validate_parse_args(setup_argparser().parse_args(args))
    with aof.AofHandler(
        rdbdir, appendonly, appenddirname, appendfilename, appendfsync
    ) as aof_handler:
        db = database.Database(rdbdir, dbfilename, aof_handler)
        replica_handler = replicas.ReplicaHandler(
            False if replicaof else True, "localhost", port, replicaof, db
        )
        # attempt to connect to master
        if replicaof:
            threading.Thread(target=replica_handler.master_recv_loop).start()

        aof_cmd_data = aof_handler.read()
        aof_cmds = codec.parse_cmd(aof_cmd_data)
        logger.info(f"{aof_cmd_data=}, {aof_cmds=}")
        for cmd in aof_cmds:
            cmd.execute_for_aof(db)
        logger.info(f"Executed {len(aof_cmds)} commands from AOF FILE")

        # for signalling to the accepting thread to close
        # automatically cleaned up after program exits
        read_socket, write_socket = socket.socketpair()

        logger.info(f"Started server on {port=}")
        try:
            accept_thread = threading.Thread(
                target=accept_conns,
                args=(read_socket, port, db, replica_handler, aof_handler),
            )
            logger.info("starting thread")
            accept_thread.start()
            # wait indefinitely, but keep stdin open
            while True:
                input()
        except (KeyboardInterrupt, EOFError):
            # keyboard interrupts are EOFErrors during input on pwsh nested in bash in a vscode terminal
            pass
        finally:
            logger.info("cleaning up...")
            write_socket.close()
            logger.info(
                f"closed write socket, waiting for accept_thread to join in {constants.CONN_TIMEOUT}s..."
            )
            # wait for accept_thread only if it has been created
            try:
                accept_thread.join()
            except (UnboundLocalError, RuntimeError):
                # UnboundLocalError: if variable has not been created, RuntimeError: thread has not been started
                pass
            logger.info("accept_thread joined.")


def accept_conns(
    read_socket: socket.socket,
    port: int,
    db: database.Database,
    replica_handler: replicas.ReplicaHandler,
    aof_handler: aof.AofHandler,
):
    try:
        server_socket = socket.create_server(("localhost", port))
        while True:
            ready = select.select([server_socket, read_socket], [], [])
            if ready[0]:
                if read_socket in ready[0]:
                    # write_socket was closed, cleanup and shutdown this thread
                    break
                conn, addr = server_socket.accept()
                thread = threading.Thread(
                    target=handle_conn,
                    args=(conn, addr, db, replica_handler, aof_handler),
                )
                thread.daemon = True
                thread.start()
        logger.info("accept_conns exiting")
    except Exception:
        # adds exception details automatically
        logger.exception("main thread exception")


def handle_conn(
    conn: socket.socket,
    addr,
    db: database.Database,
    replica_handler: replicas.ReplicaHandler,
    aof_handler: aof.AofHandler,
):
    conn_id = construct_conn_id(conn)
    with conn:
        while True:
            data = conn.recv(constants.BUFFER_SIZE)
            if not data:
                break
            logger.info(f"raw {data=}")
            cmds = codec.parse_cmd(data)
            logger.info(f"{cmds=}")
            for cmd in cmds:
                execute_cmd_for_conn(
                    cmd, db, replica_handler, aof_handler, conn, conn_id
                )

        logger.info(f"Connection closed: {addr=}")


def execute_cmd_for_conn(
    cmd: commands.Command,
    db: database.Database,
    replica_handler: replicas.ReplicaHandler,
    aof_handler: aof.AofHandler,
    conn: socket.socket,
    conn_id: tuple[int, str],
):
    in_xact = db.xact_exists(conn_id)
    in_subscribed_mode = db.in_subscribed_mode(conn_id)
    is_conn_authenticated = db.is_conn_authenticated(conn_id)

    if not is_conn_authenticated and not cmd.allowed_while_unauthenticated:
        executed = transform_to_execute_output(constants.NOAUTH_ERROR)
    elif in_xact and cmd.xact_behaviour == interfaces.XactBehaviour.ERROR:
        executed = data_types.RespSimpleError(
            f"ERR {cmd.keyword.decode().lower()} inside MULTI is not allowed".encode()
        ).encode_to_list()
    elif in_xact and cmd.xact_behaviour == interfaces.XactBehaviour.QUEUE:
        db.queue_xact_cmd(conn_id, cmd)
        executed = transform_to_execute_output(constants.XACT_QUEUED_RESPONSE)
    elif in_subscribed_mode and not cmd.allowed_in_subscribed_mode:
        executed = data_types.RespSimpleError(
            f"ERR Can't execute '{cmd.keyword.decode().lower()}': only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT / RESET are allowed in subscribed mode".encode()
        ).encode_to_list()
    else:
        if cmd.should_propogate_to_replicas:
            replica_handler.propogate(cmd._raw_cmd)
        if cmd.should_write_to_aof:
            aof_handler.write(cmd.raw_cmd)
        executed = cmd.execute(db, replica_handler, conn)

    for resp in executed:
        logger.info(f"responding with {resp}")
        conn.sendall(resp)
        # better error catching for prod
        # if isinstance(resp, data_types.RespDataType):
        #     logger.warning(
        #         f"{cmd.keyword} returned a RespDataType. Automatically encoding to bytes..."
        #     )
        #     resp_bytes = b"".join(resp.encode_to_list())
        # else:
        #     resp_bytes = resp
        # logger.info(f"responding with {resp_bytes}")
        # conn.sendall(resp_bytes)


def validate_parse_args(
    args: argparse.Namespace,
) -> tuple[
    int,
    tuple[str, int] | None,
    str,
    str,
    bool,
    str,
    str,
    aof.AppendFsyncOption,
]:
    """Throws ArgParseError if validation fails"""
    if args.port < 0 or args.port > 65535:
        raise ArgParseError(
            f"Invalid port number {args.port}, should be between 0 and 65535"
        )

    replicaof = None
    if args.replicaof is not None:
        replica_host, replica_port = args.replicaof.split(" ")
        try:
            replicaof_int = int(replica_port)
        except ValueError:
            raise ArgParseError("replicaof port is not an integer")
        replicaof = (replica_host, replicaof_int)

    if args.appendonly == "no":
        appendonly = False
    elif args.appendonly == "yes":
        appendonly = True
    else:
        raise ArgParseError(
            f"Invalid appendonly option {args.appendonly}, should be one of ('no', 'yes')"
        )

    if args.appendfsync == "always":
        appendfsync = aof.AppendFsyncOption.ALWAYS
    elif args.appendfsync == "everysec":
        appendfsync = aof.AppendFsyncOption.EVERYSEC

    return (
        args.port,
        replicaof,
        args.dir,
        args.dbfilename,
        appendonly,
        args.appenddirname,
        args.appendfilename,
        appendfsync,
    )


def setup_argparser() -> argparse.ArgumentParser:
    argparser = argparse.ArgumentParser(
        prog="pykvstore",
        description="Key value database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    argparser.add_argument("--port", type=int, default=6379, help="Port to listen on")
    argparser.add_argument(
        "--replicaof",
        # nargs=2, # tests recently changed to a single string e.g. "localhost 6380"
        default=None,
        help="Master IP and port to replicate from",
    )
    # default value was ./rdb before implementing AOF
    argparser.add_argument(
        "--dir",
        default="/app",
        help="Directory where files are stored",
    )
    argparser.add_argument(
        "--dbfilename",
        default="dump.rdb",
        help="The name of the RDB file",
    )
    argparser.add_argument(
        "--appendonly",
        default="no",
        help="Enable AOF persistence",
    )
    argparser.add_argument(
        "--appenddirname",
        default="appendonlydir",
        help="Subdirectory where AOF files are stored",
    )
    argparser.add_argument(
        "--appendfilename",
        default="appendonly.aof",
        help="Name of AOF file",
    )
    argparser.add_argument(
        "--appendfsync",
        default="everysec",
        help="Frequency of fsync",
    )
    return argparser


if __name__ == "__main__":
    main()
