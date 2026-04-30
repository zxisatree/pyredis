import argparse
import socket
from dataclasses import dataclass

try:
    import readline  # noqa: F401 - activates arrow keys for input()
except ImportError:
    pass  # Windows has no readline

from .commands import craft_command
from .data_types import RespDataType, dispatch
from .exceptions import ArgParseError
from .logs import logger


@dataclass
class ClientCliArgs:
    ip_address: str
    port: int


def send(client: socket.socket, *args: str) -> RespDataType:
    client.sendall(craft_command(*args).encode())
    data = client.recv(4096)
    resp, _ = dispatch(data, 0)
    return resp


def main():
    parsed_args = parse_and_validate_args()
    ip_address, port = parsed_args.ip_address, parsed_args.port
    prompt_str = f"{ip_address}:{port}>"
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((ip_address, port))
    try:
        while True:
            line = input(prompt_str)
            print(send(client, *line.split(" ")))
    except (KeyboardInterrupt, EOFError):
        # keyboard interrupts are EOFErrors during input on pwsh nested in bash in a vscode terminal
        logger.info("Caught keyboard interrupt. Exiting and cleaning up...")
    except Exception:
        logger.exception(
            "Caught unexpected exception in main loop. Exiting and cleaning up..."
        )
    finally:
        client.close()
        print("")


def parse_and_validate_args() -> ClientCliArgs:
    """Throws ArgParseError if validation fails"""
    args = setup_argparser().parse_args()
    if args.port < 0 or args.port > 65535:
        raise ArgParseError(
            f"Invalid port number {args.port}, should be between 0 and 65535"
        )
    try:
        ip_address = socket.gethostbyname(args.host)
    except socket.gaierror:
        raise ArgParseError(
            f"Invalid hostname {args.host}, could not resolve to an IP address"
        )
    return ClientCliArgs(ip_address, args.port)


def setup_argparser() -> argparse.ArgumentParser:
    argparser = argparse.ArgumentParser(
        prog="pykvstore_client",
        description="Client for pykvstore",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    argparser.add_argument(
        "--host", type=str, default="localhost", help="Hostname to connect to"
    )
    argparser.add_argument("--port", type=int, default=6379, help="Port to connect to")

    return argparser


if __name__ == "__main__":
    main()
