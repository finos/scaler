import functools
import socket


def get_available_tcp_port(hostname: str = "127.0.0.1") -> int:
    with socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM) as sock:
        sock.bind((hostname, 0))
        return sock.getsockname()[1]


@functools.lru_cache(maxsize=1)
def get_hostname() -> str:
    """The machine this process runs on. Cached: it cannot change, and the heartbeats are hot."""
    try:
        return socket.gethostname()
    except OSError:
        return ""
