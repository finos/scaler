# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the YMQ Python C Extension module

from enum import IntEnum
from typing import Callable, Optional, SupportsBytes, Union

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer

DEFAULT_MAX_RETRY_TIMES: int
DEFAULT_INIT_RETRY_DELAY: int

MAGIC_STRING: bytes
"""The bytes every YMQ connection starts with, before any framed message."""

class Bytes(Buffer):
    data: bytes | None
    """Data of the Bytes object"""

    len: int
    """Length of the Bytes object"""

    def __init__(self, data: Buffer | None = None) -> None: ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...

    # this type signature is not 100% accurate because it's implemented in C
    # but this satisfies the type check and is good enough
    def __buffer__(self, flags: int, /) -> memoryview: ...

class Message:
    address: Bytes | None
    payload: Bytes

    def __init__(
        self, address: Bytes | bytes | SupportsBytes | None, payload: Bytes | bytes | SupportsBytes
    ) -> None: ...
    def __repr__(self) -> str: ...

class AddressType(IntEnum):
    """Address type enum"""

    IPC = 0
    TCP = 1

class Address:
    """
    A socket address, can either be a TCP address (IPv4/6) or an IPC path.

    Example address strings:
        - ipc://some_ipc_socket_name
        - tcp://127.0.0.1:1827
        - tcp://[2001:db8::1]:1211
    """

    type: AddressType
    """Get the address type (IPC or TCP)"""

    def __init__(self, address: str) -> None:
        """Create an Address from a string."""

    def __repr__(self) -> str: ...

class TLSConfig:
    """TLS certificate configuration used by secure YMQ sockets."""

    def __init__(self, cert_chain: str, private_key: str) -> None: ...
    def __repr__(self) -> str: ...

class IOContext:
    """Manages a pool of IO event threads"""

    num_threads: int
    """Get the number of threads in the IOContext"""

    def __init__(self, num_threads: int = 1) -> None:
        """Create an IOContext with the specified number of threads"""

    def __repr__(self) -> str: ...

class BinderSocket:
    """A binder socket that can bind to an address and communicate with multiple peers"""

    identity: str
    """Get the identity of the socket"""

    def __init__(self, context: IOContext, identity: str) -> None:
        """Create a BinderSocket with the specified identity."""

    def __repr__(self) -> str: ...
    def bind_to(
        self,
        callback: Callable[[Union[Address, Exception]], None],
        address: str,
        tls_config: Optional[TLSConfig] = None,
    ) -> None:
        """Bind the socket to an address and listen for incoming connections."""

    def send_message(
        self, on_message_send: Callable[[Optional[Exception]], None], remote_identity: str, message_payload: Bytes
    ) -> None:
        """Send a message to a remote peer."""

    def send_multicast_message(self, message_payload: Bytes, remote_prefix: Optional[str] = None) -> None:
        """Send a message to all connected peers, optionally filtered by identity prefix."""

    def recv_message(self, callback: Callable[[Union[Message, Exception]], None]) -> None:
        """Receive a message from a remote peer."""

    def close_connection(self, remote_identity: str) -> None:
        """Close the connection to a specific remote peer."""

    def shutdown(self) -> None:
        """Shut down the socket and fail pending callbacks."""

class ConnectorSocket:
    """A connector socket that exchanges messages with a single remote peer.

    Can either connect to a remote binder socket or binding connector, or bind to an address
    and accept a connection from another connector socket.

    Use ConnectorSocket.connect() or ConnectorSocket.bind() to create an instance.
    """

    identity: str
    """Get the identity of the socket"""

    @classmethod
    def connect(
        cls,
        callback: Callable[[Optional[Exception]], None],
        context: IOContext,
        identity: str,
        address: str,
        max_retry_times: int = DEFAULT_MAX_RETRY_TIMES,
        init_retry_delay: int = DEFAULT_INIT_RETRY_DELAY,
        tls_config: Optional[TLSConfig] = None,
    ) -> "ConnectorSocket":
        """Create a ConnectorSocket and initiate connection to the remote address."""

    @classmethod
    def bind(
        cls,
        callback: Callable[[Union[Address, Exception]], None],
        context: IOContext,
        identity: str,
        address: str,
        tls_config: Optional[TLSConfig] = None,
    ) -> "ConnectorSocket":
        """Create a ConnectorSocket that binds to an address and waits for incoming connections."""

    def __repr__(self) -> str: ...
    def send_message(self, callback: Callable[[Optional[Exception]], None], message_payload: Bytes) -> None:
        """Send a message to the connected remote peer."""

    def recv_message(self, callback: Callable[[Union[Message, Exception]], None]) -> None:
        """Receive a message from the connected remote peer."""

    def shutdown(self) -> None:
        """Shut down the socket and fail pending callbacks."""

class ErrorCode(IntEnum):
    Uninit = 0
    InvalidPortFormat = 1
    InvalidAddressFormat = 2
    RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery = 3
    ConnectorSocketClosedByRemoteEnd = 4
    SocketStopRequested = 5
    SysCallError = 6

    def explanation(self) -> str: ...

class YMQException(Exception):
    code: ErrorCode
    """Error code"""

    message: str
    """Error message"""

    def __init__(self, /, code: ErrorCode, message: str) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class InvalidPortFormatError(YMQException): ...
class InvalidAddressFormatError(YMQException): ...
class RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError(YMQException): ...
class ConnectorSocketClosedByRemoteEndError(YMQException): ...
class SocketStopRequestedError(YMQException): ...
class SysCallError(YMQException): ...
