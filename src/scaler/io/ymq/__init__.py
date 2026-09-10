import sys

__all__ = [
    "Address",
    "AddressType",
    "BinderSocket",
    "Bytes",
    "ConnectorSocket",
    "ErrorCode",
    "IOContext",
    "MAGIC_STRING",
    "Message",
    "TLSConfig",
    # Exception types
    "YMQException",
    "ConnectorSocketClosedByRemoteEndError",
    "InvalidAddressFormatError",
    "InvalidPortFormatError",
    "RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError",
    "SocketStopRequestedError",
    "SysCallError",
]

# Pyodide / Emscripten builds cannot load the C extension. Fall back to a
# pure-Python WebSocket-based shim that mirrors the C extension's surface so
# the rest of the codebase (sockets.py, ymq_async_connector.py, ClientAgent,
# ...) is unaffected.
if sys.platform == "emscripten":
    from scaler.io.ymq._ymq_wasm import (  # noqa: F401
        MAGIC_STRING,
        Address,
        AddressType,
        BinderSocket,
        Bytes,
        ConnectorSocket,
        ConnectorSocketClosedByRemoteEndError,
        ErrorCode,
        InvalidAddressFormatError,
        InvalidPortFormatError,
        IOContext,
        Message,
        RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError,
        SocketStopRequestedError,
        SysCallError,
        TLSConfig,
        YMQException,
    )
else:
    from scaler.io.ymq._ymq import (  # Exception types
        MAGIC_STRING,
        Address,
        AddressType,
        Bytes,
        ConnectorSocketClosedByRemoteEndError,
        ErrorCode,
        InvalidAddressFormatError,
        InvalidPortFormatError,
        IOContext,
        Message,
        RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError,
        SocketStopRequestedError,
        SysCallError,
        TLSConfig,
        YMQException,
    )
    from scaler.io.ymq.sockets import BinderSocket, ConnectorSocket  # noqa: F401

ErrorCode.__module__ = __name__
for exception_type in (
    YMQException,
    ConnectorSocketClosedByRemoteEndError,
    InvalidAddressFormatError,
    InvalidPortFormatError,
    RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError,
    SocketStopRequestedError,
    SysCallError,
):
    exception_type.__module__ = __name__
