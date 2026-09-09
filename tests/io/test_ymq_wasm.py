"""Tests for the pure-Python ymq browser shim (``scaler.io.ymq._ymq_wasm``).

These tests exercise the protocol-framing and surface API of the shim without
requiring a real browser or Pyodide runtime. The WebSocket lifecycle hooks
(``_open_websocket``, ``_raw_send``) are bypassed by injecting a fake
WebSocket-like object so that we can drive the receive path directly.
"""

import struct
import unittest
import unittest.mock
from typing import Any, List, Optional

from scaler.io.ymq import _ymq_wasm
from scaler.io.ymq._ymq_wasm import (
    Address,
    AddressType,
    BinderSocket,
    Bytes,
    ConnectorSocket,
    ConnectorSocketClosedByRemoteEndError,
    ErrorCode,
    InvalidAddressFormatError,
    IOContext,
    Message,
    SocketStopRequestedError,
    YMQException,
)

_HEADER = "<Q"


def _frame(payload: bytes) -> bytes:
    return struct.pack(_HEADER, len(payload)) + payload


class _FakeWebSocket:
    """Minimal stand-in for ``js.WebSocket`` to drive the shim from Python."""

    def __init__(self) -> None:
        self.sent: List[bytes] = []
        self.closed: bool = False
        self.binaryType: str = ""

    def send(self, data: Any) -> None:
        # Real shim wraps bytes in js.Uint8Array; here we accept raw bytes.
        self.sent.append(bytes(data))

    def close(self) -> None:
        self.closed = True


def _make_socket(identity: str = "test-client") -> ConnectorSocket:
    """Construct a ConnectorSocket without invoking the real WebSocket path."""
    socket = ConnectorSocket(identity=identity, address="ws://example:1234")
    socket._ws = _FakeWebSocket()
    return socket


def _open(socket: ConnectorSocket) -> None:
    """Simulate the JS ``open`` event and patch ``_raw_send`` to use the fake."""

    def raw_send(data: bytes, callback: Optional[Any]) -> None:
        socket._ws.send(data)
        if callback is not None:
            callback(None)

    socket._raw_send = raw_send  # type: ignore[method-assign]
    socket._on_open(None)


def _feed(socket: ConnectorSocket, data: bytes) -> None:
    """Simulate the JS ``message`` event by injecting raw bytes."""

    class _Evt:
        def __init__(self, b: bytes) -> None:
            self.data = b

        # _on_message attempts ``data.to_py()`` first; we don't provide that,
        # so it falls through to ``bytes(memoryview(data))``.

    socket._on_message(_Evt(data))


class BytesTest(unittest.TestCase):
    def test_default_is_none(self) -> None:
        b = Bytes()
        self.assertIsNone(b.data)
        self.assertEqual(b.len, 0)
        self.assertEqual(len(b), 0)

    def test_wraps_bytes(self) -> None:
        b = Bytes(b"hello")
        self.assertEqual(b.data, b"hello")
        self.assertEqual(len(b), 5)
        self.assertEqual(bytes(b), b"hello")

    def test_wraps_bytes_subclass(self) -> None:
        b = Bytes(Bytes(b"abc"))
        self.assertEqual(b.data, b"abc")

    def test_repr_does_not_leak_payload(self) -> None:
        self.assertNotIn("secret", repr(Bytes(b"secret")))


class MessageTest(unittest.TestCase):
    def test_payload_only(self) -> None:
        msg = Message(address=None, payload=b"hi")
        self.assertIsNone(msg.address)
        self.assertEqual(msg.payload.data, b"hi")

    def test_with_address(self) -> None:
        msg = Message(address=b"peer", payload=b"hi")
        assert msg.address is not None
        self.assertEqual(msg.address.data, b"peer")


class AddressTest(unittest.TestCase):
    def test_ws_classifies_as_websocket(self) -> None:
        self.assertEqual(Address("ws://host:1").type, AddressType.WebSocket)
        self.assertEqual(Address("wss://host:1/path").type, AddressType.WebSocket)

    def test_tcp_classifies_as_tcp(self) -> None:
        self.assertEqual(Address("tcp://127.0.0.1:1234").type, AddressType.TCP)

    def test_ipc_classifies_as_ipc(self) -> None:
        self.assertEqual(Address("ipc://name").type, AddressType.IPC)

    def test_unknown_scheme_raises(self) -> None:
        with self.assertRaises(InvalidAddressFormatError):
            Address("http://nope")

    def test_repr_round_trips(self) -> None:
        self.assertEqual(repr(Address("ws://h:1/p")), "ws://h:1/p")


class IOContextTest(unittest.TestCase):
    def test_default_one_thread(self) -> None:
        self.assertEqual(IOContext().num_threads, 1)
        self.assertEqual(IOContext(num_threads=4).num_threads, 4)


class BinderSocketTest(unittest.TestCase):
    def test_not_implemented(self) -> None:
        with self.assertRaises(NotImplementedError):
            BinderSocket(IOContext(), "id")


class ConnectorSocketSurfaceTest(unittest.TestCase):
    def test_bind_not_implemented(self) -> None:
        with self.assertRaises(NotImplementedError):
            ConnectorSocket.bind(lambda r: None, IOContext(), "id", "ws://host:1")

    def test_invalid_scheme_raises(self) -> None:
        with self.assertRaises(InvalidAddressFormatError):
            ConnectorSocket.connect(IOContext(), "id", "tcp://host:1")


class HandshakeTest(unittest.TestCase):
    def test_handshake_sent_on_open(self) -> None:
        socket = _make_socket(identity="my-id")
        _open(socket)
        self.assertEqual(len(socket._ws.sent), 1)
        sent = socket._ws.sent[0]
        magic_size = len(_ymq_wasm.MAGIC_STRING)
        self.assertEqual(sent[:magic_size], _ymq_wasm.MAGIC_STRING)
        (length,) = struct.unpack_from(_HEADER, sent, magic_size)
        self.assertEqual(length, len(b"my-id"))
        self.assertEqual(sent[magic_size + struct.calcsize(_HEADER) :], b"my-id")

    def test_remote_handshake_consumed_silently(self) -> None:
        # Remote sends magic + identity; should not surface as a Message.
        socket = _make_socket()
        _open(socket)
        received: List[Any] = []
        socket.recv_message_with_callback(lambda r: received.append(r))
        _feed(socket, _ymq_wasm.MAGIC_STRING + _frame(b"remote-id"))
        self.assertEqual(received, [])  # no message yet
        self.assertEqual(socket._remote_identity, b"remote-id")
        self.assertTrue(socket._handshake_complete)

    def test_invalid_remote_magic_fails_socket(self) -> None:
        socket = _make_socket()
        _open(socket)
        received: List[Any] = []
        socket.recv_message_with_callback(lambda r: received.append(r))
        _feed(socket, b"BAD!" + _frame(b"id"))
        self.assertEqual(len(received), 1)
        self.assertIsInstance(received[0], YMQException)
        self.assertTrue(socket._closed)


class FramingTest(unittest.TestCase):
    def _open_handshaken(self) -> ConnectorSocket:
        socket = _make_socket()
        _open(socket)
        _feed(socket, _ymq_wasm.MAGIC_STRING + _frame(b"remote"))
        return socket

    def test_send_message_frames_payload(self) -> None:
        socket = _make_socket()
        _open(socket)
        socket._ws.sent.clear()
        socket.send_message_with_callback(lambda r: None, Bytes(b"hello"))
        self.assertEqual(len(socket._ws.sent), 1)
        self.assertEqual(socket._ws.sent[0], _frame(b"hello"))

    def test_send_before_open_is_queued(self) -> None:
        socket = _make_socket()
        # Don't call _open yet
        cb_results: List[Any] = []
        socket.send_message_with_callback(cb_results.append, Bytes(b"q"))
        self.assertEqual(socket._ws.sent, [])
        self.assertEqual(cb_results, [])
        _open(socket)
        # First sent frame is the local handshake, second is the queued payload.
        self.assertEqual(len(socket._ws.sent), 2)
        self.assertEqual(socket._ws.sent[1], _frame(b"q"))
        self.assertEqual(cb_results, [None])

    def test_recv_delivers_after_handshake(self) -> None:
        socket = self._open_handshaken()
        received: List[Any] = []
        socket.recv_message_with_callback(received.append)
        _feed(socket, _frame(b"payload-1"))
        self.assertEqual(len(received), 1)
        self.assertIsInstance(received[0], Message)
        self.assertEqual(received[0].payload.data, b"payload-1")

    def test_recv_buffers_message_arriving_before_callback(self) -> None:
        socket = self._open_handshaken()
        _feed(socket, _frame(b"early"))
        received: List[Any] = []
        socket.recv_message_with_callback(received.append)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload.data, b"early")

    def test_split_frame_is_reassembled(self) -> None:
        socket = self._open_handshaken()
        received: List[Any] = []
        socket.recv_message_with_callback(received.append)
        full = _frame(b"reassembled-payload")
        # Feed in chunks of 1 byte.
        for i in range(len(full)):
            _feed(socket, full[i : i + 1])
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload.data, b"reassembled-payload")

    def test_multiple_messages_in_single_chunk(self) -> None:
        socket = self._open_handshaken()
        received: List[Any] = []
        socket.recv_message_with_callback(received.append)
        socket.recv_message_with_callback(received.append)
        socket.recv_message_with_callback(received.append)
        _feed(socket, _frame(b"a") + _frame(b"bb") + _frame(b"ccc"))
        self.assertEqual([m.payload.data for m in received], [b"a", b"bb", b"ccc"])

    def test_zero_length_message(self) -> None:
        socket = self._open_handshaken()
        received: List[Any] = []
        socket.recv_message_with_callback(received.append)
        _feed(socket, _frame(b""))
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload.data, b"")

    def test_handshake_and_first_message_in_one_chunk(self) -> None:
        socket = _make_socket()
        _open(socket)
        received: List[Any] = []
        socket.recv_message_with_callback(received.append)
        _feed(socket, _ymq_wasm.MAGIC_STRING + _frame(b"remote") + _frame(b"first"))
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload.data, b"first")


class ShutdownTest(unittest.TestCase):
    def test_shutdown_closes_ws_and_drains_callbacks(self) -> None:
        socket = _make_socket()
        _open(socket)
        recv_results: List[Any] = []
        send_results: List[Any] = []
        socket.recv_message_with_callback(recv_results.append)
        socket.send_message_with_callback(send_results.append, Bytes(b"x"))  # immediate send (open)
        # Queue another send AFTER closing the underlying ws to test pending path.
        socket.shutdown()
        self.assertTrue(socket._ws.closed)
        self.assertEqual(len(recv_results), 1)
        self.assertIsInstance(recv_results[0], SocketStopRequestedError)

    def test_shutdown_fails_pending_sends(self) -> None:
        socket = _make_socket()  # NOT opened yet
        send_results: List[Any] = []
        socket.send_message_with_callback(send_results.append, Bytes(b"x"))
        socket.shutdown()
        self.assertEqual(len(send_results), 1)
        self.assertIsInstance(send_results[0], SocketStopRequestedError)

    def test_send_after_shutdown_fails_immediately(self) -> None:
        socket = _make_socket()
        _open(socket)
        socket.shutdown()
        results: List[Any] = []
        socket.send_message_with_callback(results.append, Bytes(b"x"))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], SocketStopRequestedError)

    def test_recv_after_shutdown_fails_immediately(self) -> None:
        socket = _make_socket()
        _open(socket)
        socket.shutdown()
        results: List[Any] = []
        socket.recv_message_with_callback(results.append)
        self.assertEqual(len(results), 1)
        # Buffered close error is SocketStopRequested when shutdown was explicit.
        self.assertIsInstance(results[0], YMQException)


class RemoteCloseTest(unittest.TestCase):
    def test_remote_close_surfaces_error(self) -> None:
        socket = _make_socket()
        _open(socket)
        results: List[Any] = []
        socket.recv_message_with_callback(results.append)

        class _Evt:
            code = 1006
            reason = "Abnormal closure"

        socket._on_close(_Evt())
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], ConnectorSocketClosedByRemoteEndError)
        self.assertTrue(socket._closed)

    def test_buffered_messages_still_delivered_after_close(self) -> None:
        socket = _make_socket()
        _open(socket)
        _feed(socket, _ymq_wasm.MAGIC_STRING + _frame(b"remote") + _frame(b"buffered"))

        class _Evt:
            code = 1000
            reason = "ok"

        socket._on_close(_Evt())
        # The buffered message was queued before close; recv_message should see
        # it ahead of the close error.
        results: List[Any] = []
        socket.recv_message_with_callback(results.append)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].payload.data, b"buffered")


class ErrorCodeTest(unittest.TestCase):
    def test_explanation_returns_string(self) -> None:
        for code in ErrorCode:
            if code is ErrorCode.Uninit:
                # Uninit has an explanation too.
                pass
            self.assertIsInstance(code.explanation(), str)

    def test_make_exception_dispatches_to_subclass(self) -> None:
        exc = _ymq_wasm._make_exception(ErrorCode.ConnectorSocketClosedByRemoteEnd, "boom")
        self.assertIsInstance(exc, ConnectorSocketClosedByRemoteEndError)


class _RunSyncShim:
    """Drive a coroutine/Future to completion on a fresh event loop.

    Stand-in for ``pyodide.ffi.run_sync`` so the JSPI-only sync helpers can
    be exercised under CPython.
    """

    def __init__(self) -> None:
        import asyncio

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def __call__(self, awaitable: Any) -> Any:
        return self.loop.run_until_complete(awaitable)

    def close(self) -> None:
        import asyncio

        asyncio.set_event_loop(None)
        self.loop.close()


class SyncHelpersTest(unittest.TestCase):
    """Exercise ``send_message_sync`` / ``recv_message_sync`` via a patched JSPI."""

    def setUp(self) -> None:
        self._driver = _RunSyncShim()
        self._patch = unittest.mock.patch.object(_ymq_wasm, "_run_sync_jspi", self._driver)
        self._patch.start()

    def tearDown(self) -> None:
        self._patch.stop()
        self._driver.close()

    def test_send_message_sync_writes_frame(self) -> None:
        socket = _make_socket()
        _open(socket)
        socket._handshake_complete = True  # bypass; not relevant for outbound

        # First two sent items are the handshake (magic+identity); start fresh.
        socket._ws.sent.clear()

        socket.send_message_sync(Bytes(b"hello"))
        self.assertEqual(socket._ws.sent, [_frame(b"hello")])

    def test_recv_message_sync_returns_buffered_message(self) -> None:
        socket = _make_socket()
        _open(socket)
        # Feed a complete inbound stream: magic, peer identity, then payload.
        _feed(socket, _ymq_wasm.MAGIC_STRING + _frame(b"peer-identity") + _frame(b"buffered"))
        msg = socket.recv_message_sync()
        self.assertEqual(msg.payload.data, b"buffered")

    def test_send_message_sync_propagates_shutdown_error(self) -> None:
        socket = _make_socket()
        _open(socket)
        socket.shutdown()
        with self.assertRaises(SocketStopRequestedError):
            socket.send_message_sync(Bytes(b"x"))

    def test_recv_message_sync_propagates_remote_close_error(self) -> None:
        socket = _make_socket()
        _open(socket)
        # Simulate the remote closing before the handshake completes.
        socket._on_close(None)
        with self.assertRaises(ConnectorSocketClosedByRemoteEndError):
            socket.recv_message_sync()


if __name__ == "__main__":
    unittest.main()
