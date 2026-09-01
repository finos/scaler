"""io-layer contract tests for ``YMQAsyncBinder``.

A ``detached=True`` send is fire-and-forget: it schedules the send and returns, so a peer that never
connects cannot block the caller, and a failure is logged rather than raised at a caller that has
already moved on. Callers that need the send to have left the process pass ``detached=False``, and
they still get the ``SocketStopRequested`` fail-fast the worker boundary handles -- the graceful
shutdown path is the one that relies on it. See ``tests/worker/test_worker.py`` for that boundary.
"""

import asyncio
import unittest

from scaler.config.types.address import AddressConfig
from scaler.io.utility import deserialize
from scaler.io.ymq import ConnectorSocket, IOContext, SocketStopRequestedError
from scaler.io.ymq_async_binder import YMQAsyncBinder
from scaler.protocol.capnp import BaseMessage, ClientDisconnect


class TestYMQAsyncBinderSend(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._received: list[tuple[bytes, BaseMessage]] = []

        self._context = IOContext()
        self._binder = YMQAsyncBinder(self._context, identity=b"binder-under-test", callback=self._on_receive)
        await self._binder.bind(AddressConfig.from_string("tcp://127.0.0.1:0"))

    async def asyncTearDown(self) -> None:
        # Destroy here rather than only in the tests: a binder left alive is destroyed by __del__ at
        # interpreter exit instead, which dead-locks on the GIL (finos/opengris-scaler#945), so a failing
        # assertion would hang the whole run rather than just fail.
        self._binder.destroy()

    async def _on_receive(self, address: bytes, message: BaseMessage) -> None:
        self._received.append((address, message))

    @staticmethod
    def _make_message() -> ClientDisconnect:
        # Any message with a payload will do; this one just has to survive the round trip.
        return ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.disconnect)

    async def test_detached_send_does_not_wait_for_the_peer(self) -> None:
        """A detached send returns without waiting for a peer that never connects."""
        # Would never return if the send were awaited through to the C++ socket.
        await asyncio.wait_for(
            self._binder.send(b"peer-that-never-connects", self._make_message(), detached=True), timeout=5.0
        )

    async def test_attached_send_propagates_socket_stop_requested_when_socket_shut_down(self) -> None:
        """detached=False surfaces SocketStopRequested when the socket is shut down mid-send.

        The send is queued inside the C++ binder (the peer never connects), then the binder is
        destroyed. The native socket fails the pending send with ``SocketStopRequested``, which the io
        layer propagates as-is (fail fast). This is the exception the worker boundary must handle, and
        it is why the graceful shutdown path opts out of detaching.
        """
        send_task = asyncio.ensure_future(
            self._binder.send(b"peer-that-never-connects", self._make_message(), detached=False)
        )

        # Let the send reach the binder's event-loop thread and park in its pending-send queue.
        await asyncio.sleep(0.2)
        self.assertFalse(send_task.done(), "an attached send should still be pending (peer never connected)")

        # Shut the binder down while the send is in flight (mirrors worker teardown / `disconnect`).
        self._binder.destroy()

        with self.assertRaises(SocketStopRequestedError):
            await asyncio.wait_for(send_task, timeout=5.0)

    async def test_normal_send_still_delivers(self) -> None:
        """A normal send still reaches a connected peer (happy path is unaffected)."""
        connector = ConnectorSocket.connect(self._context, "peer", repr(self._binder.address))

        message = self._make_message()
        await self._binder.send(b"peer", message, detached=False)  # completes once the peer identifies itself

        ymq_msg = await asyncio.wait_for(connector.recv_message(), timeout=5.0)
        received = deserialize(ymq_msg.payload.data)

        assert isinstance(received, ClientDisconnect)
        self.assertEqual(received.disconnectType, message.disconnectType)


if __name__ == "__main__":
    unittest.main()
