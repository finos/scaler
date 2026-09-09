import asyncio
import struct
import unittest

import websockets.asyncio.client
import websockets.asyncio.server

from scaler.io.ymq import MAGIC_STRING, BinderSocket, Bytes, ConnectorSocket, IOContext


def _encode_message(payload: bytes) -> bytes:
    return struct.pack("<Q", len(payload)) + payload


def _decode_message(frame: bytes) -> bytes:
    length = struct.unpack("<Q", frame[:8])[0]
    return frame[8 : 8 + length]


class TestWebSocketInterop(unittest.IsolatedAsyncioTestCase):
    async def test_ymq_server_websocket_client(self):
        """A standard websockets client can connect to a YMQ BinderSocket and exchange messages."""
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")
        address = await binder.bind_to("ws://127.0.0.1:0/")

        async with websockets.asyncio.client.connect(repr(address)) as ws:
            await ws.send(MAGIC_STRING)
            await ws.send(_encode_message(b"py-ws-client"))

            magic = await ws.recv()
            self.assertEqual(magic, MAGIC_STRING)

            identity_frame = await ws.recv()
            assert isinstance(identity_frame, bytes)
            self.assertEqual(_decode_message(identity_frame), b"binder")

            await ws.send(_encode_message(b"hello from websockets"))

            msg = await binder.recv_message()
            assert msg.address is not None
            self.assertEqual(msg.payload.data, b"hello from websockets")
            self.assertEqual(msg.address.data, b"py-ws-client")

            await binder.send_message("py-ws-client", Bytes(b"hello from ymq"))

            reply_frame = await ws.recv()
            assert isinstance(reply_frame, bytes)
            self.assertEqual(_decode_message(reply_frame), b"hello from ymq")

    async def test_websocket_server_ymq_client(self):
        """A YMQ ConnectorSocket can connect to a standard websockets server and exchange messages."""
        received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        ymq_recv: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

        async def handle(ws: websockets.asyncio.server.ServerConnection) -> None:
            self.assertEqual(await ws.recv(), MAGIC_STRING)
            identity_frame = await ws.recv()
            assert isinstance(identity_frame, bytes)
            self.assertEqual(_decode_message(identity_frame), b"connector")

            await ws.send(MAGIC_STRING)
            await ws.send(_encode_message(b"py-ws-server"))

            msg_frame = await ws.recv()
            assert isinstance(msg_frame, bytes)
            received.set_result(_decode_message(msg_frame))

            await ws.send(_encode_message(await ymq_recv))

        async with websockets.asyncio.server.serve(handle, "127.0.0.1", 0) as server:
            port = next(iter(server.sockets)).getsockname()[1]

            ctx = IOContext()
            connector = await ConnectorSocket.async_connect(ctx, "connector", f"ws://127.0.0.1:{port}/")

            await connector.send_message(Bytes(b"hello from ymq"))

            payload = await asyncio.wait_for(received, timeout=5.0)
            self.assertEqual(payload, b"hello from ymq")

            ymq_recv.set_result(b"hello from websockets")

            msg = await asyncio.wait_for(connector.recv_message(), timeout=5.0)
            self.assertEqual(msg.payload.data, b"hello from websockets")
