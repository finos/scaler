import logging
from collections import defaultdict
from typing import Awaitable, Callable, Optional

import zmq
import zmq.asyncio
from zmq import Frame

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.network_backends import ZMQNetworkBackend
from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import BaseMessage, BinderStatus

logger = logging.getLogger(__name__)


class ZMQAsyncBinder(AsyncBinder):
    def __init__(
        self, context: zmq.asyncio.Context, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]
    ):
        self._zmq_context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._socket = self._zmq_context.socket(zmq.ROUTER)

        self._callback: Callable[[bytes, BaseMessage], Awaitable[None]] = callback

        self._received: dict[str, int] = defaultdict(lambda: 0)
        self._sent: dict[str, int] = defaultdict(lambda: 0)

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket.closed:
            return

        self._socket.close(linger=0)

    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        ZMQNetworkBackend.raise_if_unsupported(address, security_config)

        self.__set_socket_options()
        self._socket.bind(repr(address))

        endpoint = self._socket.getsockopt(zmq.LAST_ENDPOINT)
        assert isinstance(endpoint, bytes)

        self._address = AddressConfig.from_string(endpoint.decode())

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def routine(self):
        frames: list[Frame] = await self._socket.recv_multipart(copy=False)
        if not self.__is_valid_message(frames):
            return

        source, payload = frames
        try:
            message: Optional[BaseMessage] = deserialize(payload.bytes)
            if message is None:
                logger.error(f"received unknown message from {source.bytes!r}: {payload!r}")
                return
        except Exception as e:
            logger.error(f"{self.__get_prefix()} failed to deserialize message from {source.bytes!r}: {e}")
            return

        self.__count_received(message.__class__.__name__)
        await self._callback(source.bytes, message)

    async def send(self, to: bytes, message: BaseMessage, *, detached: bool):
        # `detached` is ignored: a ZMQ ROUTER socket is always fire-and-forget.
        self.__count_sent(message.__class__.__name__)
        await self._socket.send_multipart([to, serialize(message)], copy=False)

    def get_status(self) -> BinderStatus:
        return BinderStatus(
            received=[
                BinderStatus.Pair(client=message_type, number=count) for message_type, count in self._received.items()
            ],
            sent=[BinderStatus.Pair(client=message_type, number=count) for message_type, count in self._sent.items()],
        )

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __is_valid_message(self, frames: list[Frame]) -> bool:
        if len(frames) != 2:
            logger.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        return True

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity!r}]:"
