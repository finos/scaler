import asyncio
import logging
from collections import defaultdict
from typing import Awaitable, Callable, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import BinderSocket, Bytes, ConnectorSocketClosedByRemoteEndError, IOContext
from scaler.io.ymq.utils import run_detached, to_tls_config
from scaler.protocol.capnp import BaseMessage, BinderStatus

logger = logging.getLogger(__name__)


class YMQAsyncBinder(AsyncBinder):
    def __init__(self, context: IOContext, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]):
        self._ymq_context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._socket: Optional[BinderSocket] = BinderSocket(self._ymq_context, self._identity.decode())

        self._callback: Callable[[bytes, BaseMessage], Awaitable[None]] = callback

        self._received: dict[str, int] = defaultdict(lambda: 0)
        self._sent: dict[str, int] = defaultdict(lambda: 0)

        self._pending_tasks: set[asyncio.Task] = set()

    def __del__(self):
        self.destroy()

    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        assert self._socket is not None
        bound_address = await self._socket.bind_to(repr(address), to_tls_config(security_config))
        self._address = AddressConfig.from_string(repr(bound_address))

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    def destroy(self):
        if self._socket is None:
            return

        for task in self._pending_tasks.copy():
            task.cancel()

        self._socket.shutdown()

        self._socket = None
        self._ymq_context = None

    async def routine(self):
        assert self._socket is not None
        ymq_msg = await self._socket.recv_message()

        message: Optional[BaseMessage] = deserialize(ymq_msg.payload.data)
        if message is None:
            logger.error(f"received unknown message from {ymq_msg.address.data!r}: {ymq_msg.payload.data!r}")
            return

        self.__count_received(message.__class__.__name__)
        try:
            await self._callback(ymq_msg.address.data, message)
        except ConnectorSocketClosedByRemoteEndError:
            # The callback (e.g. on_heartbeat -> binder.send echo) tried to reply to a peer that
            # had already disconnected by the time Python caught up on libuv-buffered messages.
            # Treat as a no-op: peer-gone is a routine event handled by the scheduler controllers'
            # own timeout/cleanup paths. Re-raising would bubble up through asyncio.gather and
            # tear down the whole scheduler for what is a normal peer departure.
            pass

    async def send(self, to: bytes, message: BaseMessage, *, detached: bool):
        assert self._socket is not None
        self.__count_sent(message.__class__.__name__)

        awaitable = self._socket.send_message(to.decode(), Bytes(serialize(message)))

        if not detached:
            await awaitable
            return

        self._pending_tasks.add(
            run_detached(awaitable, f"{self.__get_prefix()} send to {to!r}", self._pending_tasks.discard)
        )

    def get_status(self) -> BinderStatus:
        return BinderStatus(
            received=[
                BinderStatus.Pair(client=message_type, number=count) for message_type, count in self._received.items()
            ],
            sent=[BinderStatus.Pair(client=message_type, number=count) for message_type, count in self._sent.items()],
        )

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1

    def __get_prefix(self) -> str:
        return f"{self.__class__.__name__}[{self._identity!r}]:"
