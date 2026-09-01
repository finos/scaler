import asyncio
import logging
from typing import Awaitable, Callable, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import Bytes, ConnectorSocket, IOContext
from scaler.io.ymq.utils import run_detached, to_tls_config
from scaler.protocol.capnp import BaseMessage

logger = logging.getLogger(__name__)


class YMQAsyncConnector(AsyncConnector):
    def __init__(self, context: IOContext, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]):
        self._ymq_context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._callback: Callable[[BaseMessage], Awaitable[None]] = callback
        self._socket: Optional[ConnectorSocket] = None

        self._pending_tasks: set[asyncio.Task] = set()

    def __del__(self):
        self.destroy()

    async def connect(
        self, address: AddressConfig, remote_type: ConnectorRemoteType, security_config: Optional[SecurityConfig] = None
    ) -> None:
        assert self._ymq_context is not None

        if remote_type not in {ConnectorRemoteType.Binder, ConnectorRemoteType.Connector}:
            raise ValueError(f"unsupported remote_type={remote_type}")

        self._address = address
        self._socket = ConnectorSocket.connect(
            self._ymq_context, self._identity.decode(), repr(self._address), tls_config=to_tls_config(security_config)
        )

    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        assert self._ymq_context is not None

        self._address = address
        self._socket = ConnectorSocket.bind(
            self._ymq_context, self._identity.decode(), repr(self._address), tls_config=to_tls_config(security_config)
        )

    def destroy(self):
        if self._socket is None:
            return

        for task in self._pending_tasks.copy():
            task.cancel()

        self._socket.shutdown()

        self._socket = None
        self._ymq_context = None

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def routine(self):
        message: Optional[BaseMessage] = await self.receive()
        if message is None:
            return

        await self._callback(message)

    async def receive(self) -> Optional[BaseMessage]:
        if self._ymq_context is None:
            return None

        if self._socket is None:
            return None

        msg = await self._socket.recv_message()
        payload_data = msg.payload.data
        if payload_data is None:
            return None

        result: Optional[BaseMessage] = deserialize(payload_data)
        if result is None:
            logger.error(f"received unknown message: {payload_data!r}")
            return None

        return result

    async def send(self, message: BaseMessage, *, detached: bool):
        if self._socket is None:
            return

        awaitable = self._socket.send_message(Bytes(serialize(message)))

        if not detached:
            await awaitable
            return

        self._pending_tasks.add(run_detached(awaitable, self.__get_prefix(), self._pending_tasks.discard))

    def __get_prefix(self) -> str:
        return f"{self.__class__.__name__}[{self._identity!r}]:"
