import abc
import dataclasses
import struct
import threading
from datetime import timedelta
from enum import Enum
from typing import Awaitable, Callable, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import BaseMessage, BinderStatus
from scaler.utility.identifiers import ObjectID
from scaler.utility.mixins import Looper, Reporter


class ConnectorRemoteType(Enum):
    # Connector connects to a binder
    Binder = "binder"

    # Connector connects to another connector
    Connector = "connector"


class NetworkBackend(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def create_internal_address(name: str, same_process: bool) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_binder(
        self, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]
    ) -> "AsyncBinder":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_connector(
        self, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
    ) -> "AsyncConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_publisher(self, identity: bytes) -> "AsyncPublisher":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_connector(
        self,
        identity: bytes,
        connector_remote_type: ConnectorRemoteType,
        address: AddressConfig,
        security_config: Optional[SecurityConfig] = None,
    ) -> "SyncConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_object_storage_connector(self, identity: bytes) -> "AsyncObjectStorageConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_object_storage_connector(
        self, identity: bytes, address: AddressConfig, security_config: Optional[SecurityConfig] = None
    ) -> "SyncObjectStorageConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_subscriber(
        self,
        identity: bytes,
        address: AddressConfig,
        callback: Callable[[BaseMessage], None],
        timeout: Optional[timedelta],
        security_config: Optional[SecurityConfig] = None,
    ) -> "SyncSubscriber":
        raise NotImplementedError()


class AsyncBinder(Looper, Reporter, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, to: bytes, message: BaseMessage, *, detached: bool):
        """Sends a message to one peer.

        A detached send returns once the send is scheduled and logs whatever failure follows, so a peer that
        left -- or one that never connected, which parks a send forever -- can neither raise into nor block a
        caller that has already moved on. Ask for an attached send only when the caller has to know the
        message left the process: it waits for that and raises when it cannot, which is what the graceful
        shutdown path relies on.

        The two do not order against each other on one socket: a detached send is enqueued on the next loop
        iteration, so an attached one issued after it leaves first.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self) -> BinderStatus:
        raise NotImplementedError()


class AsyncConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(
        self, address: AddressConfig, remote_type: ConnectorRemoteType, security_config: Optional[SecurityConfig] = None
    ) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: BaseMessage, *, detached: bool):
        """Sends a message to the remote end.

        Detaching means the same here as it does for a binder, see ``AsyncBinder.send``.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self) -> Optional[BaseMessage]:
        raise NotImplementedError()


class AsyncPublisher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: BaseMessage):
        raise NotImplementedError()


class SyncConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def send(self, message: BaseMessage):
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self) -> Optional[BaseMessage]:
        raise NotImplementedError()


@dataclasses.dataclass(frozen=True)
class ObjectStorageTotals:
    """What the object storage server holds, as its infoGetTotal request answers.

    Fields the server does not answer with read as 0, so a newer reader stays usable against an older one.
    """

    object_count: int = 0  # object IDs the server holds
    unique_object_count: int = 0  # distinct payloads behind them, so IDs minus what deduplication shares
    total_bytes: int = 0  # bytes those payloads occupy
    pending_request_count: int = 0  # requests waiting for an object that does not exist yet
    pending_object_count: int = 0  # distinct objects those requests wait for
    oldest_pending_seconds: int = 0  # how long the oldest of them has waited

    @classmethod
    def from_payload(cls, payload: bytes) -> "ObjectStorageTotals":
        """Read the fields the server answered with and leave the rest at 0."""
        field_count = min(len(payload) // struct.calcsize("<Q"), len(dataclasses.fields(cls)))
        return cls(*struct.unpack_from(f"<{field_count}Q", payload))


class AsyncObjectStorageConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None):
        raise NotImplementedError()

    @abc.abstractmethod
    async def wait_until_connected(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def delete_object(self, object_id: ObjectID) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def info_get_total(self) -> ObjectStorageTotals:
        raise NotImplementedError()


class SyncObjectStorageConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_object(self, object_id: ObjectID, payload: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_object(self, object_id: ObjectID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        raise NotImplementedError()


class SyncSubscriber(threading.Thread, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self) -> None:
        raise NotImplementedError()
