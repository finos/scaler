import asyncio
import logging
from typing import Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncObjectStorageConnector
from scaler.io.ymq import Bytes, ConnectorSocket, IOContext, YMQException
from scaler.io.ymq.utils import to_tls_config
from scaler.protocol.capnp import ObjectRequestHeader, ObjectResponseHeader
from scaler.protocol.helpers import from_capnp_object_id, to_capnp_object_id
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ObjectID

logger = logging.getLogger(__name__)


class YMQAsyncObjectStorageConnector(AsyncObjectStorageConnector):
    """An asyncio connector that uses YMQ to connect to a Scaler's object storage instance."""

    RESPONSE_HEADER_LENGTH = 80

    def __init__(self, context: IOContext, identity: bytes):
        self._ymq_context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._connected_event = asyncio.Event()

        self._next_request_id = 0
        self._pending_get_requests: dict[ObjectID, asyncio.Future] = {}

        self._lock = asyncio.Lock()
        self._socket: Optional[ConnectorSocket] = None

    def __del__(self):
        self.destroy()

    async def connect(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None):
        self._address = address

        if self.is_connected():
            raise ObjectStorageException("connector is already connected.")

        assert self._ymq_context is not None
        self._socket = ConnectorSocket.connect(
            self._ymq_context, self._identity.decode(), repr(address), tls_config=to_tls_config(security_config)
        )
        self._connected_event.set()

    async def wait_until_connected(self):
        await self._connected_event.wait()

    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    def destroy(self):
        if self._socket is None:
            return

        self._socket.shutdown()

        self._socket = None
        self._ymq_context = None

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def routine(self):
        await self.wait_until_connected()

        response = await self.__receive_response()
        if response is None:
            return

        header, payload = response

        if header.responseType != ObjectResponseHeader.ObjectResponseType.getOK:
            return

        pending_get_future = self._pending_get_requests.pop(from_capnp_object_id(header.objectID), None)

        if pending_get_future is None:
            logger.warning(f"unknown get-ok response for unrequested object_id={repr(header.objectID)}.")
            return

        pending_get_future.set_result(payload)

    async def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        await self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.setObject, payload)

    async def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytes:
        pending_get_future = self._pending_get_requests.get(object_id)

        if pending_get_future is None:
            pending_get_future = asyncio.Future()
            self._pending_get_requests[object_id] = pending_get_future

            await self.__send_request(
                object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.getObject, None
            )

        return await pending_get_future

    async def delete_object(self, object_id: ObjectID) -> None:
        await self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.deleteObject, None)

    async def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        object_id_payload = to_capnp_object_id(object_id).to_bytes()

        await self.__send_request(
            new_object_id,
            len(object_id_payload),
            ObjectRequestHeader.ObjectRequestType.duplicateObjectID,
            object_id_payload,
        )

    def __ensure_is_connected(self):
        if self._socket is None:
            raise ObjectStorageException("connector is not connected.")

    async def __send_request(
        self,
        object_id: ObjectID,
        payload_length: int,
        request_type: ObjectRequestHeader.ObjectRequestType,
        payload: Optional[bytes],
    ):
        self.__ensure_is_connected()

        request_id = self._next_request_id
        self._next_request_id += 1
        self._next_request_id %= 2**64 - 1  # UINT64_MAX

        header = ObjectRequestHeader(
            objectID=to_capnp_object_id(object_id),
            payloadLength=payload_length,
            requestID=request_id,
            requestType=request_type,
        )

        try:
            async with self._lock:
                await self.__write_request_header(header)

                if payload is not None:
                    await self.__write_request_payload(payload)

        except YMQException as e:
            self._socket = None
            raise ObjectStorageException("connection failure to object storage server.") from e

    async def __write_request_header(self, header: ObjectRequestHeader):
        assert self._socket is not None
        await self._socket.send_message(Bytes(header.to_bytes()))

    async def __write_request_payload(self, payload: bytes):
        assert self._socket is not None
        await self._socket.send_message(Bytes(payload))

    async def __receive_response(self) -> Optional[tuple[ObjectResponseHeader, bytes]]:
        if self._socket is None:
            return None

        try:
            header = await self.__read_response_header()
            payload = await self.__read_response_payload(header)
        except YMQException as e:
            self._socket = None
            raise ObjectStorageException("connection failure to object storage server.") from e

        return header, payload

    async def __read_response_header(self) -> ObjectResponseHeader:
        assert self._socket is not None

        msg = await self._socket.recv_message()
        header_data = msg.payload.data
        assert header_data is not None
        assert len(header_data) == self.RESPONSE_HEADER_LENGTH

        return ObjectResponseHeader.from_bytes(header_data)

    async def __read_response_payload(self, header: ObjectResponseHeader) -> bytes:
        assert self._socket is not None

        if header.payloadLength > 0:
            res = await self._socket.recv_message()
            payload_data = res.payload.data
            assert payload_data is not None
            assert len(payload_data) == header.payloadLength
            return payload_data
        else:
            return b""
