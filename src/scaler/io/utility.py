import logging
import os
import uuid
from typing import Optional

from scaler.config.defaults import CAPNP_DATA_SIZE_LIMIT, CAPNP_MESSAGE_SIZE_LIMIT
from scaler.protocol.capnp import BaseMessage, Message
from scaler.protocol.helpers import PROTOCOL
from scaler.utility.exceptions import CapnpDeserializationError

logger = logging.getLogger(__name__)

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer


def generate_identity_from_name(name: str) -> bytes:
    return f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()


def deserialize(data: Buffer) -> Optional[BaseMessage]:
    try:
        payload = Message.from_bytes(bytes(data), traversal_limit_in_words=CAPNP_MESSAGE_SIZE_LIMIT)
    except (ValueError, RuntimeError) as e:
        raise CapnpDeserializationError(str(e)) from e
    if not hasattr(payload, payload.which()):
        logger.error(f"unknown message type: {payload.which()}")
        return None

    return getattr(payload, payload.which())


def serialize(message: BaseMessage) -> bytes:
    payload = Message(**{PROTOCOL.inverse[type(message)]: message})
    return payload.to_bytes()


def chunk_to_list_of_bytes(data: bytes) -> list[bytes]:
    # TODO: change to list of memoryview when capnp can support memoryview
    return [data[i : i + CAPNP_DATA_SIZE_LIMIT] for i in range(0, len(data), CAPNP_DATA_SIZE_LIMIT)]


def concat_list_of_bytes(data: list[bytes]) -> bytes:
    return b"".join(data)
