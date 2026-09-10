"""The ``soamapi.Message`` subclass that carries task payloads.

See this package's ``__init__`` for the import rule that keeps it reachable without Symphony.
"""

from __future__ import annotations

import array
from typing import Union

import soamapi

# `OutputStream.write_byte_array` takes an `array.array` and `InputStream.read_byte_array` returns
# one, so a payload is bytes on the way out and an `array.array` on the way back in.
Payload = Union[bytes, array.array]


class SoamMessage(soamapi.Message):
    def __init__(self, payload: Payload = b"") -> None:
        self._payload = payload

    def set_payload(self, payload: Payload) -> None:
        self._payload = payload

    def get_payload(self) -> Payload:
        return self._payload

    def on_serialize(self, stream: soamapi.OutputStream) -> None:
        payload_array = array.array("b", self._payload)
        stream.write_byte_array(payload_array, 0, len(payload_array))

    def on_deserialize(self, stream: soamapi.InputStream) -> None:
        self._payload = stream.read_byte_array("b")
