import ctypes
import gc
import logging
import multiprocessing
import platform
import sys
import threading
import time
from typing import Any, Optional

import cloudpickle
import psutil

from scaler.client.serializer.mixins import Serializer
from scaler.config.defaults import CLEANUP_INTERVAL_SECONDS
from scaler.utility.exceptions import DeserializeObjectError
from scaler.utility.identifiers import ClientID, ObjectID

logger = logging.getLogger(__name__)


class ObjectCache(threading.Thread):
    def __init__(self, garbage_collect_interval_seconds: int, trim_memory_threshold_bytes: int):
        threading.Thread.__init__(self)

        self._serializers: dict[ClientID, Serializer] = dict()

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._previous_garbage_collect_time = time.time()
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes

        self._cached_objects: dict[ObjectID, Any] = {}
        self._cached_objects_alive_since: dict[ObjectID, float] = dict()
        self._process = psutil.Process(multiprocessing.current_process().pid)
        # Windows has no libc-style malloc_trim; skip the optional RSS-trim hook there.
        if sys.platform == "win32":
            self._libc: Optional[ctypes.CDLL] = None
        else:
            self._libc = ctypes.cdll.LoadLibrary(
                "libc.{}".format("so.6" if platform.uname()[0] != "Darwin" else "dylib")
            )

        self._stop_event = threading.Event()

    def run(self) -> None:
        try:
            while not self._stop_event.wait(timeout=CLEANUP_INTERVAL_SECONDS):
                self.__clean_memory()
        finally:
            self.__clear()  # gracefully destroy all cached objects

    def destroy(self) -> None:
        self._stop_event.set()

    def add_serializer(self, client: ClientID, serializer: Serializer):
        self._serializers[client] = serializer

    def serialize(self, client: ClientID, obj: Any) -> bytes:
        return self.get_serializer(client).serialize(obj)

    def deserialize(self, client: ClientID, payload: bytes) -> Any:
        return self.get_serializer(client).deserialize(payload)

    def add_object(self, client: ClientID, object_id: ObjectID, object_bytes: bytes) -> None:

        if object_id.is_serializer():
            self.add_serializer(client, cloudpickle.loads(object_bytes))
        else:
            try:
                deserialized: Any = self.deserialize(client, object_bytes)
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"failed to deserialize received {object_id!r}, length={len(object_bytes)}")
                deserialized = DeserializeObjectError(
                    f"failed to deserialize {object_id!r}: {type(exc).__name__}: {exc}"
                )

            self._cached_objects[object_id] = deserialized
            self._cached_objects_alive_since[object_id] = time.time()

    def del_object(self, object_id: ObjectID):
        self._cached_objects_alive_since.pop(object_id, None)
        self._cached_objects.pop(object_id, None)

    def has_object(self, object_id: ObjectID):
        return object_id in self._cached_objects or object_id in self._serializers

    def get_object(self, object_id: ObjectID) -> Optional[Any]:
        if object_id not in self._cached_objects:
            raise ValueError(f"cannot get object for {object_id!r}")

        obj = self._cached_objects[object_id]

        self._cached_objects_alive_since[object_id] = time.time()

        if isinstance(obj, DeserializeObjectError):
            raise obj

        return obj

    def get_serializer(self, client: ClientID) -> Serializer:
        serializer = self._serializers.get(client)

        if serializer is None:
            raise DeserializeObjectError(f"cannot get serializer for {client!r}")

        return serializer

    def __clean_memory(self):
        if time.time() - self._previous_garbage_collect_time < self._garbage_collect_interval_seconds:
            return

        self._previous_garbage_collect_time = time.time()

        gc.collect()

        if self._process.memory_info().rss < self._trim_memory_threshold_bytes:
            return

        if self._libc is not None:
            self._libc.malloc_trim(0)

    def __clear(self) -> None:
        self._cached_objects.clear()
        self._cached_objects_alive_since.clear()
