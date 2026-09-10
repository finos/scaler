import sys
import time
from concurrent.futures import Future
from typing import Optional

try:
    import psutil
except ModuleNotFoundError:
    if sys.platform != "emscripten":
        raise
    psutil = None

from scaler.client.agent.mixins import HeartbeatManager, ObjectManager
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import ClientHeartbeat, ClientHeartbeatEcho, Resource
from scaler.utility.mixins import Looper


class ClientHeartbeatManager(Looper, HeartbeatManager):
    def __init__(self, death_timeout_seconds: int, storage_address_future: Future):
        self._death_timeout_seconds = death_timeout_seconds
        self._object_storage_address = storage_address_future

        self._process = psutil.Process() if psutil is not None else None

        self._last_scheduler_contact = time.time()
        self._start_timestamp_ns = 0
        self._latency_us = 0
        self._connected = False

        self._connector_external: Optional[AsyncConnector] = None
        self._object_manager: Optional[ObjectManager] = None

    def register(self, connector_external: AsyncConnector):
        self._connector_external = connector_external

    async def send_heartbeat(self):
        if self._process is not None:
            cpu = int(self._process.cpu_percent() * 10)
            rss = self._process.memory_info().rss
        else:
            cpu = 0
            rss = 0
        await self._connector_external.send(
            ClientHeartbeat(resource=Resource(cpu=cpu, rss=rss), latencyUS=self._latency_us), detached=True
        )

    async def on_heartbeat_echo(self, heartbeat: ClientHeartbeatEcho):
        if not self._connected:
            self._connected = True

        self._last_scheduler_contact = time.time()
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0

        if self._object_storage_address.done():
            return

        object_storage_address_message = heartbeat.objectStorageAddress
        scheme = SocketType(object_storage_address_message.scheme)
        self._object_storage_address.set_result(
            AddressConfig(scheme, object_storage_address_message.host, object_storage_address_message.port)
        )

    async def routine(self):
        # On Pyodide the agent shares the single asyncio event loop with the
        # user's notebook code. Any long synchronous block in user code (large
        # cloudpickle (de)serialization, pargraph graph walking, big numpy
        # result hand-off) freezes the loop, so this routine cannot run. By
        # the time the loop is unblocked, ``time.time() - last_scheduler_contact``
        # already exceeds the death timeout and this self-check would falsely
        # raise, killing the agent mid-computation. The scheduler still runs
        # its own dead-client cleanup over the WebSocket, and the user can
        # interrupt the kernel manually, so skip the local check in browser.
        if sys.platform != "emscripten":
            if time.time() - self._last_scheduler_contact > self._death_timeout_seconds:
                raise TimeoutError(
                    f"Timeout when connecting to scheduler {self._connector_external.address} "
                    f"in {self._death_timeout_seconds} seconds"
                )

        if self._start_timestamp_ns != 0:
            # already sent heartbeat, expecting heartbeat echo, so not sending
            return

        await self.send_heartbeat()
        self._start_timestamp_ns = time.time_ns()
