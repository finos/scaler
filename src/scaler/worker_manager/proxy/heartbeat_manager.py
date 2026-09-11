import time
from typing import TYPE_CHECKING, Dict, Optional

import psutil

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import Resource, WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.memory import get_memory_limit_and_available, get_process_memory
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager as HeartbeatManagerMixin
from scaler.worker.agent.mixins import TimeoutManager
from scaler.worker_manager.proxy.mixins import ProcessorStatusProvider

if TYPE_CHECKING:
    from scaler.worker_manager.proxy.task_manager import TaskManager


class HeartbeatManager(Looper, HeartbeatManagerMixin):
    def __init__(
        self,
        object_storage_address: Optional[AddressConfig],
        capabilities: Dict[str, int],
        task_queue_size: int,
        worker_manager_id: bytes,
        processor_status_provider: ProcessorStatusProvider,
        security_config: Optional[SecurityConfig] = None,
    ) -> None:
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size
        self._worker_manager_id = worker_manager_id
        self._processor_status_provider = processor_status_provider
        self._security_config = security_config

        self._agent_process = psutil.Process()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager: Optional["TaskManager"] = None
        self._timeout_manager: Optional[TimeoutManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

        self._object_storage_address: Optional[AddressConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager: "TaskManager",
        timeout_manager: TimeoutManager,
    ) -> None:
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._task_manager = worker_task_manager
        self._timeout_manager = timeout_manager
        self._processor_status_provider.set_task_manager(worker_task_manager)

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho) -> None:
        if self._start_timestamp_ns == 0:
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        if self._object_storage_address is None:
            address_message = heartbeat.objectStorageAddress
            scheme = SocketType(address_message.scheme)
            self._object_storage_address = AddressConfig(scheme, address_message.host, address_message.port)
            await self._connector_storage.connect(self._object_storage_address, security_config=self._security_config)

    def get_object_storage_address(self) -> Optional[AddressConfig]:
        return self._object_storage_address

    async def routine(self) -> None:
        if self._start_timestamp_ns != 0:
            return

        try:
            agent_cpu = int(self._agent_process.cpu_percent() * 10)
            agent_rss = get_process_memory(self._agent_process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            agent_cpu = 0
            agent_rss = 0

        mem_limit, mem_available = get_memory_limit_and_available()

        await self._connector_external.send(
            WorkerHeartbeat(
                agent=Resource(cpu=agent_cpu, rss=agent_rss),
                rssFree=mem_available,
                memLimit=mem_limit,
                queueSize=self._task_queue_size,
                queuedTasks=self._task_manager.get_queued_size(),
                latencyUS=self._latency_us,
                taskLock=not self._task_manager.can_accept_task(),
                processors=self._processor_status_provider.get_processor_statuses(),
                capabilities=dict_to_capabilities(self._capabilities),
                workerManagerID=self._worker_manager_id,
            ),
            detached=True,
        )
        self._start_timestamp_ns = time.time_ns()
