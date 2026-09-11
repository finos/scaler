import time
from typing import Optional

import psutil

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import ProcessorStatus, Resource, WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.memory import get_memory_limit_and_available, get_process_memory
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager, ProcessorManager, TaskManager, TimeoutManager
from scaler.worker.agent.processor_holder import ProcessorHolder


class VanillaHeartbeatManager(Looper, HeartbeatManager):
    def __init__(
        self,
        object_storage_address: Optional[AddressConfig],
        capabilities: dict[str, int],
        task_queue_size: int,
        worker_manager_id: bytes,
        security_config: Optional[SecurityConfig] = None,
    ):
        self._agent_process = psutil.Process()
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size
        self._worker_manager_id = worker_manager_id
        self._security_config = security_config

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._worker_task_manager: Optional[TaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None
        self._processor_manager: Optional[ProcessorManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

        self._object_storage_address: Optional[AddressConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager: TaskManager,
        timeout_manager: TimeoutManager,
        processor_manager: ProcessorManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager
        self._processor_manager = processor_manager

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        if self._object_storage_address is None:
            address_message = heartbeat.objectStorageAddress
            scheme = SocketType(address_message.scheme)
            self._object_storage_address = AddressConfig(scheme, address_message.host, address_message.port)
            await self._connector_storage.connect(self._object_storage_address, security_config=self._security_config)

    async def routine(self):
        processors = self._processor_manager.processors()

        if self._start_timestamp_ns != 0:
            # already sent heartbeat, expecting heartbeat echo, so not sending
            return

        for processor_holder in processors:
            try:
                status = processor_holder.process().status()
            except psutil.NoSuchProcess:
                # The OS process has already exited; treat as dead so it gets cleaned up.
                status = psutil.STATUS_DEAD
            if status in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}:
                await self._processor_manager.on_failing_processor(processor_holder.processor_id(), status)

        processors = self._processor_manager.processors()  # refreshes for removed dead and zombie processors
        num_suspended_processors = self._processor_manager.num_suspended_processors()

        mem_limit, mem_available = get_memory_limit_and_available()

        queued_tasks = self._worker_task_manager.get_queued_size() - num_suspended_processors
        assert queued_tasks >= 0, f"negative queued task count, {num_suspended_processors=}"

        # TODO: add task queue size to WorkerHeartbeat
        await self._connector_external.send(
            WorkerHeartbeat(
                agent=Resource(
                    cpu=int(self._agent_process.cpu_percent() * 10), rss=get_process_memory(self._agent_process)
                ),
                rssFree=mem_available,
                memLimit=mem_limit,
                queueSize=self._task_queue_size,
                queuedTasks=queued_tasks,
                latencyUS=self._latency_us,
                taskLock=self._processor_manager.can_accept_task(),
                processors=[self.__get_processor_status_from_holder(processor) for processor in processors],
                capabilities=dict_to_capabilities(self._capabilities),
                workerManagerID=self._worker_manager_id,
            ),
            detached=True,
        )
        self._start_timestamp_ns = time.time_ns()

    def get_object_storage_address(self) -> Optional[AddressConfig]:
        return self._object_storage_address

    @staticmethod
    def __get_processor_status_from_holder(processor: ProcessorHolder) -> ProcessorStatus:
        process = processor.process()

        try:
            resource = Resource(cpu=int(process.cpu_percent() * 10), rss=get_process_memory(process))
        except (psutil.ZombieProcess, psutil.NoSuchProcess):
            # Assumes dead/missing processes do not use any resources.
            resource = Resource(cpu=0, rss=0)

        return ProcessorStatus(
            pid=processor.pid(),
            initialized=processor.initialized(),
            hasTask=processor.task() is not None,
            suspended=processor.suspended(),
            resource=resource,
        )
