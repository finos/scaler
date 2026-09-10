from __future__ import annotations

import logging
import os
import signal
from typing import TYPE_CHECKING, List

from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.symphony.worker import create_symphony_worker
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)


class SymphonyWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: SymphonyWorkerManagerConfig) -> None:
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._service_name = config.service_name
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._event_loop = config.worker_config.event_loop
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()

        self._workers: List[WorkerProcess] = []
        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=self._max_task_concurrency,
            scale_down_cooldown_seconds=config.worker_manager_config.scale_down_cooldown_seconds,
        )

    def active_unit_count(self) -> int:
        return len(self._workers)

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        task_concurrency = extract_desired_count(requests, self._capabilities)
        await self._capacity_coordinator.set_desired_unit_count(task_concurrency)

    def _start_unit(self) -> None:
        worker = create_symphony_worker(
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            service_name=self._service_name,
            capabilities=self._capabilities,
            base_concurrency=self._max_task_concurrency,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            task_queue_size=self._task_queue_size,
            io_threads=self._io_threads,
            event_loop=self._event_loop,
            worker_manager_id=self._worker_manager_id,
        )
        worker.start()
        self._workers.append(worker)
        logger.info(f"Started Symphony worker {worker.identity!r}")

    async def start_units(self, count: int) -> None:
        for _ in range(count):
            self._start_unit()

    async def stop_units(self, count: int) -> None:
        to_stop = self._workers[:count]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} worker(s) but only {len(to_stop)} available.")
        for worker in to_stop:
            os.kill(worker.pid, signal.SIGINT)
            self._workers.pop(0)
            logger.info(f"Stopped Symphony worker {worker.identity!r}")

    async def terminate(self) -> None:
        self._capacity_coordinator.cancel()
        await self.stop_units(len(self._workers))


class SymphonyWorkerManager:
    def __init__(self, config: SymphonyWorkerManagerConfig) -> None:
        provisioner = SymphonyWorkerProvisioner(config)
        self._runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_symphony",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=config.worker_manager_config.max_task_concurrency,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
        )

    def run(self) -> None:
        self._runner.run()
