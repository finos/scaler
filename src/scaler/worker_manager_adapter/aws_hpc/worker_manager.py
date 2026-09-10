from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, List

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig, AWSHPCBackend
from scaler.worker_manager_adapter.aws_hpc.worker import create_aws_batch_worker
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)


class BatchWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: AWSBatchWorkerManagerConfig) -> None:
        self._config = config
        self._base_concurrency = config.max_concurrent_jobs
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._units: List[WorkerProcess] = []
        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=-1,
            scale_down_cooldown_seconds=config.worker_manager_config.scale_down_cooldown_seconds,
        )

    def active_unit_count(self) -> int:
        return len(self._units)

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        task_concurrency = extract_desired_count(requests, self._capabilities)
        new_desired = math.ceil(task_concurrency / self._base_concurrency)
        await self._capacity_coordinator.set_desired_unit_count(new_desired)

    def _start_unit(self) -> None:
        config = self._config
        worker = create_aws_batch_worker(
            name=config.name,
            address=config.worker_manager_config.effective_worker_scheduler_address,
            object_storage_address=config.worker_manager_config.object_storage_address,
            job_queue=config.job_queue,
            job_definition=config.job_definition,
            aws_region=config.aws_region,
            s3_bucket=config.s3_bucket,
            s3_prefix=config.s3_prefix,
            capabilities=self._capabilities,
            base_concurrency=self._base_concurrency,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            death_timeout_seconds=config.worker_config.death_timeout_seconds,
            task_queue_size=config.worker_config.per_worker_task_queue_size,
            io_threads=config.worker_config.io_threads,
            event_loop=config.worker_config.event_loop,
            job_timeout_seconds=config.job_timeout_minutes * 60,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
        )
        worker.start()
        self._units.append(worker)
        logger.info(f"Started Batch worker process {worker.name!r}")

    async def start_units(self, count: int) -> None:
        for _ in range(count):
            self._start_unit()

    async def stop_units(self, count: int) -> None:
        to_stop = self._units[:count]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} worker process(es) but only {len(to_stop)} available.")
        for worker in to_stop:
            worker.terminate()
            self._units.pop(0)
            logger.info(f"Stopped Batch worker process {worker.name!r}")

    async def terminate(self) -> None:
        self._capacity_coordinator.cancel()
        for worker in self._units:
            worker.terminate()
        self._units.clear()


class AWSHPCWorkerManager:
    def __init__(self, config: AWSBatchWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logger.info(f"Starting AWS HPC Worker Manager (backend: {config.backend.name})")
        if config.backend != AWSHPCBackend.batch:
            raise NotImplementedError(f"backend {config.backend.name!r} is not yet implemented")

        provisioner = BatchWorkerProvisioner(config)
        runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_aws_hpc",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=-1,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=config.max_concurrent_jobs,
        )
        runner.run()
