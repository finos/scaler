from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, List

from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.oci_hpc.worker import create_oci_hpc_worker
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)


class OCIHPCWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: OCIHPCWorkerManagerConfig) -> None:
        self._config = config
        self._base_concurrency = config.base_concurrency
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

    async def start_units(self, count: int) -> None:
        for _ in range(count):
            self._start_unit()

    async def stop_units(self, count: int) -> None:
        to_stop = self._units[:count]
        self._units = self._units[count:]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} worker process(es) but only {len(to_stop)} available.")
        for worker in to_stop:
            worker.terminate()
            logger.info(f"Stopped OCI HPC worker process {worker.name!r}")

    async def terminate(self) -> None:
        self._capacity_coordinator.cancel()
        for worker in self._units:
            worker.terminate()
        self._units.clear()

    def _start_unit(self) -> None:
        config = self._config
        container_instance_config = config.container_instance_config
        worker = create_oci_hpc_worker(
            name=f"oci-hpc-{len(self._units)}",
            address=config.worker_manager_config.effective_worker_scheduler_address,
            object_storage_address=config.worker_manager_config.object_storage_address,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            compartment_id=container_instance_config.compartment_id,
            availability_domain=container_instance_config.availability_domain,
            subnet_id=container_instance_config.subnet_id,
            container_image=container_instance_config.container_image,
            oci_region=container_instance_config.oci_region,
            object_storage_namespace=config.object_storage_namespace,
            object_storage_bucket=config.object_storage_bucket,
            object_storage_prefix=config.object_storage_prefix,
            instance_shape=container_instance_config.instance_shape,
            instance_ocpus=config.instance_ocpus,
            instance_memory_gb=config.instance_memory_gb,
            capabilities=self._capabilities,
            base_concurrency=self._base_concurrency,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            death_timeout_seconds=config.worker_config.death_timeout_seconds,
            task_queue_size=config.worker_config.per_worker_task_queue_size,
            io_threads=config.worker_config.io_threads,
            event_loop=config.worker_config.event_loop,
            job_timeout_seconds=config.job_timeout_seconds,
            oci_profile=container_instance_config.oci_profile,
            auth_type=container_instance_config.auth_type,
        )
        worker.start()
        self._units.append(worker)
        logger.info(f"Started OCI HPC worker process {worker.name!r}")


class OCIHPCWorkerManager:
    def __init__(self, config: OCIHPCWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logger.info(
            f"Starting OCI HPC Worker Manager\n"
            f"  Scheduler: {config.worker_manager_config.scheduler_address}\n"
            f"  Compartment: {config.container_instance_config.compartment_id}\n"
            f"  Region: {config.container_instance_config.oci_region}\n"
            f"  Object Storage: oci://{config.object_storage_bucket}/{config.object_storage_prefix}\n"
            f"  Container Image: {config.container_instance_config.container_image}\n"
            f"  Max Concurrent Jobs: {config.base_concurrency}\n"
            f"  Job Timeout: {config.job_timeout_seconds}s"
        )
        provisioner = OCIHPCWorkerProvisioner(config)
        runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_oci_hpc",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=-1,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=config.base_concurrency,
        )
        runner.run()
