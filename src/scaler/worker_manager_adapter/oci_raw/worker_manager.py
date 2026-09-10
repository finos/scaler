from __future__ import annotations

import asyncio
import base64
import functools
import logging
import math
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

import oci

from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
from scaler.config.types.oci_auth_type import OCIAuthType
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count, format_capabilities, load_requirements_content
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)

_OCI_POLL_INTERVAL_SECONDS = 10
_OCI_MAX_POLL_ATTEMPTS = 30  # 5 minutes total


@dataclass
class _InstanceInfo:
    instance_id: str


class OCIRawWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: OCIRawWorkerManagerConfig, max_instances: int) -> None:
        self._config = config
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._instances: List[_InstanceInfo] = []
        self._container_instances_client: Any = None
        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=max_instances,
            scale_down_cooldown_seconds=config.worker_manager_config.scale_down_cooldown_seconds,
        )
        self._initialize_oci_client()

    def _initialize_oci_client(self) -> None:
        container_instance_config = self._config.container_instance_config
        if container_instance_config.auth_type == OCIAuthType.instance_principal:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            self._container_instances_client = oci.container_instances.ContainerInstanceClient(
                config={"region": container_instance_config.oci_region}, signer=signer
            )
        else:
            oci_config = oci.config.from_file(profile_name=container_instance_config.oci_profile)
            oci_config["region"] = container_instance_config.oci_region
            self._container_instances_client = oci.container_instances.ContainerInstanceClient(oci_config)

    def active_unit_count(self) -> int:
        return len(self._instances)

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        task_concurrency = extract_desired_count(requests, self._capabilities)
        workers_per_instance = max(1, int(self._config.instance_ocpus))
        new_desired = math.ceil(task_concurrency / workers_per_instance)
        await self._capacity_coordinator.set_desired_unit_count(new_desired)

    async def start_units(self, count: int) -> None:
        for _ in range(count):
            instance_id = await self._start_instance()
            if instance_id is not None:
                self._instances.append(_InstanceInfo(instance_id=instance_id))

    async def stop_units(self, count: int) -> None:
        to_stop = self._instances[:count]
        self._instances = self._instances[count:]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} Container Instance(s) but only {len(to_stop)} available.")
        for info in to_stop:
            await self._stop_instance(info.instance_id)

    async def terminate(self) -> None:
        self._capacity_coordinator.cancel()
        for info in self._instances:
            await self._stop_instance(info.instance_id)
        self._instances.clear()

    async def _start_instance(self) -> Optional[str]:
        config = self._config
        container_instance_config = config.container_instance_config
        num_workers = max(1, int(config.instance_ocpus))
        worker_config = config.worker_config
        scheduler_address = str(config.worker_manager_config.effective_worker_scheduler_address)
        requirements_content = load_requirements_content(config.python_worker_environment.requirements_txt)

        command = f"""scaler_worker_manager baremetal_native {scheduler_address!r} \
--mode fixed \
--worker-type OCI_RAW \
--max-task-concurrency {num_workers} \
--worker-manager-id {config.worker_manager_config.worker_manager_id} \
--per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \
--heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \
--task-timeout-seconds {worker_config.task_timeout_seconds} \
--garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \
--death-timeout-seconds {worker_config.death_timeout_seconds} \
--trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \
--event-loop {worker_config.event_loop} \
--io-threads {worker_config.io_threads}"""

        if worker_config.hard_processor_suspend:
            command += " --hard-processor-suspend"

        object_storage_address = config.worker_manager_config.object_storage_address
        if object_storage_address is not None:
            command += f" --object-storage-address {object_storage_address}"

        capabilities_str = format_capabilities(self._capabilities).strip()
        if capabilities_str:
            command += f" --per-worker-capabilities {capabilities_str}"

        image_pull_secrets = None
        if container_instance_config.image_pull_username and container_instance_config.image_pull_password:
            registry_endpoint = container_instance_config.container_image.split("/")[0]
            image_pull_secrets = [
                oci.container_instances.models.CreateBasicImagePullSecretDetails(
                    registry_endpoint=registry_endpoint,
                    username=base64.b64encode(container_instance_config.image_pull_username.encode()).decode(),
                    password=base64.b64encode(container_instance_config.image_pull_password.encode()).decode(),
                )
            ]

        display_name = f"scaler-worker-{uuid.uuid4().hex[:8]}"
        create_details = oci.container_instances.models.CreateContainerInstanceDetails(
            compartment_id=container_instance_config.compartment_id,
            availability_domain=container_instance_config.availability_domain,
            shape=container_instance_config.instance_shape,
            shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                ocpus=config.instance_ocpus, memory_in_gbs=config.instance_memory_gb
            ),
            containers=[
                oci.container_instances.models.CreateContainerDetails(
                    image_url=container_instance_config.container_image,
                    display_name="scaler-container",
                    environment_variables={
                        "COMMAND": command,
                        "PYTHON_REQUIREMENTS": requirements_content,
                        "PYTHON_VERSION": config.python_worker_environment.python_version,
                    },
                )
            ],
            vnics=[
                oci.container_instances.models.CreateContainerVnicDetails(subnet_id=container_instance_config.subnet_id)
            ],
            image_pull_secrets=image_pull_secrets,
            display_name=display_name,
        )

        loop = asyncio.get_running_loop()
        try:
            response = await loop.run_in_executor(
                None,
                functools.partial(
                    self._container_instances_client.create_container_instance,
                    create_container_instance_details=create_details,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            logger.error(f"OCI create_container_instance failed: {exc}")
            return None

        instance_id = response.data.id
        logger.info(f"OCI Container Instance {instance_id[-20:]} ({display_name}) created, waiting for ACTIVE...")

        for _ in range(_OCI_MAX_POLL_ATTEMPTS):
            await asyncio.sleep(_OCI_POLL_INTERVAL_SECONDS)
            try:
                poll_response = await loop.run_in_executor(
                    None,
                    functools.partial(
                        self._container_instances_client.get_container_instance, container_instance_id=instance_id
                    ),
                )
            except oci.exceptions.ServiceError as exc:
                logger.error(f"OCI get_container_instance failed for {instance_id[-20:]}: {exc}")
                await self._delete_instance(instance_id)
                return None

            instance = poll_response.data
            if instance.lifecycle_state == "ACTIVE":
                logger.info(f"OCI Container Instance {instance_id[-20:]} ({display_name}) is ACTIVE")
                return instance_id
            if instance.lifecycle_state == "FAILED":
                details = instance.lifecycle_details or "no details provided"
                logger.error(
                    f"OCI Container Instance {instance_id[-20:]} ({display_name}) failed to provision: {details}"
                )
                await self._delete_instance(instance_id)
                return None

        timeout_seconds = _OCI_MAX_POLL_ATTEMPTS * _OCI_POLL_INTERVAL_SECONDS
        logger.warning(
            f"OCI Container Instance {instance_id[-20:]} ({display_name}) timed out after "
            f"{timeout_seconds}s waiting for ACTIVE, deleting"
        )
        await self._delete_instance(instance_id)
        return None

    async def _delete_instance(self, instance_id: str) -> bool:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._container_instances_client.delete_container_instance, container_instance_id=instance_id
                ),
            )
            return True
        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                logger.warning(f"OCI Container Instance {instance_id[-20:]} not found during delete (already gone?)")
            else:
                logger.error(f"OCI delete_container_instance failed for {instance_id[-20:]}: {exc}")
        except Exception as exc:
            logger.error(f"Failed to delete OCI Container Instance {instance_id[-20:]}: {exc}")
        return False

    async def _stop_instance(self, instance_id: str) -> None:
        if await self._delete_instance(instance_id):
            logger.info(f"Stopped OCI Container Instance {instance_id[-20:]}")
        else:
            logger.error(f"Failed to stop OCI Container Instance {instance_id[-20:]}")


class OCIRawWorkerManager:
    def __init__(self, config: OCIRawWorkerManagerConfig) -> None:
        workers_per_instance = max(1, int(config.instance_ocpus))
        mtc = config.worker_manager_config.max_task_concurrency
        max_instances = math.ceil(mtc / workers_per_instance) if mtc != -1 else -1
        provisioner = OCIRawWorkerProvisioner(config, max_instances)
        self._runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_oci_raw",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=max_instances,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=workers_per_instance,
        )

    def run(self) -> None:
        self._runner.run()
