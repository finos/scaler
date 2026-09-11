from functools import partial
from typing import Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.config.types.oci_auth_type import OCIAuthType
from scaler.worker_manager.proxy.oci.execution_backend import OCIExecutionBackend
from scaler.worker_manager.proxy.oci.processor_status import OCIProcessorStatusProvider
from scaler.worker_manager.proxy.worker_process import WorkerProcess


def create_oci_worker(
    name: str,
    address: AddressConfig,
    object_storage_address: Optional[AddressConfig],
    worker_manager_id: bytes,
    compartment_id: str,
    availability_domain: str,
    subnet_id: str,
    container_image: str,
    oci_region: str,
    object_storage_namespace: str,
    object_storage_bucket: str,
    object_storage_prefix: str = "scaler-tasks",
    instance_shape: str = "CI.Standard.E4.Flex",
    instance_ocpus: float = 1.0,
    instance_memory_gb: float = 6.0,
    capabilities: Optional[Dict[str, int]] = None,
    base_concurrency: int = 100,
    heartbeat_interval_seconds: int = 1,
    death_timeout_seconds: int = 30,
    task_queue_size: int = 1000,
    io_threads: int = 2,
    event_loop: str = "builtin",
    job_timeout_seconds: int = 3600,
    oci_profile: str = "DEFAULT",
    auth_type: OCIAuthType = OCIAuthType.config_file,
) -> WorkerProcess:
    return WorkerProcess(
        name=name,
        address=address,
        object_storage_address=object_storage_address,
        capabilities=capabilities or {},
        base_concurrency=base_concurrency,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        death_timeout_seconds=death_timeout_seconds,
        task_queue_size=task_queue_size,
        io_threads=io_threads,
        event_loop=event_loop,
        worker_manager_id=worker_manager_id,
        processor_status_provider_factory=OCIProcessorStatusProvider,
        execution_backend_factory=partial(
            OCIExecutionBackend,
            compartment_id=compartment_id,
            availability_domain=availability_domain,
            subnet_id=subnet_id,
            container_image=container_image,
            oci_region=oci_region,
            object_storage_namespace=object_storage_namespace,
            object_storage_bucket=object_storage_bucket,
            object_storage_prefix=object_storage_prefix,
            instance_shape=instance_shape,
            instance_ocpus=instance_ocpus,
            instance_memory_gb=instance_memory_gb,
            job_timeout_seconds=job_timeout_seconds,
            oci_profile=oci_profile,
            auth_type=auth_type,
        ),
        idle_sleep_seconds=0.1,
    )
