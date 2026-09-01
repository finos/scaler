import uuid
from functools import partial
from typing import Optional

from scaler.config.types.address import AddressConfig
from scaler.worker_manager_adapter.symphony.heartbeat_manager import SymphonyProcessorStatusProvider
from scaler.worker_manager_adapter.symphony.task_manager import SymphonyExecutionBackend
from scaler.worker_manager_adapter.worker_process import WorkerProcess


def create_symphony_worker(
    address: AddressConfig,
    object_storage_address: Optional[AddressConfig],
    service_name: str,
    capabilities: dict[str, int],
    base_concurrency: int,
    heartbeat_interval_seconds: int,
    death_timeout_seconds: int,
    task_queue_size: int,
    io_threads: int,
    event_loop: str,
    worker_manager_id: bytes,
) -> WorkerProcess:
    return WorkerProcess(
        name=f"SYM|{uuid.uuid4().hex}",
        address=address,
        object_storage_address=object_storage_address,
        capabilities=capabilities,
        base_concurrency=base_concurrency,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        death_timeout_seconds=death_timeout_seconds,
        task_queue_size=task_queue_size,
        io_threads=io_threads,
        event_loop=event_loop,
        worker_manager_id=worker_manager_id,
        processor_status_provider_factory=SymphonyProcessorStatusProvider,
        execution_backend_factory=partial(SymphonyExecutionBackend, service_name=service_name),
    )
