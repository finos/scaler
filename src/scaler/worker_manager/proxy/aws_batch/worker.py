"""
AWS Batch worker factory.

Creates a WorkerProcess configured to submit tasks to AWS Batch for execution.
"""

import uuid
from functools import partial
from typing import Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.worker_manager.proxy.aws_batch.execution_backend import AWSBatchExecutionBackend
from scaler.worker_manager.proxy.aws_batch.processor_status import AWSBatchProcessorStatusProvider
from scaler.worker_manager.proxy.worker_process import WorkerProcess


def create_aws_batch_worker(
    address: AddressConfig,
    object_storage_address: Optional[AddressConfig],
    job_queue: str,
    job_definition: str,
    aws_region: str,
    s3_bucket: str,
    worker_manager_id: bytes,
    name: Optional[str] = None,
    s3_prefix: str = "scaler-tasks",
    capabilities: Optional[Dict[str, int]] = None,
    base_concurrency: int = 100,
    heartbeat_interval_seconds: int = 1,
    death_timeout_seconds: int = 30,
    task_queue_size: int = 1000,
    io_threads: int = 2,
    event_loop: str = "builtin",
    job_timeout_seconds: int = 3600,
) -> WorkerProcess:
    return WorkerProcess(
        name=name or f"AWS|{uuid.uuid4().hex}",
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
        processor_status_provider_factory=AWSBatchProcessorStatusProvider,
        execution_backend_factory=partial(
            AWSBatchExecutionBackend,
            job_queue=job_queue,
            job_definition=job_definition,
            aws_region=aws_region,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            job_timeout_seconds=job_timeout_seconds,
        ),
        idle_sleep_seconds=0.1,
    )
