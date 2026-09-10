"""Route tasks to workers by capability with submit_verbose."""

import math
import multiprocessing

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.worker_manager.native.worker_manager import NativeWorkerManager


def gpu_task(x: float) -> float:
    return math.sqrt(x) * 2


def cpu_task(x: float) -> float:
    return x * 2


def main():
    cluster = SchedulerClusterCombo(
        n_workers=2, scaler_policy=PolicyConfig(policy_content="allocate=capability; scaling=no")
    )

    base_config = cluster._worker_manager.config
    base_worker_config = base_config.worker_config
    base_logging_config = base_config.logging_config
    gpu_manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=base_config.worker_manager_config.scheduler_address,
                worker_manager_id="test_manager",
                object_storage_address=base_config.worker_manager_config.object_storage_address,
                max_task_concurrency=1,
            ),
            mode=NativeWorkerManagerMode.FIXED,
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                per_worker_task_queue_size=base_worker_config.per_worker_task_queue_size,
                heartbeat_interval_seconds=base_worker_config.heartbeat_interval_seconds,
                task_timeout_seconds=base_worker_config.task_timeout_seconds,
                death_timeout_seconds=base_worker_config.death_timeout_seconds,
                garbage_collect_interval_seconds=base_worker_config.garbage_collect_interval_seconds,
                trim_memory_threshold_bytes=base_worker_config.trim_memory_threshold_bytes,
                hard_processor_suspend=base_worker_config.hard_processor_suspend,
                io_threads=base_worker_config.io_threads,
                event_loop=base_worker_config.event_loop,
            ),
            logging_config=LoggingConfig(
                paths=base_logging_config.paths,
                level=base_logging_config.level,
                config_file=base_logging_config.config_file,
            ),
        )
    )
    gpu_manager_process = multiprocessing.get_context("spawn").Process(target=gpu_manager.run)
    gpu_manager_process.start()

    with Client(address=cluster.get_address()) as client:
        gpu_future = client.submit_verbose(gpu_task, args=(16.0,), kwargs={}, capabilities={"gpu": 1})
        cpu_future = client.submit_verbose(cpu_task, args=(16.0,), kwargs={}, capabilities={})
        gpu_future.result()
        cpu_future.result()

    if gpu_manager_process.is_alive():
        gpu_manager_process.terminate()
    gpu_manager_process.join()
    cluster.shutdown()


if __name__ == "__main__":
    main()
