from __future__ import annotations

import logging
import multiprocessing.connection
import os
import signal
import sys
import uuid
from typing import TYPE_CHECKING

import psutil

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.utility.exitcode import describe_exitcode
from scaler.worker.worker import Worker
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)


class NativeWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.worker_config.event_loop
        self._preload = config.worker_config.preload
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._security_config = config.security

        if config.worker_type is not None:
            self._worker_prefix = config.worker_type
        elif config.mode == NativeWorkerManagerMode.FIXED:
            self._worker_prefix = "FIX"
        elif config.mode == NativeWorkerManagerMode.DYNAMIC:
            self._worker_prefix = "NAT"
        else:
            raise ValueError(f"worker_type is not set and mode is unrecognised: {config.mode!r}")

        self._workers: list[Worker] = []
        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=self._max_task_concurrency,
            scale_down_cooldown_seconds=config.worker_manager_config.scale_down_cooldown_seconds,
        )

    def _create_worker(self) -> Worker:
        return Worker(
            name=f"{self._worker_prefix}|{uuid.uuid4().hex}",
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            preload=self._preload,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            task_timeout_seconds=self._task_timeout_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            event_loop=self._event_loop,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
            worker_manager_id=self._worker_manager_id,
            security_config=self._security_config,
        )

    def run_fixed(self) -> None:
        workers: list[Worker] = []
        for _ in range(self._max_task_concurrency):
            worker = self._create_worker()
            worker.start()
            workers.append(worker)

        terminated_by_us: set[Worker] = set()

        def _on_signal(sig: int, frame: object) -> None:
            logger.info("NativeWorkerProvisioner (FIXED): received signal %d, terminating workers", sig)
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    terminated_by_us.add(worker)

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        workers_by_sentinel = {worker.sentinel: worker for worker in workers}
        while workers_by_sentinel:
            for sentinel in multiprocessing.connection.wait(list(workers_by_sentinel)):
                worker = workers_by_sentinel.pop(sentinel)
                worker.join()

                if worker in terminated_by_us:
                    logger.info(
                        f"native worker {worker.identity!r} stopped (exitcode={describe_exitcode(worker.exitcode)})"
                    )
                elif worker.exitcode == 0:
                    # A worker exits 0 only when it was told to stop (by the scheduler or a
                    # cancellation), never as a symptom of a problem, even though this manager
                    # was not the one that asked.
                    logger.info(f"native worker {worker.identity!r} shut down cleanly")
                else:
                    logger.warning(
                        f"native worker {worker.identity!r} exited unexpectedly "
                        f"(exitcode={describe_exitcode(worker.exitcode)})"
                    )

    async def set_desired_task_concurrency(
        self, requests: list[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        task_concurrency = extract_desired_count(requests, self._capabilities)
        await self._capacity_coordinator.set_desired_unit_count(task_concurrency)

    def active_unit_count(self) -> int:
        return len(self._workers)

    async def start_units(self, count: int) -> None:
        for _ in range(count):
            worker = self._create_worker()
            worker.start()
            self._workers.append(worker)
            logger.info(f"Started native worker {worker.identity!r}")

    async def stop_units(self, count: int) -> None:
        to_stop = self._workers[:count]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} worker(s) but only {len(to_stop)} available.")
        for worker in to_stop:
            if sys.platform == "win32":
                # Windows os.kill with SIGINT only works for processes attached to the same console.
                # TerminateProcess is forceful: the worker's teardown (which sends
                # WorkerDisconnectNotification before exiting) does not run, so the scheduler will
                # time out the worker on its own.
                psutil.Process(worker.pid).terminate()
            else:
                os.kill(worker.pid, signal.SIGINT)
            self._workers.pop(0)
            logger.info(f"Stopped native worker {worker.identity!r}")

    async def terminate(self) -> None:
        self._capacity_coordinator.cancel()
        await self.stop_units(len(self._workers))


class NativeWorkerManager:
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._config = config

    @property
    def config(self) -> NativeWorkerManagerConfig:
        return self._config

    def run(self) -> None:
        provisioner = NativeWorkerProvisioner(self._config)

        if self._config.mode == NativeWorkerManagerMode.FIXED:
            provisioner.run_fixed()
            return

        runner = WorkerManagerRunner(
            address=self._config.worker_manager_config.scheduler_address,
            name="worker_manager_native",
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=self._config.worker_manager_config.max_task_concurrency,
            worker_manager_id=self._config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=self._config.worker_config.io_threads,
            security_config=self._config.security,
        )
        runner.run()
