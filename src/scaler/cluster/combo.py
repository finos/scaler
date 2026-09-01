import logging
import multiprocessing
import sys
from typing import Optional

import psutil

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.security import SecurityConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_PATHS,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.process_bootstrap import bootstrap_process
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

logger = logging.getLogger(__name__)

SCHEDULER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 10


def _run_native_worker_manager(worker_manager: NativeWorkerManager) -> None:
    # This process is spawned directly, so it must bootstrap its own logging/faulthandler.
    logging_config = worker_manager.config.logging_config
    bootstrap_process(
        logging_config.paths, logging_config.config_file, logging_config.level, process_name="worker_manager_native"
    )
    worker_manager.run()


class SchedulerClusterCombo:
    def __init__(
        self,
        n_workers: int,
        address: Optional[str] = None,
        object_storage_address: Optional[str] = None,
        monitor_address: Optional[str] = None,
        per_worker_capabilities: Optional[dict[str, int]] = None,
        worker_io_threads: int = DEFAULT_IO_THREADS,
        scheduler_io_threads: int = DEFAULT_IO_THREADS,
        max_number_of_tasks_waiting: int = DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        client_timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
        worker_timeout_seconds: int = DEFAULT_WORKER_TIMEOUT_SECONDS,
        object_retention_seconds: int = DEFAULT_OBJECT_RETENTION_SECONDS,
        task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS,
        death_timeout_seconds: int = DEFAULT_WORKER_DEATH_TIMEOUT,
        load_balance_seconds: int = DEFAULT_LOAD_BALANCE_SECONDS,
        load_balance_trigger_times: int = DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        garbage_collect_interval_seconds: int = DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes: int = DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        per_worker_task_queue_size: int = DEFAULT_PER_WORKER_QUEUE_SIZE,
        hard_processor_suspend: bool = DEFAULT_HARD_PROCESSOR_SUSPEND,
        protected: bool = True,
        scaler_policy: PolicyConfig = PolicyConfig(),
        event_loop: str = "builtin",
        logging_paths: tuple[str, ...] = DEFAULT_LOGGING_PATHS,
        logging_level: str = DEFAULT_LOGGING_LEVEL,
        logging_config_file: Optional[str] = None,
        worker_manager_id: str = "combo",
        security_config: Optional[SecurityConfig] = None,
    ):
        self._shutdown_called = False
        self._security_config = security_config if security_config is not None else SecurityConfig()

        if address is None:
            self._address = AddressConfig(SocketType.tcp, "127.0.0.1", get_available_tcp_port())
        else:
            self._address = AddressConfig.from_string(address)

        if object_storage_address is None:
            self._object_storage_address = AddressConfig(
                self._address.type, self._address.host, get_available_tcp_port()
            )
        else:
            self._object_storage_address = AddressConfig.from_string(object_storage_address)

        if monitor_address is None:
            self._monitor_address = None
        else:
            self._monitor_address = AddressConfig.from_string(monitor_address)

        self._object_storage = ObjectStorageServerProcess(
            bind_address=self._object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=logging_paths,
            logging_level=logging_level,
            logging_config_file=logging_config_file,
            security_config=self._security_config,
        )
        self._object_storage.start()
        self._object_storage.wait_until_ready()  # object storage should be ready before starting the cluster

        self._worker_manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=self._address,
                    worker_manager_id=worker_manager_id,
                    object_storage_address=self._object_storage_address,
                    max_task_concurrency=n_workers,
                ),
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities(per_worker_capabilities or {}),
                    per_worker_task_queue_size=per_worker_task_queue_size,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                    task_timeout_seconds=task_timeout_seconds,
                    death_timeout_seconds=death_timeout_seconds,
                    garbage_collect_interval_seconds=garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=trim_memory_threshold_bytes,
                    hard_processor_suspend=hard_processor_suspend,
                    io_threads=worker_io_threads,
                    event_loop=event_loop,
                ),
                logging_config=LoggingConfig(paths=logging_paths, config_file=logging_config_file, level=logging_level),
                security=self._security_config,
            )
        )

        self._worker_manager_process = multiprocessing.get_context("spawn").Process(
            target=_run_native_worker_manager, args=(self._worker_manager,)
        )

        # Synthesized signal-disposition for the scheduler. multiprocessing.Process.terminate() is
        # TerminateProcess on Windows, which never runs Python signal handlers, so the scheduler would
        # die abruptly without sending FIN to its connected workers and the workers' connector_external
        # would enter the YMQ retry loop. Setting this Event on shutdown lets the scheduler's daemon
        # waiter trigger the same graceful-shutdown path that SIGTERM triggers on POSIX.
        self._scheduler_shutdown_event = multiprocessing.get_context("spawn").Event()

        self._scheduler = SchedulerProcess(
            bind_address=self._address,
            object_storage_address=self._object_storage_address,
            advertised_object_storage_address=None,
            monitor_address=self._monitor_address,
            io_threads=scheduler_io_threads,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            client_timeout_seconds=client_timeout_seconds,
            worker_timeout_seconds=worker_timeout_seconds,
            object_retention_seconds=object_retention_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
            protected=protected,
            event_loop=event_loop,
            logging_paths=logging_paths,
            logging_config_file=logging_config_file,
            logging_level=logging_level,
            policy=scaler_policy,
            shutdown_event=self._scheduler_shutdown_event,
            security_config=self._security_config,
        )

        self._scheduler.start()
        self._worker_manager_process.start()
        logger.info(f"{self.__get_prefix()} started")

    def __del__(self):
        if not self._shutdown_called and not sys.is_finalizing():
            self.shutdown()

    def shutdown(self):
        self._shutdown_called = True

        logger.info(f"{self.__get_prefix()} shutdown")
        if self._worker_manager_process.is_alive():
            # On POSIX, multiprocessing.Process.terminate() sends SIGTERM and the worker manager's
            # signal handler iterates self._workers and terminates each child worker, which in turn
            # cleanly tears down their processors. On Windows terminate() is TerminateProcess --
            # the handler never runs and the worker / processor children become orphaned, kept
            # alive (and busy retrying YMQ connections) until they happen to notice the scheduler
            # has gone away. That orphaned-but-alive period is what added ~52s teardown latency
            # to test_cancel-style tests on the CI Windows runner. Snapshot the descendant tree
            # before terminating so we can directly TerminateProcess the orphans afterwards.
            descendants: list[psutil.Process] = []
            if sys.platform == "win32":
                try:
                    descendants = psutil.Process(self._worker_manager_process.pid).children(recursive=True)
                except psutil.NoSuchProcess:
                    descendants = []

            self._worker_manager_process.terminate()

            for proc in descendants:
                try:
                    proc.terminate()
                except psutil.NoSuchProcess:
                    pass
        self._worker_manager_process.join()

        if sys.platform == "win32":
            # Process.terminate() on Windows is TerminateProcess, which kills the scheduler
            # without running its signal handler -- the BinderSocket then dies without sending
            # FIN, and connected workers enter the YMQ reconnect retry loop. set the shutdown
            # event so the scheduler's daemon waiter triggers the same graceful path SIGTERM
            # triggers on POSIX, then fall back to terminate() if the scheduler does not exit
            # within the timeout.
            #
            # Skip the event.set() if the scheduler is already dead: multiprocessing.Event.set()
            # internally calls Condition.notify_all(), which waits for the sleeper to release the
            # woken_count counter. If the daemon waiter died with the scheduler (e.g. when the
            # test_scheduler_crash test deliberately crashes the scheduler), set() hangs forever
            # because no one is left to release that counter. We rely on terminate()+join() in
            # that case.
            if self._scheduler.is_alive():
                self._scheduler_shutdown_event.set()
                self._scheduler.join(timeout=SCHEDULER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS)
            if self._scheduler.is_alive():
                self._scheduler.terminate()
                self._scheduler.join()
        else:
            self._scheduler.terminate()
            self._scheduler.join()

        # object storage should terminate after the cluster and scheduler.
        self._object_storage.terminate()
        self._object_storage.join()

    def get_address(self) -> str:
        return repr(self._address)

    def __get_prefix(self):
        return f"{self.__class__.__name__}:"
