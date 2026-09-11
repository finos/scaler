import multiprocessing
import unittest
from concurrent.futures import TimeoutError

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name


class TestCapabilities(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self._workers = 3
        self.combo = SchedulerClusterCombo(
            n_workers=self._workers,
            event_loop="builtin",
            scaler_policy=PolicyConfig(policy_content="allocate=capability; scaling=no"),
        )
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_capabilities(self):
        base_config = self.combo._worker_manager.config

        with Client(self.address) as client:
            client.submit(round, 3.14).result()  # Ensures the cluster is ready

            future = client.submit_verbose(round, args=(3.14,), kwargs={}, capabilities={"gpu": 1})

            # No worker can accept the task, should timeout
            with self.assertRaises(TimeoutError):
                future.result(timeout=1)

            # Connects a worker that can handle the task
            gpu_manager = NativeWorkerManager(
                NativeWorkerManagerConfig(
                    worker_manager_config=WorkerManagerConfig(
                        scheduler_address=base_config.worker_manager_config.scheduler_address,
                        worker_manager_id="test_manager",
                        object_storage_address=None,
                        max_task_concurrency=1,
                    ),
                    mode=NativeWorkerManagerMode.FIXED,
                    worker_config=WorkerConfig(
                        per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                        per_worker_task_queue_size=base_config.worker_config.per_worker_task_queue_size,
                        heartbeat_interval_seconds=base_config.worker_config.heartbeat_interval_seconds,
                        task_timeout_seconds=base_config.worker_config.task_timeout_seconds,
                        death_timeout_seconds=base_config.worker_config.death_timeout_seconds,
                        garbage_collect_interval_seconds=base_config.worker_config.garbage_collect_interval_seconds,
                        trim_memory_threshold_bytes=base_config.worker_config.trim_memory_threshold_bytes,
                        hard_processor_suspend=base_config.worker_config.hard_processor_suspend,
                        io_threads=1,
                        event_loop=base_config.worker_config.event_loop,
                    ),
                    logging_config=LoggingConfig(
                        paths=base_config.logging_config.paths,
                        level=base_config.logging_config.level,
                        config_file=base_config.logging_config.config_file,
                    ),
                )
            )
            gpu_process = multiprocessing.get_context("spawn").Process(target=gpu_manager.run)
            gpu_process.start()

            self.assertEqual(future.result(), 3.0)

            gpu_process.terminate()
            gpu_process.join()

    def test_graph_capabilities(self):
        base_config = self.combo._worker_manager.config

        with Client(self.address) as client:
            client.submit(round, 3.14).result()  # Ensures the cluster is ready

            graph = {"a": 2.3, "b": 3.1, "c": (round, "a"), "d": (round, "b"), "e": (pow, "c", "d")}

            future = client.get(graph, keys=["e"], capabilities={"gpu": 1}, block=False)["e"]

            with self.assertRaises(TimeoutError):
                future.result(timeout=1)

            # Connect a worker that can handle the task
            gpu_manager = NativeWorkerManager(
                NativeWorkerManagerConfig(
                    worker_manager_config=WorkerManagerConfig(
                        scheduler_address=base_config.worker_manager_config.scheduler_address,
                        worker_manager_id="test_manager",
                        object_storage_address=None,
                        max_task_concurrency=1,
                    ),
                    mode=NativeWorkerManagerMode.FIXED,
                    worker_config=WorkerConfig(
                        per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                        per_worker_task_queue_size=base_config.worker_config.per_worker_task_queue_size,
                        heartbeat_interval_seconds=base_config.worker_config.heartbeat_interval_seconds,
                        task_timeout_seconds=base_config.worker_config.task_timeout_seconds,
                        death_timeout_seconds=base_config.worker_config.death_timeout_seconds,
                        garbage_collect_interval_seconds=base_config.worker_config.garbage_collect_interval_seconds,
                        trim_memory_threshold_bytes=base_config.worker_config.trim_memory_threshold_bytes,
                        hard_processor_suspend=base_config.worker_config.hard_processor_suspend,
                        io_threads=1,
                        event_loop=base_config.worker_config.event_loop,
                    ),
                    logging_config=LoggingConfig(
                        paths=base_config.logging_config.paths,
                        level=base_config.logging_config.level,
                        config_file=base_config.logging_config.config_file,
                    ),
                )
            )
            gpu_process = multiprocessing.get_context("spawn").Process(target=gpu_manager.run)
            gpu_process.start()

            self.assertEqual(future.result(), 8)

            gpu_process.terminate()
            gpu_process.join()
