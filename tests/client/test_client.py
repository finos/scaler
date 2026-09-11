import functools
import logging
import multiprocessing
import os
import random
import sys
import tempfile
import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.exceptions import DisconnectedError, MissingObjects, ProcessorDiedError
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger
from scaler.worker.preload import PreloadSpecError, _parse_preload_spec, execute_preload
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name


def noop(sec: int):
    return sec * 1


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


def heavy_function(sec: int, payload: bytes):
    return len(payload) * sec


def raise_exception(foo: int):
    if foo == 11:
        raise ValueError("foo cannot be 100")


def get_preloaded_value():
    """Function that retrieves value set by preload"""
    from tests.utility.utility import get_global_value

    return get_global_value()


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self._workers = 3
        self.combo = SchedulerClusterCombo(n_workers=self._workers, event_loop="builtin")
        self.address = self.combo.get_address()
        # self.address = f"tcp://127.0.0.1:2345"

    def tearDown(self) -> None:
        self.combo.shutdown()
        pass

    def test_one_submit(self):
        with Client(self.address) as client:
            with ScopedLogger("submitting 1 task"):
                future1 = client.submit(noop, 1)

            self.assertEqual(future1.result(), 1)

    def test_one_map(self):
        with Client(self.address) as client:
            with ScopedLogger("mapping 1 task"):
                result = client.map(noop, [1, 2])

            self.assertEqual(result, [1, 2])

    def test_submit_and_map(self):
        with Client(self.address) as client:
            tasks = [random.randint(0, 100) for _ in range(100)]
            with ScopedLogger(f"submitting {len(tasks)} tasks using map"):
                map_results = client.map(noop, tasks)

            self.assertEqual(map_results, tasks)

            with ScopedLogger(f"submitting {len(tasks)} tasks using submit"):
                futures = [client.submit(noop, i) for i in tasks]
                submit_results = [future.result() for future in futures]

            self.assertEqual(submit_results, tasks)

    def test_noop_submit(self):
        with Client(self.address) as client:
            tasks = [random.randint(0, 100) for _ in range(10000)]
            with ScopedLogger(f"submit {len(tasks)} noop tasks"):
                futures = [client.submit(noop, i) for i in tasks]

            with ScopedLogger(f"gather {len(futures)} results"):
                results = [future.result() for future in futures]

            self.assertEqual(results, tasks)

    def test_noop_map(self):
        with Client(self.address) as client:
            tasks = [random.randint(0, 100) for _ in range(10000)]
            with ScopedLogger(f"submit {len(tasks)} noop tasks"):
                results = client.map(noop, tasks)

            self.assertEqual(results, tasks)

    def test_noop_cancel(self):
        with Client(self.address) as client:
            tasks = [10, 1, 1] * 10
            with ScopedLogger(f"submit {len(tasks)} noop and cancel tasks"):
                futures = [client.submit(noop_sleep, i) for i in tasks]
                assert isinstance(futures, list)

                for future in futures:
                    future.cancel()

                time.sleep(3)

        time.sleep(1)

    def test_cancel_unassigned(self):
        # Cancels a graph task that hasn't been assigned to a worker yet.
        combo = SchedulerClusterCombo(n_workers=0, event_loop="builtin")

        with Client(combo.get_address()) as client:
            future = client.submit(round, 31.416)

            time.sleep(0.15)

            future.cancel()

        combo.shutdown()

    def test_heavy_function(self):
        with Client(self.address) as client:
            size = 500_000_000
            number_of_tasks = 10000
            tasks = [random.randint(0, 100) for _ in range(number_of_tasks)]
            function = functools.partial(heavy_function, payload=b"1" * size)

            with ScopedLogger(f"submit {len(tasks)} heavy function (500mb) for {number_of_tasks} tasks"):
                results = client.map(function, tasks)

            expected = [task * size for task in tasks]
            self.assertEqual(results, expected)

    def test_very_large_payload(self):
        def func(data: bytes):
            return data

        with Client(self.address) as client:
            payload = os.urandom(2**29 + 300)  # 512MB + 300B
            future = client.submit(func, payload)

            result = future.result()

            self.assertTrue(payload == result)

    def test_sleep(self):
        with Client(self.address) as client:
            time.sleep(5)

            tasks = [10, 1, 1] * 10
            # tasks = [10] * 10
            with ScopedLogger(f"submit {len(tasks)} sleep and balance tasks"):
                futures = [client.submit(noop_sleep, i) for i in tasks]

            # time.sleep(60)
            # print(f"number of futures: {len(futures)}")
            # print(f"number of states: {Counter([future._state for future in futures])}")
            with ScopedLogger(f"gather {len(futures)} results"):
                results = [future.result() for future in futures]

            self.assertEqual(results, tasks)

    def test_raise_exception(self):
        with Client(self.address) as client:
            tasks = [i for i in range(100)]
            with ScopedLogger(f"submit {len(tasks)} tasks, raise 1 of the tasks"):
                futures = [client.submit(raise_exception, i) for i in tasks]

            with self.assertRaises(ValueError), ScopedLogger(f"gather {len(futures)} results"):
                _ = [future.result() for future in futures]

    def test_function(self):
        def func_args(a: int, b: int, c: int, d: int = 0):
            return a, b, c, d

        def func_args2(a: int, b: int, *, c: int, d: int = 0):
            return a, b, c, d

        with Client(self.address) as client:
            with ScopedLogger("test mix of positional and keyword arguments and with some arguments default value"):
                self.assertEqual(client.submit(func_args, 1, c=4, b=2).result(), (1, 2, 4, 0))

            with ScopedLogger("test all keyword arguments"):
                self.assertEqual(client.submit(func_args, d=5, b=3, c=1, a=4).result(), (4, 3, 1, 5))

            with ScopedLogger("test mix of positional and keyword arguments with override default value"):
                self.assertEqual(client.submit(func_args, 1, c=4, b=2, d=6).result(), (1, 2, 4, 6))

            with ScopedLogger("test partial function"):
                self.assertEqual(client.submit(functools.partial(func_args, 5, 6), 1, 2).result(), (5, 6, 1, 2))

            with ScopedLogger("test insufficient arguments"), self.assertRaises(TypeError):
                client.submit(func_args, 1)

            with ScopedLogger("test not allow keyword only arguments even assigned"), self.assertRaises(TypeError):
                client.submit(func_args2, 1, c=4, b=2, d=6).result()

            with ScopedLogger("test not allow keyword only arguments"), self.assertRaises(TypeError):
                client.submit(func_args2, a=3, b=4).result()

    def test_map(self):
        def func(x, y):
            return x * y

        with Client(self.address) as client:
            result = client.starmap(func, [(1, 2), (3, 4), (5, 6), (7, 8)])
            self.assertEqual(result, [2, 12, 30, 56])

    def test_more_tasks(self):
        def func(a):
            time.sleep(random.randint(1, 10))
            return a * 2

        with Client(self.address) as client:
            client.map(func, range(self._workers * 2))

    def test_context_manager(self):
        with Client(self.address) as client:
            self.assertEqual(client.submit(noop, 1).result(), 1)

    def test_no_disconnect(self):
        with Client(self.address) as client:
            self.assertEqual(client.submit(noop, 1).result(), 1)

    def test_extra_disconnects(self):
        with Client(self.address) as client:
            self.assertEqual(client.submit(noop, 1).result(), 1)
            client.disconnect()
            client.disconnect()
            client.disconnect()

    def test_processor_died(self):
        def func():
            time.sleep(1)
            os._exit(1)  # noqa

        with Client(self.address) as client:
            with self.assertRaises(ProcessorDiedError):
                client.submit(func).result()

    def test_non_hashable_client(self):
        def func(a):
            return a * 2

        with Client(self.address) as client:
            client.submit(func, [1, 2, 3, 4, 5])

    def test_send_object(self):
        def func(a):
            return len(a)

        with Client(self.address) as client:
            ref1 = client.send_object("abcdef")
            self.assertEqual(client.submit(func, ref1).result(), 6)

            ref2 = client.send_object("123456789")
            self.assertEqual(client.map(func, [ref1, ref2]), [6, 9])

    def test_send_object2(self):
        def add(a, b):
            return a + b

        with Client(address=self.address) as client:
            ref = client.send_object([1, 2, 3, 4, 5], name="large_object")

            fut = client.submit(add, ref, [6])
            self.assertEqual(fut.result(), [1, 2, 3, 4, 5, 6])

    @unittest.skipIf(
        sys.platform == "win32",
        "hangs on Windows: client teardown blocks forever waiting on a killed peer's socket "
        "shutdown; see https://github.com/finos/opengris-scaler/issues/912",
    )
    def test_scheduler_crash(self):
        client_timeout_seconds = 5
        with Client(address=self.address, timeout_seconds=client_timeout_seconds) as client:
            future = client.submit(noop, 10)

            self.combo._scheduler.kill()

            time.sleep(5)

            with self.assertRaises((TimeoutError, DisconnectedError)):
                future.result()

    def test_responsiveness(self):
        MAX_DELAY_SECONDS = 0.3

        # Makes sure the cluster has the time to start up.
        with Client(self.address) as client:
            client.submit(pow, 1, 1).result()

        try:
            connect_start_time = time.time()
            client = Client(self.address)
            self.assertLess(time.time() - connect_start_time, MAX_DELAY_SECONDS)

            submit_start_time = time.time()
            future = client.submit(pow, 2, 3)
            self.assertLess(time.time() - submit_start_time, MAX_DELAY_SECONDS)

            result_start_time = time.time()
            self.assertEqual(future.result(), 8)
            self.assertLess(time.time() - result_start_time, MAX_DELAY_SECONDS)
        finally:
            disconnect_start_time = time.time()
            client.disconnect()
            self.assertLess(time.time() - disconnect_start_time, MAX_DELAY_SECONDS)

    def test_clear(self):
        with Client(self.address) as client:
            finished_future = client.submit(round, 3.14)
            finished_future.result()

            arg_reference = client.send_object(0.5)
            unfinished_future = client.submit(noop_sleep, arg_reference)

            client.clear()

            # clear() cancels all running futures
            self.assertFalse(finished_future.cancelled())
            with self.assertRaises(CancelledError):
                unfinished_future.result()
            self.assertTrue(unfinished_future.cancelled())

            # using an old reference should fail
            with self.assertRaises(MissingObjects):
                client.submit(noop_sleep, arg_reference).result()

            # but new tasks should work fine
            self.assertEqual(client.submit(round, 3.14).result(), 3.0)

    def test_client_no_address_outside_worker(self):
        """Test that creating a Client without an address outside worker context raises ValueError."""
        with self.assertRaises(ValueError) as context:
            Client()

        self.assertIn("No scheduler address provided", str(context.exception))
        self.assertIn("not running inside a worker context", str(context.exception))


class TestClientPreload(unittest.TestCase):
    # Separate class for preload functionality with separate cluster to avoid interfering with time-sensitive tests

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(n_workers=0, event_loop="builtin")

    def tearDown(self) -> None:
        self.combo.shutdown()

    def _create_preload_cluster(self, preload: str, logging_paths: tuple = ("/dev/stdout",)):
        base_config = self.combo._worker_manager.config
        base_worker_config = base_config.worker_config
        base_logging_config = base_config.logging_config
        preload_manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=self.combo._address,
                    worker_manager_id="test_manager",
                    object_storage_address=self.combo._object_storage_address,
                    max_task_concurrency=1,
                ),
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=base_worker_config.per_worker_task_queue_size,
                    heartbeat_interval_seconds=base_worker_config.heartbeat_interval_seconds,
                    task_timeout_seconds=base_worker_config.task_timeout_seconds,
                    death_timeout_seconds=base_worker_config.death_timeout_seconds,
                    garbage_collect_interval_seconds=base_worker_config.garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=base_worker_config.trim_memory_threshold_bytes,
                    hard_processor_suspend=base_worker_config.hard_processor_suspend,
                    io_threads=base_worker_config.io_threads,
                    event_loop=base_worker_config.event_loop,
                    preload=preload,
                ),
                logging_config=LoggingConfig(
                    paths=logging_paths, level=base_logging_config.level, config_file=base_logging_config.config_file
                ),
            )
        )
        return multiprocessing.get_context("spawn").Process(target=preload_manager.run)

    def test_preload_success(self):
        preload_process = self._create_preload_cluster(
            preload="tests.utility.utility:setup_global_value('test_preload_value')"
        )

        try:
            preload_process.start()
            time.sleep(2)

            with Client(self.combo.get_address()) as client:
                # Submit a task that should access the preloaded global value
                future = client.submit(get_preloaded_value)
                result = future.result()

                # Verify the preloaded value is accessible
                self.assertEqual(result, "test_preload_value")
        finally:
            preload_process.terminate()
            preload_process.join()

    def test_preload_failure(self):
        # For checking if the failure was logged, Processor will create log_path-{pid}
        log_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".log")
        log_path = log_file.name
        log_dir = os.path.dirname(log_path)
        log_basename = os.path.basename(log_path)

        try:
            preload_process = self._create_preload_cluster(
                preload="tests.utility.utility:failing_preload()", logging_paths=(log_path,)
            )

            try:
                preload_process.start()
                time.sleep(10)

                # Find processor log files by looking for files with PID suffixes
                processor_log_content = ""
                for file in os.listdir(log_dir):
                    if file.startswith(log_basename + "-") and file != log_basename:
                        processor_log_path = os.path.join(log_dir, file)
                        with open(processor_log_path, "r") as f:
                            processor_log_content += f.read()

                # Verify that the preload failure was logged properly
                self.assertIn("preloading: tests.utility.utility:failing_preload with args", processor_log_content)

                # If we reach here without any other exceptions, the test is successful
            finally:
                preload_process.terminate()
                preload_process.join()
        finally:
            # Clean up log files. On Windows the logging handlers may still hold an open
            # handle on the log files even after preload_process.join(), which makes
            # os.unlink raise PermissionError (WinError 32, sharing violation). Skip the
            # cleanup in that case; OS will reap the temp files later.
            try:
                os.unlink(log_path)
                for file in os.listdir(log_dir):
                    if file.startswith(log_basename + "-") and file != log_basename:
                        os.unlink(os.path.join(log_dir, file))
            except OSError as exc:
                logging.warning(f"could not clean up preload log files: {exc}")

    def test_parse_preload_spec_error(self):
        # Test that _parse_preload_spec raises PreloadSpecError for invalid specs
        with self.assertRaises(PreloadSpecError) as cm:
            _parse_preload_spec("module_without_colon")

        self.assertIn("preload must be in 'module.sub:func(...)' format", str(cm.exception))

    def test_execute_preload_error(self):
        # Test that execute_preload raises PreloadSpecError for non-callable targets
        with self.assertRaises(PreloadSpecError) as cm:
            execute_preload("sys:version")  # sys.version is a string, not callable

        self.assertIn("Preload target must be callable", str(cm.exception))
