import concurrent.futures
import threading
import time
import unittest

import psutil

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name


class TestDeadWorker(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_scheduler_responsive_after_worker_death(self):
        """
        Regression test: the scheduler must not block because of a dead worker.

        Sending to a disconnected (or disconnecting) peer used to block the scheduler's event loop indefinitely, which
        stopped it from processing any other request.

        Clients could then no longer submit tasks and eventually timed out.
        """

        # The scheduler must keep believing the killed worker is alive, so that it still routes tasks to it.
        WORKER_TIMEOUT_SECONDS = 5

        # One worker is killed, the other will remain able to run the new client's tasks.
        N_WORKERS = 2

        N_TASKS = 10

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address, n_workers=N_WORKERS, worker_timeout_seconds=WORKER_TIMEOUT_SECONDS
        )
        self.addCleanup(combo.shutdown)

        with Client(address=address) as client:
            # Ensures the workers are connected and registered by the scheduler.
            self.assertEqual(client.submit(round, 3.14).result(), 3)

            worker_processes = self.__wait_for_worker_processes(combo._worker_manager_process.pid, N_WORKERS)

            # Kill the first worker
            self._kill_process_tree(worker_processes[0])

            # The scheduler still considers the killed worker alive (see WORKER_TIMEOUT_SECONDS) and keeps routing it a
            # share of these tasks.
            for _ in range(N_TASKS):
                client.submit(round, 3.14)

            time.sleep(1.0)  # let the scheduler process the submitted tasks

            self.__assert_new_client_can_run_task(address)

    def __wait_for_worker_processes(self, worker_manager_pid: int, expected_count: int) -> list[psutil.Process]:
        """Waits until the worker manager has spawned expected_count worker processes, and returns them."""

        WORKER_SPAWN_TIMEOUT_SECONDS = 30.0

        deadline = time.time() + WORKER_SPAWN_TIMEOUT_SECONDS
        worker_processes: list[psutil.Process] = []

        while len(worker_processes) < expected_count:
            if time.time() > deadline:
                self.fail(f"only {len(worker_processes)} of {expected_count} workers started in time")

            worker_processes = psutil.Process(worker_manager_pid).children()
            time.sleep(0.1)

        return worker_processes

    def __assert_new_client_can_run_task(self, address: str) -> None:
        """
        Fails if a new client cannot connect to the scheduler, submit a task and get its result within
        CLIENT_TIMEOUT_SECONDS.
        """

        CLIENT_TIMEOUT_SECONDS = 2.0

        outcome: concurrent.futures.Future = concurrent.futures.Future()

        def connect_and_run_task() -> None:
            try:
                with Client(address=address) as new_client:
                    outcome.set_result(new_client.submit(round, 3.14).result())
            except BaseException as exception:
                outcome.set_exception(exception)

        # A daemon thread, as connecting blocks forever if the scheduler is unresponsive.
        threading.Thread(target=connect_and_run_task, daemon=True).start()

        try:
            self.assertEqual(outcome.result(timeout=CLIENT_TIMEOUT_SECONDS), 3)
        except concurrent.futures.TimeoutError:
            self.fail("a new client could not run a task: the scheduler is stuck sending to the dead worker")

    @staticmethod
    def _kill_process_tree(process: psutil.Process) -> None:
        """Abruptly kills a process and all of its descendants with SIGKILL, so that none of them can clean up."""

        try:
            members = [process, *process.children(recursive=True)]
        except psutil.NoSuchProcess:
            return

        for member in members:
            try:
                member.kill()
            except psutil.NoSuchProcess:
                pass

        psutil.wait_procs(members)
