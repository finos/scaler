import os
import pathlib
import shutil
import signal
import sys
import tempfile
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.defaults import DEFAULT_HEARTBEAT_INTERVAL_SECONDS
from scaler.utility.exceptions import ProcessorDiedError
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name

RESULT_TIMEOUT_SECONDS = 60


def parent_task(client: Client, parent_pid_path: str, child_started_path: str, child_duration_seconds: float) -> str:
    """Reports its pid, then submits a nested task, which suspends this processor."""
    pathlib.Path(parent_pid_path).write_text(str(os.getpid()))

    return client.submit(child_task, child_started_path, child_duration_seconds).result()


def child_task(child_started_path: str, duration_seconds: float) -> str:
    """Runs while the parent is suspended; only starts once the parent has been suspended."""
    pathlib.Path(child_started_path).touch()
    time.sleep(duration_seconds)

    return "child done"


def square(value: int) -> int:
    return value**2


@unittest.skipIf(sys.platform == "win32", "sends SIGKILL to a suspended processor")
class TestDyingSuspendedProcessor(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=1, event_loop="builtin")
        self.directory = tempfile.mkdtemp(prefix="scaler_suspended_processor_")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_dying_suspended_processor(self) -> None:
        """Validates a processor can die while it is suspended.

        See https://github.com/finos/opengris-scaler/issues/921
        """

        # Must keep the parent suspended until the worker notices it died, which happens on the next heartbeat.
        CHILD_DURATION_SECONDS = 2 * DEFAULT_HEARTBEAT_INTERVAL_SECONDS

        parent_pid_path = os.path.join(self.directory, "parent_pid")
        child_started_path = os.path.join(self.directory, "child_started")

        with Client(self.address) as client:
            future = client.submit(parent_task, client, parent_pid_path, child_started_path, CHILD_DURATION_SECONDS)

            # The child only starts once the parent has been suspended to make room for it.
            self.__wait_for_file(parent_pid_path, "the parent task to report its pid")
            self.__wait_for_file(child_started_path, "the nested task to start")
            parent_pid = int(pathlib.Path(parent_pid_path).read_text())

            # Kill the suspended process
            os.kill(parent_pid, signal.SIGKILL)  # type: ignore[attr-defined, unused-ignore]

            with self.assertRaises(ProcessorDiedError):
                future.result(timeout=RESULT_TIMEOUT_SECONDS)

            # The worker owning the killed processor must still be able to run tasks.
            self.assertEqual(client.submit(square, 6).result(timeout=RESULT_TIMEOUT_SECONDS), 36)

    def __wait_for_file(self, path: str, description: str) -> None:
        POLL_INTERVAL_SECONDS = 0.05

        deadline = time.monotonic() + RESULT_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if os.path.exists(path):
                return
            time.sleep(POLL_INTERVAL_SECONDS)

        raise AssertionError(f"timed out waiting for {description}")


if __name__ == "__main__":
    unittest.main()
