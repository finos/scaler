"""Regression tests for the worker's top-level lifecycle handling (``Worker._run``).

The ``jne-fix-ymq`` failure: a worker's internal YMQ binder is shut down (``disconnect``/teardown)
while a ``binder.send`` driven by one of the worker's loops is still in flight. The send surfaces
``SocketStopRequested`` (see ``tests/io/test_ymq_async_binder.py``), which propagates through
``asyncio.gather`` into ``Worker._run``. Before the fix the handler logged it as a "failed
with unhandled exception" crash; it must instead be treated like ``ConnectorSocketClosedByRemoteEnd``
- an expected teardown condition that is logged, never surfaced as a crash, and does not produce a
nonzero exit code.
"""

import asyncio
import unittest
from typing import Any, Awaitable, Callable
from unittest import mock

import scaler.worker.worker as worker_module
from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, SocketStopRequestedError, SysCallError
from scaler.protocol.capnp import WorkerDisconnectNotification
from scaler.worker.worker import Worker


async def _hang() -> None:
    await asyncio.Event().wait()


class _StubCollaborator:
    """Minimal stand-in for a worker collaborator, exposing the hooks ``__main_loop`` touches."""

    def __init__(self, routine_behavior: Callable[[], Awaitable[None]] = _hang) -> None:
        self._routine_behavior = routine_behavior
        self.destroyed = False
        self.sent_messages: list = []

    async def routine(self) -> None:
        await self._routine_behavior()

    async def connect(self, *args: object, **kwargs: object) -> None:
        return None

    async def bind(self, *args: object, **kwargs: object) -> None:
        return None

    async def send(self, message: object, *args: object, **kwargs: object) -> None:
        self.sent_messages.append(message)

    async def initialize(self, *args: object, **kwargs: object) -> None:
        await _hang()

    def destroy(self, *args: object) -> None:
        self.destroyed = True


class WorkerTeardownYMQErrorTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _build_worker(task_routine_error: ymq.YMQException) -> Any:
        # Typed as Any: the test deliberately injects duck-typed stubs into the worker's typed
        # collaborator slots and reaches a name-mangled private method, which the type system rejects.
        worker: Any = Worker(
            event_loop="builtin",
            name="test-worker",
            address=AddressConfig.from_string("tcp://127.0.0.1:2345"),
            object_storage_address=None,
            preload=None,
            capabilities={},
            io_threads=1,
            task_queue_size=10,
            heartbeat_interval_seconds=1,
            garbage_collect_interval_seconds=1,
            trim_memory_threshold_bytes=0,
            task_timeout_seconds=10,
            death_timeout_seconds=10,
            hard_processor_suspend=False,
            logging_paths=(),
            logging_level="INFO",
            worker_manager_id=b"wm",
        )

        worker._backend = None  # only used as a collaborator factory, which __initialize skips below
        worker._address_internal = AddressConfig.from_string("tcp://127.0.0.1:2346")  # tcp -> no ipc unlink

        # __initialize would overwrite the stubs below with real backend collaborators; skip it and
        # go straight to __main_loop with the worker already wired up.
        worker._Worker__initialize = lambda: None

        async def _raise() -> None:
            raise task_routine_error

        worker._connector_external = _StubCollaborator()
        worker._connector_storage = _StubCollaborator()
        worker._binder_internal = _StubCollaborator()
        worker._heartbeat_manager = _StubCollaborator()
        worker._timeout_manager = _StubCollaborator()
        worker._profiling_manager = _StubCollaborator()
        worker._processor_manager = _StubCollaborator()
        # The task loop is what drives processor_manager.on_task -> binder_internal.send; model that
        # send raising the YMQ error by having the task routine raise it directly into the gather.
        worker._task_manager = _StubCollaborator(_raise)

        return worker

    async def _drain_pending_tasks(self) -> None:
        # The sibling loop coroutines created by __main_loop's gather keep running after the gather
        # propagates the first exception; cancel them so the test loop tears down cleanly.
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _run_worker(self, task_routine_error: ymq.YMQException) -> tuple[mock.MagicMock, int]:
        worker = self._build_worker(task_routine_error)
        worker._loop = asyncio.get_running_loop()
        with mock.patch.object(worker_module, "logger") as mock_logger:
            exit_code = await worker._run()
        await self._drain_pending_tasks()

        # Regardless of the error, the worker must always reach clean teardown.
        self.assertTrue(worker._binder_internal.destroyed, "worker did not reach teardown / destroy")
        return mock_logger, exit_code

    async def test_socket_stop_requested_during_teardown_is_logged_not_crashed(self) -> None:
        error = SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "binder socket shut down mid-send")
        mock_logger, exit_code = await self._run_worker(error)

        unhandled = [c for c in mock_logger.exception.call_args_list if "failed with unhandled exception" in str(c)]
        self.assertEqual(unhandled, [], "SocketStopRequested surfaced as an unhandled-exception crash")

        handled = [c for c in mock_logger.info.call_args_list if "shut down during teardown" in str(c)]
        self.assertTrue(handled, "SocketStopRequested was not logged as an expected teardown condition")

        self.assertEqual(exit_code, 0, "an expected teardown condition should not produce a nonzero exit code")

    async def test_unexpected_ymq_error_still_surfaces_as_unhandled(self) -> None:
        # A YMQ error that is NOT an expected teardown condition must still be logged loudly, so real
        # bugs continue to fail fast during development rather than being blanket-swallowed, and must
        # produce a nonzero exit code so process-level monitoring can tell it apart from a clean exit.
        error = SysCallError(ymq.ErrorCode.SysCallError, "something genuinely broke")
        mock_logger, exit_code = await self._run_worker(error)

        unhandled = [c for c in mock_logger.exception.call_args_list if "failed with unhandled exception" in str(c)]
        self.assertTrue(unhandled, "an unexpected YMQ error should still be logged as an unhandled exception")

        self.assertEqual(exit_code, 1, "an unexpected error should produce a nonzero exit code")

    async def test_connector_closed_before_ever_connecting_is_nonzero_exit(self) -> None:
        # ConnectorSocketClosedByRemoteEnd is also what the connect retries exhausting looks like:
        # if we never heard from the scheduler, that is an unreachable dependency, not a clean
        # teardown, and must produce a nonzero exit code.
        error = ConnectorSocketClosedByRemoteEndError(
            ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd, "retries exhausted"
        )
        worker = self._build_worker(error)
        worker._loop = asyncio.get_running_loop()
        self.assertFalse(worker._connected_to_scheduler)

        with mock.patch.object(worker_module, "logger") as mock_logger:
            exit_code = await worker._run()
        await self._drain_pending_tasks()

        self.assertEqual(exit_code, 1, "never having connected to the scheduler should produce a nonzero exit code")
        never_connected = [c for c in mock_logger.warning.call_args_list if "never connected to scheduler" in str(c)]
        self.assertTrue(never_connected, "the never-connected case was not logged distinctly")

    async def test_connector_closed_after_connecting_is_clean_exit(self) -> None:
        # The same error after we have successfully talked to the scheduler at least once (e.g. a
        # scale-down disconnect) is an expected teardown condition, not an anomaly.
        error = ConnectorSocketClosedByRemoteEndError(
            ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd, "remote end closed the socket"
        )
        worker = self._build_worker(error)
        worker._loop = asyncio.get_running_loop()
        worker._connected_to_scheduler = True

        with mock.patch.object(worker_module, "logger") as mock_logger:
            exit_code = await worker._run()
        await self._drain_pending_tasks()

        self.assertEqual(exit_code, 0, "a post-connection disconnect should not produce a nonzero exit code")
        handled = [c for c in mock_logger.info.call_args_list if "connector socket closed by remote end" in str(c)]
        self.assertTrue(handled, "the post-connection case was not logged as an expected condition")

    async def test_teardown_failure_escalates_a_clean_exit_to_nonzero(self) -> None:
        # A teardown failure must not swallow the "quit" log line or silently produce a clean exit;
        # since nothing else went wrong, it's the only reason to exit nonzero.
        error = SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "binder socket shut down mid-send")
        worker = self._build_worker(error)
        worker._loop = asyncio.get_running_loop()
        worker._binder_internal.destroy = mock.Mock(side_effect=RuntimeError("teardown boom"))

        with mock.patch.object(worker_module, "logger") as mock_logger:
            exit_code = await worker._run()
        await self._drain_pending_tasks()

        self.assertEqual(exit_code, 1, "a teardown failure should escalate an otherwise-clean exit to nonzero")
        teardown_failed = [c for c in mock_logger.exception.call_args_list if "teardown failed" in str(c)]
        self.assertTrue(teardown_failed, "the teardown failure was not logged")
        quit_logged = [c for c in mock_logger.info.call_args_list if "quit" in str(c)]
        self.assertTrue(quit_logged, "the 'quit' log line was lost when teardown failed")

    async def test_teardown_failure_does_not_mask_a_more_specific_nonzero_exit(self) -> None:
        error = SysCallError(ymq.ErrorCode.SysCallError, "something genuinely broke")
        worker = self._build_worker(error)
        worker._loop = asyncio.get_running_loop()
        worker._binder_internal.destroy = mock.Mock(side_effect=RuntimeError("teardown boom"))

        with mock.patch.object(worker_module, "logger") as mock_logger:
            exit_code = await worker._run()
        await self._drain_pending_tasks()

        self.assertEqual(exit_code, 1, "the original nonzero exit code should be preserved")
        quit_logged = [c for c in mock_logger.info.call_args_list if "quit" in str(c)]
        self.assertTrue(quit_logged, "the 'quit' log line was lost when teardown failed")

    async def test_worker_disconnect_notification_sent_during_teardown(self) -> None:
        # A worker that stops tells the scheduler on the way out, so its tasks are re-dispatched
        # immediately instead of waiting for the heartbeat timeout to expire. The notification is
        # one-way: the scheduler never replies, so teardown does not wait for an acknowledgement.
        # Teardown runs on every exit path and for every backend, so this needs no backend set up.
        error = SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "binder socket shut down mid-send")
        worker = self._build_worker(error)
        worker._loop = asyncio.get_running_loop()

        await worker._Worker__teardown()  # name-mangled private method

        sent = worker._connector_external.sent_messages
        self.assertEqual(len(sent), 1, "WorkerDisconnectNotification was not sent during teardown")
        self.assertIsInstance(sent[0], WorkerDisconnectNotification)

    async def test_teardown_completes_when_the_notification_send_hangs(self) -> None:
        # The notification only saves the scheduler from waiting out the heartbeat timeout, so a
        # connection that is wedged rather than closed must not keep the worker alive forever.
        error = SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "binder socket shut down mid-send")
        worker = self._build_worker(error)
        worker._loop = asyncio.get_running_loop()
        worker._connector_external.send = lambda *_args, **_kwargs: _hang()

        with mock.patch.object(worker_module, "WORKER_EXIT_NOTIFICATION_TIMEOUT_SECONDS", 0.05):
            with mock.patch.object(worker_module, "logger") as mock_logger:
                await asyncio.wait_for(worker._Worker__teardown(), timeout=5)

        self.assertTrue(worker._binder_internal.destroyed, "teardown stopped at the hung notification")
        timed_out = [c for c in mock_logger.warning.call_args_list if "quitting anyway" in str(c)]
        self.assertTrue(timed_out, "a notification that never sent was not reported")


if __name__ == "__main__":
    unittest.main()
