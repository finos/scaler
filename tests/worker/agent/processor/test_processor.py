import logging
import unittest
from typing import Any, Optional
from unittest import mock
from unittest.mock import Mock

import scaler.worker.agent.processor.processor as processor_module
from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.ymq import SocketStopRequestedError
from scaler.protocol.capnp import Task, TaskResultType
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.worker.agent.processor.processor import Processor

_LOGGER_NAME = "scaler.worker.agent.processor.processor"


def _make_processor() -> Processor:
    # The processor is never started: these tests exercise the pure logic of __log_exit and __send_result with
    # the connectors mocked out, so nothing here needs a live child process.
    address = AddressConfig.from_string("tcp://127.0.0.1:0")
    return Processor(
        event_loop="builtin",
        agent_address=address,
        scheduler_address=address,
        object_storage_address=address,
        preload=None,
        resume_event=None,
        resumed_event=None,
        suspend_trigger=None,
        garbage_collect_interval_seconds=60,
        trim_memory_threshold_bytes=10**9,
        logging_paths=("/dev/stdout",),
        logging_level="INFO",
    )


def _make_task() -> Task:
    return Task(
        taskId=TaskID.generate_task_id(),
        source=ClientID.generate_client_id("test"),
        metadata=b"",
        funcObjectId=ObjectID(b"\x00" * 16 + b"\x01" * 16),
        functionArgs=[],
    )


class ProcessorLogExitTest(unittest.TestCase):
    """__log_exit is the processor's only report of why its main loop stopped, so it must name the orphaned task,
    keep the traceback for genuine faults, and stay quiet for a teardown the agent itself requested."""

    def setUp(self) -> None:
        self.processor = _make_processor()

    def __log_exit(self, reason: str, exception: Optional[BaseException] = None) -> logging.LogRecord:
        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG) as captured:
            self.processor._Processor__log_exit(reason, exception=exception)

        self.assertEqual(len(captured.records), 1)
        return captured.records[0]

    def test_idle_exit_is_debug_without_traceback(self) -> None:
        record = self.__log_exit("agent connector stop requested")

        self.assertEqual(record.levelno, logging.DEBUG)
        self.assertIsNone(record.exc_info)
        self.assertIn("agent connector stop requested", record.getMessage())

    def test_exception_is_error_with_traceback(self) -> None:
        exception = ObjectStorageException("storage went away")

        record = self.__log_exit("object storage error", exception=exception)

        self.assertEqual(record.levelno, logging.ERROR)
        self.assertIsNotNone(record.exc_info)
        self.assertIs(record.exc_info[1], exception)

    def test_in_flight_task_is_named_when_an_exception_is_present(self) -> None:
        # A storage failure mid-result has to name the task it orphaned, which needs _current_task still set.
        task = _make_task()
        self.processor._current_task = task

        record = self.__log_exit("object storage error", exception=ObjectStorageException("boom"))

        message = record.getMessage()
        self.assertEqual(record.levelno, logging.ERROR)
        self.assertIn(task.taskId.hex(), message)
        self.assertIn("no task result will be sent", message)

    def test_in_flight_task_without_exception_is_warning(self) -> None:
        task = _make_task()
        self.processor._current_task = task

        record = self.__log_exit("interrupted")

        message = record.getMessage()
        self.assertEqual(record.levelno, logging.WARNING)
        self.assertIsNone(record.exc_info)
        self.assertIn(task.taskId.hex(), message)
        self.assertIn("no task result will be sent", message)

    def test_interrupted_exit_is_quiet_even_with_an_exception(self) -> None:
        # A deliberate kill sends SIGTERM, whose handler destroys the connectors on purpose; a processor mid
        # storage call then raises "connector is closed." That is expected teardown, not an error, so it must
        # not be reported as ERROR with a traceback for something the agent asked for.
        self.processor._current_task = _make_task()
        self.processor._interrupted = True

        record = self.__log_exit("object storage error", exception=ObjectStorageException("closed"))

        self.assertEqual(record.levelno, logging.DEBUG)
        self.assertIsNone(record.exc_info)
        self.assertIn("stopped on agent request", record.getMessage())

    def test_interrupt_sets_the_interrupted_flag(self) -> None:
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()

        self.assertFalse(self.processor._interrupted)

        self.processor._Processor__interrupt()

        self.assertTrue(self.processor._interrupted)
        self.processor._connector_agent.destroy.assert_called_once()
        self.processor._connector_storage.destroy.assert_called_once()


class ProcessorSendResultTest(unittest.TestCase):
    """_current_task must stay set until the result is fully handed off, so a failure part-way through can still
    report which task was orphaned."""

    def setUp(self) -> None:
        self.processor = _make_processor()
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()

        self.task = _make_task()
        self.processor._current_task = self.task

        # a failing hand-off is retried with a backoff; the retries themselves are covered below
        patcher = mock.patch.object(processor_module.time, "sleep")
        patcher.start()
        self.addCleanup(patcher.stop)

    def __send_result(self) -> None:
        self.processor._Processor__send_result(
            self.task.source, self.task.taskId, TaskResultType.success, b"result-bytes"
        )

    def test_current_task_is_cleared_once_the_result_is_sent(self) -> None:
        self.__send_result()

        self.assertIsNone(self.processor._current_task)

    def test_current_task_survives_a_storage_failure(self) -> None:
        storage: Any = self.processor._connector_storage
        storage.set_object.side_effect = ObjectStorageException("storage went away")

        with self.assertRaises(ObjectStorageException):
            self.__send_result()

        self.assertIs(self.processor._current_task, self.task)


class ProcessorResultHandOffRetryTest(unittest.TestCase):
    """A finished task's result is unreproducible work, so a transport that drops under the hand-off must be
    retried instead of unwinding the main loop, which exits the processor and loses the result."""

    def setUp(self) -> None:
        self.processor = _make_processor()
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()

        self.task = _make_task()
        self.processor._current_task = self.task

        # the retries sleep between attempts; nothing here depends on wall-clock time
        patcher = mock.patch.object(processor_module.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def __send_result(self) -> None:
        self.processor._Processor__send_result(
            self.task.source, self.task.taskId, TaskResultType.success, b"result-bytes"
        )

    @staticmethod
    def __canceled_send() -> SocketStopRequestedError:
        return SocketStopRequestedError(ymq.ErrorCode.SocketStopRequested, "connection aborted mid-write")

    def test_a_dropped_storage_write_is_retried_until_it_lands(self) -> None:
        storage: Any = self.processor._connector_storage
        storage.set_object.side_effect = [ObjectStorageException("connection failure"), None]

        self.__send_result()

        self.assertEqual(storage.set_object.call_count, 2)
        self.assertIsNone(self.processor._current_task, "the result was never handed off")

    def test_the_announced_object_id_is_the_one_that_landed(self) -> None:
        # A retry can follow a request the server only received part of, so every attempt writes under a fresh
        # object ID. The ID handed to the agent must be the one whose write succeeded.
        storage: Any = self.processor._connector_storage
        agent: Any = self.processor._connector_agent
        storage.set_object.side_effect = [ObjectStorageException("connection failure"), None]

        self.__send_result()

        first_id, second_id = (call.args[0] for call in storage.set_object.call_args_list)
        self.assertNotEqual(first_id, second_id)

        task_result = agent.send.call_args_list[-1].args[0]
        self.assertEqual(task_result.results, [bytes(second_id)])

    def test_a_canceled_send_to_the_agent_is_retried(self) -> None:
        agent: Any = self.processor._connector_agent
        agent.send.side_effect = [self.__canceled_send(), None, None]

        self.__send_result()

        self.assertEqual(agent.send.call_count, 3, "the object instruction was not resent, or the result never was")
        self.assertIsNone(self.processor._current_task)

    def test_retries_are_given_up_on_and_the_task_stays_in_flight(self) -> None:
        storage: Any = self.processor._connector_storage
        storage.set_object.side_effect = ObjectStorageException("storage went away")

        with self.assertRaises(ObjectStorageException):
            self.__send_result()

        self.assertEqual(storage.set_object.call_count, processor_module.RESULT_HAND_OFF_MAX_ATTEMPTS)
        self.assertIs(self.processor._current_task, self.task, "the orphaned task must still be reportable")

    def test_a_teardown_the_agent_asked_for_is_not_retried(self) -> None:
        # __interrupt destroys the connectors on purpose, so every further attempt would fail the same way;
        # retrying would only delay a shutdown the agent already asked for.
        self.processor._interrupted = True
        storage: Any = self.processor._connector_storage
        storage.set_object.side_effect = ObjectStorageException("connector is closed.")

        with self.assertRaises(ObjectStorageException):
            self.__send_result()

        self.assertEqual(storage.set_object.call_count, 1)
        self.sleep.assert_not_called()

    def test_backoff_grows_between_attempts(self) -> None:
        storage: Any = self.processor._connector_storage
        storage.set_object.side_effect = [self.__canceled_send(), self.__canceled_send(), None]

        self.__send_result()

        delays = [call.args[0] for call in self.sleep.call_args_list]
        self.assertEqual(
            delays,
            [
                processor_module.RESULT_HAND_OFF_RETRY_DELAY_SECONDS,
                processor_module.RESULT_HAND_OFF_RETRY_DELAY_SECONDS * 2,
            ],
        )


class ProcessorRunForeverTest(unittest.TestCase):
    """SystemExit must reach multiprocessing's _bootstrap, otherwise the processor exits 0 and the exit code
    the task asked for is lost."""

    def setUp(self) -> None:
        self.processor = _make_processor()
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()
        self.processor._object_cache = Mock()

    def test_system_exit_is_logged_and_re_raised(self) -> None:
        # A task calling sys.exit(3) raises SystemExit, which __process_task's `except Exception` does not
        # catch, so it unwinds into __run_forever.
        agent: Any = self.processor._connector_agent
        agent.receive.side_effect = SystemExit(3)

        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG):
            with self.assertRaises(SystemExit) as context:
                self.processor._Processor__run_forever()

        self.assertEqual(context.exception.code, 3)

    def test_storage_error_does_not_escape_the_loop(self) -> None:
        agent: Any = self.processor._connector_agent
        agent.receive.side_effect = ObjectStorageException("storage went away")

        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG) as captured:
            self.processor._Processor__run_forever()

        self.assertEqual(captured.records[0].levelno, logging.ERROR)


if __name__ == "__main__":
    unittest.main()
