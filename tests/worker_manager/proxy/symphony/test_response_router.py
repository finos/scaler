"""Covers what a Symphony response turns into on the future the worker manager is waiting on.

The failure a client finally sees is decided here, and the client has no Symphony installation: a
``soamapi`` exception object put on a future arrives there as ``ModuleNotFoundError: No module named
'soamapi'``, hiding the real failure. These tests pin that nothing ``soamapi``-typed escapes, and that a
task whose function raised keeps its own exception.

The fakes stand in for ``soamapi`` types, which exist only on a host with Symphony, so the factories
below hand them out as ``Any``: the annotations they satisfy cannot be written down here.
"""

import concurrent.futures
import pickle
import unittest
from typing import Any, Optional, Tuple

import cloudpickle

from scaler.utility.exceptions import SymphonyTaskError, TaskExceptionNotSerializableError
from scaler.worker_manager.proxy.symphony.response_router import TaskResponseRouter, describe_soam_exception
from scaler.worker_manager.proxy.symphony.task_output import TaskOutputTag

TASK_ID = "task-1"


class FakeSoamException(Exception):
    def __init__(self, description: str, embedded: Optional[BaseException] = None) -> None:
        super().__init__(description)
        self._embedded = embedded

    def get_embedded_exception(self) -> Optional[BaseException]:
        return self._embedded


class FakeMessage:
    def __init__(self) -> None:
        self._payload: bytes = b""

    def set_payload(self, payload: bytes) -> None:
        self._payload = payload

    def get_payload(self) -> bytes:
        return self._payload


class FakeTaskOutputHandle:
    def __init__(self, successful: bool, payload: bytes, exception: Optional[FakeSoamException]) -> None:
        self._successful = successful
        self._payload = payload
        self._exception = exception

    def get_id(self) -> str:
        return TASK_ID

    def is_successful(self) -> bool:
        return self._successful

    def populate_task_output(self, message: FakeMessage) -> None:
        message.set_payload(self._payload)

    def get_exception(self) -> Optional[FakeSoamException]:
        return self._exception


def soam_exception(description: str, embedded: Optional[BaseException] = None) -> Any:
    return FakeSoamException(description, embedded)


def message_factory() -> Any:
    return FakeMessage()


def completed_task(payload: bytes) -> Any:
    return FakeTaskOutputHandle(True, payload, None)


def failed_task(exception: Any) -> Any:
    return FakeTaskOutputHandle(False, b"", exception)


def router_with_pending_task() -> Tuple[TaskResponseRouter, concurrent.futures.Future]:
    router = TaskResponseRouter(message_factory)
    future: concurrent.futures.Future = concurrent.futures.Future()
    future.set_running_or_notify_cancel()
    router.submit_task(TASK_ID, future)
    return router, future


class OnResponseTest(unittest.TestCase):
    def test_a_returned_value_completes_the_future(self) -> None:
        router, future = router_with_pending_task()

        router.on_response(completed_task(cloudpickle.dumps((TaskOutputTag.RESULT.value, 49))))

        self.assertEqual(future.result(), 49)

    def test_the_exception_a_function_raised_reaches_the_future(self) -> None:
        """The client wants the exception its task raised, not a report that Symphony saw a failure."""
        router, future = router_with_pending_task()

        router.on_response(completed_task(cloudpickle.dumps((TaskOutputTag.EXCEPTION.value, ValueError("deliberate")))))

        with self.assertRaises(ValueError) as caught:
            future.result()
        self.assertEqual(str(caught.exception), "deliberate")

    def test_an_exception_the_service_could_not_pickle_keeps_its_description(self) -> None:
        router, future = router_with_pending_task()
        detail = "RuntimeError: holds a lock"

        router.on_response(completed_task(cloudpickle.dumps((TaskOutputTag.UNSERIALIZABLE_EXCEPTION.value, detail))))

        with self.assertRaises(TaskExceptionNotSerializableError) as caught:
            future.result()
        self.assertIn(detail, str(caught.exception))

    def test_a_symphony_failure_becomes_a_picklable_error(self) -> None:
        """A soamapi exception on the future would reach the client as an import error instead."""
        router, future = router_with_pending_task()
        embedded = soam_exception("service exploded")

        router.on_response(failed_task(soam_exception("task 1 failed", embedded=embedded)))

        with self.assertRaises(SymphonyTaskError) as caught:
            future.result()
        self.assertIn("service exploded", str(caught.exception))
        pickle.loads(pickle.dumps(caught.exception))

    def test_a_symphony_failure_with_nothing_embedded_still_reports(self) -> None:
        """get_embedded_exception returns None when Symphony failed the task rather than the service."""
        router, future = router_with_pending_task()

        router.on_response(failed_task(soam_exception("host blocked")))

        with self.assertRaises(SymphonyTaskError) as caught:
            future.result()
        self.assertIn("host blocked", str(caught.exception))

    def test_an_output_from_a_service_that_predates_the_tags_asks_for_a_redeploy(self) -> None:
        router, future = router_with_pending_task()

        router.on_response(completed_task(cloudpickle.dumps(49)))

        with self.assertRaises(SymphonyTaskError) as caught:
            future.result()
        self.assertIn("setup_application.py", str(caught.exception))

    def test_an_unreadable_output_names_the_task(self) -> None:
        router, future = router_with_pending_task()

        router.on_response(completed_task(b"not a pickle"))

        with self.assertRaises(SymphonyTaskError) as caught:
            future.result()
        self.assertIn(TASK_ID, str(caught.exception))


class OnExceptionTest(unittest.TestCase):
    def test_a_session_failure_fails_every_pending_task_picklably(self) -> None:
        router, future = router_with_pending_task()

        router.on_exception(soam_exception("connection broken"))

        with self.assertRaises(SymphonyTaskError) as caught:
            future.result()
        self.assertIn("connection broken", str(caught.exception))
        pickle.loads(pickle.dumps(caught.exception))


class DescribeSoamExceptionTest(unittest.TestCase):
    def test_the_embedded_exception_is_rendered_as_text(self) -> None:
        description = describe_soam_exception(soam_exception("failed", embedded=ValueError("bad input")))

        self.assertEqual(description, "failed: ValueError: bad input")

    def test_a_missing_exception_still_yields_a_description(self) -> None:
        self.assertTrue(describe_soam_exception(None))


if __name__ == "__main__":
    unittest.main()
