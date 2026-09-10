"""Routing of Symphony task responses back to the futures that are waiting on them.

Holds no ``soamapi`` values, only annotations, so it is importable and unit testable without a Symphony
installation. Keeping it out of ``callback`` lets ``_soam.session_callback`` depend on it without the two
modules importing each other.

Nothing ``soamapi``-typed may reach a future set here. The failure travels on to a client that has no
Symphony installation, where unpickling a ``soamapi`` exception raises ``ModuleNotFoundError: No module
named 'soamapi'`` in place of the real failure, so Symphony's own errors are rendered as text.
"""

from __future__ import annotations

import concurrent.futures
import sys
import threading
from typing import TYPE_CHECKING, Callable, Dict, Optional

import cloudpickle

from scaler.utility.exceptions import SymphonyTaskError, TaskExceptionNotSerializableError
from scaler.worker_manager.proxy.symphony.task_output import TaskOutputTag

if sys.version_info >= (3, 11):
    from typing import assert_never
else:
    from typing_extensions import assert_never

if TYPE_CHECKING:
    import soamapi

    from scaler.worker_manager.proxy.symphony._soam.message import Payload, SoamMessage

_REDEPLOY_HINT = "redeploy the service with scripts/symphony/setup_application.py"


class TaskResponseRouter:
    """Tracks in-flight Symphony tasks and completes their futures as responses arrive.

    Holds no ``soamapi`` values, so it stays usable without a Symphony installation. Its methods are
    called from threads owned by the Symphony API, so access to the future map is locked.
    """

    def __init__(self, message_factory: Callable[[], SoamMessage]) -> None:
        self._message_factory = message_factory
        self._callback_lock = threading.Lock()
        self._task_id_to_future: Dict[str, concurrent.futures.Future] = {}

    def on_response(self, task_output_handle: soamapi.TaskOutputHandle) -> None:
        with self._callback_lock:
            task_id = task_output_handle.get_id()

            future = self._task_id_to_future.pop(task_id)

            if not task_output_handle.is_successful():
                future.set_exception(SymphonyTaskError(describe_soam_exception(task_output_handle.get_exception())))
                return

            output_message = self._message_factory()
            task_output_handle.populate_task_output(output_message)
            self._complete(future, task_id, output_message.get_payload())

    def on_exception(self, exception: soamapi.SoamException) -> None:
        with self._callback_lock:
            failure = SymphonyTaskError(describe_soam_exception(exception))

            for future in self._task_id_to_future.values():
                future.set_exception(failure)

            self._task_id_to_future.clear()

    def submit_task(self, task_id: str, future: concurrent.futures.Future) -> None:
        self._task_id_to_future[task_id] = future

    def get_callback_lock(self) -> threading.Lock:
        return self._callback_lock

    @staticmethod
    def _complete(future: concurrent.futures.Future, task_id: str, payload: Payload) -> None:
        """Complete ``future`` from the output payload of a Symphony task that ran to completion.

        A task that ran is not necessarily a task that succeeded: the service reports a raising function
        as a tagged exception rather than as a Symphony failure, so that the original type, message and
        traceback survive the trip.
        """
        try:
            output = cloudpickle.loads(payload)
        except Exception as error:
            future.set_exception(SymphonyTaskError(f"cannot deserialize the output of task {task_id}: {error}"))
            return

        if not isinstance(output, tuple) or len(output) != 2:
            future.set_exception(
                SymphonyTaskError(f"task {task_id} returned an output this version cannot read, {_REDEPLOY_HINT}")
            )
            return

        raw_tag, value = output

        try:
            tag = TaskOutputTag(raw_tag)
        except ValueError:
            # Reachable, unlike the match below: the tag comes from a separately deployed service, which
            # can be older or newer than this worker manager.
            future.set_exception(
                SymphonyTaskError(f"task {task_id} returned an output tagged {raw_tag!r}, {_REDEPLOY_HINT}")
            )
            return

        match tag:
            case TaskOutputTag.RESULT:
                future.set_result(value)
            case TaskOutputTag.EXCEPTION if isinstance(value, BaseException):
                future.set_exception(value)
            case TaskOutputTag.EXCEPTION:
                future.set_exception(
                    SymphonyTaskError(f"task {task_id} tagged a {type(value).__name__} as an exception")
                )
            case TaskOutputTag.UNSERIALIZABLE_EXCEPTION:
                future.set_exception(TaskExceptionNotSerializableError(value))
            case _:
                assert_never(tag)


def describe_soam_exception(exception: Optional[soamapi.SoamException]) -> str:
    """Render a Symphony failure as text, keeping whatever the service embedded in it.

    ``get_embedded_exception`` returns ``None`` when Symphony failed the task itself rather than the
    service raising, so the description carries the failure on its own in that case.
    """
    if exception is None:
        return "IBM Spectrum Symphony reported a failure without a description"

    description = str(exception).strip() or type(exception).__name__

    embedded = exception.get_embedded_exception()
    if embedded is None:
        return description

    return f"{description}: {type(embedded).__name__}: {embedded}"
