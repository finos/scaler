import logging
import sys
import threading
from concurrent.futures import Future, InvalidStateError
from typing import Optional

from scaler.client.agent.mixins import FutureManager
from scaler.client.future import ScalerFuture
from scaler.client.serializer.mixins import Serializer
from scaler.protocol.capnp import TaskCancelConfirm, TaskCancelConfirmType, TaskResult, TaskResultType
from scaler.utility.exceptions import WorkerDiedError
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.profile_result import retrieve_profiling_result_from_task_result

if sys.version_info >= (3, 11):
    from typing import assert_never
else:
    from typing_extensions import assert_never

logger = logging.getLogger(__name__)


class ClientFutureManager(FutureManager):
    def __init__(self, serializer: Serializer):
        self._lock = threading.RLock()
        self._serializer = serializer

        self._task_id_to_future: dict[TaskID, ScalerFuture] = dict()

    def add_future(self, future: Future):
        assert isinstance(future, ScalerFuture)
        with self._lock:
            future.set_running_or_notify_cancel()
            self._task_id_to_future[future.task_id] = future

    def cancel_all_futures(self):
        with self._lock:
            futures_to_cancel = list(self._task_id_to_future.values())

        # Actually cancelling the futures should occur without holding the future manager's lock. That's because
        # `cancel()` is blocking, and requires the manager to process result and cancel confirm messages.

        logger.info(f"canceling {len(futures_to_cancel)} task(s)")
        for future in futures_to_cancel:
            try:
                future.cancel()
            except Exception:
                logger.exception("failed to cancel future during disconnect")

            # The network-driven cancel above may not transition the future to CANCELLED if the
            # agent thread races to set an exception/result on it during the cancel-confirm
            # round-trip (e.g. set_all_futures_with_exception runs while we are waiting on the
            # cancel confirm, leaving the future FINISHED instead of CANCELLED). The client is
            # disconnecting; the original outcome is no longer reachable to user code, so
            # collapse to CANCELLED so callers observing the future see a consistent state.
            if not future.cancelled():
                future.force_set_canceled()

    def set_all_futures_with_exception(self, exception: Exception):
        with self._lock:
            for future in self._task_id_to_future.values():
                try:
                    future.set_exception(exception)
                except InvalidStateError:
                    continue  # Future got canceled

            self._task_id_to_future.clear()

    def on_task_result(self, result: TaskResult):
        result_type = TaskResultType(result.resultType.value)
        with self._lock:
            task_id = result.taskId
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert result.taskId == future.task_id

        # Setting the future's result should occur without holding the future manager's lock, so that it does not delay
        # the other tasks running on the client agent's event loop, such as the heartbeats.

        profile_result = retrieve_profiling_result_from_task_result(result)

        match result_type:
            case TaskResultType.failedWorkerDied:
                future.set_exception(
                    WorkerDiedError(f"worker died when processing task: {task_id.hex()}"), profile_result
                )

            case TaskResultType.success | TaskResultType.failed:
                future.set_result_ready(self.__get_result_object_id(result), result_type, profile_result)

            case _:
                assert_never(result_type)

    def on_task_cancel_confirm(self, cancel_confirm: TaskCancelConfirm):
        cancel_confirm_type = TaskCancelConfirmType(cancel_confirm.cancelConfirmType.value)
        with self._lock:
            task_id = cancel_confirm.taskId
            if task_id not in self._task_id_to_future:
                return

            future = self._task_id_to_future.pop(task_id)
            assert cancel_confirm.taskId == future.task_id

            match cancel_confirm_type:
                case TaskCancelConfirmType.canceled:
                    future.set_canceled()

                case TaskCancelConfirmType.cancelNotFound:
                    logger.error(f"{task_id!r}: task to cancel not found")
                    future.set_canceled()

                case TaskCancelConfirmType.cancelFailed:
                    logger.error(f"{task_id!r}: task cancel failed")
                    self._task_id_to_future[task_id] = future

                case _:
                    assert_never(cancel_confirm_type)

    @staticmethod
    def __get_result_object_id(result: TaskResult) -> Optional[ObjectID]:
        if len(result.results) == 1:
            result_object_id = ObjectID(result.results[0])
        elif len(result.results) == 0:
            # this will happen only if umbrella task is done
            result_object_id = None
        else:
            raise ValueError(f"{result.taskId!r}: received multiple objects for the results: {len(result.results)=}")

        return result_object_id
