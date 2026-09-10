import asyncio
import contextlib
import logging
import sys
from collections import deque
from typing import AsyncGenerator, Literal, Optional

from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher
from scaler.protocol.capnp import (
    ObjectMetadata,
    StateTask,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskManagerStatus,
    TaskResult,
    TaskResultType,
    TaskState,
)
from scaler.protocol.helpers import capabilities_to_dict, dict_to_capabilities
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    GraphTaskController,
    ObjectController,
    TaskController,
    WorkerController,
)
from scaler.scheduler.task.task_event import (
    BalanceCancelRequested,
    CancelConfirmCanceled,
    CancelConfirmFailed,
    CancelConfirmNotFound,
    HasCapacity,
    TaskCancelRequested,
    TaskEvent,
    TaskResultReceived,
    WorkerDisconnected,
)
from scaler.scheduler.task.task_state_machine import TERMINAL_TASK_STATES, TaskStateMachine
from scaler.scheduler.task.task_state_manager import TaskStateManager
from scaler.utility.exceptions import SchedulerError
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.serialization import serialize_failure

if sys.version_info >= (3, 11):
    from typing import assert_never
else:
    from typing_extensions import assert_never

logger = logging.getLogger(__name__)

# A transition holds its machine's lock for a few sends and no I/O waits, so this is not a tuning knob balancing
# throughput against latency: it is the point past which the only remaining explanation is a deadlock. It sits well
# below the 60 second worker and client timeouts, so a stuck lock reports itself as a specific error naming the task
# rather than resurfacing later as a worker or client being declared dead. If it ever fires, fix the cycle.
LOCK_ACQUIRE_TIMEOUT_SECONDS = 10

# The state that each action can produce. mypy checks every return against these, so the behavior of an action cannot
# widen without its annotation widening too.
DispatchTargetStates = Literal[TaskState.inactive, TaskState.running]
HasCapacityTargetStates = Literal[TaskState.running]
TaskCancelTargetStates = Literal[TaskState.canceled, TaskState.canceling, TaskState.canceledNotFound]
BalanceCancelTargetStates = Literal[TaskState.balanceCanceling]
TaskResultTargetStates = Literal[TaskState.success, TaskState.failed, TaskState.failedWorkerDied]
CancelConfirmCanceledTargetStates = Literal[TaskState.canceled, TaskState.inactive, TaskState.running]
CancelConfirmFailedTargetStates = Literal[TaskState.running]
CancelConfirmNotFoundTargetStates = Literal[TaskState.canceledNotFound]
DisconnectTargetStates = Literal[TaskState.inactive, TaskState.running, TaskState.canceled]


def task_result_target(result_type: TaskResultType) -> TaskResultTargetStates:
    match result_type:
        case TaskResultType.success:
            return TaskState.success
        case TaskResultType.failed:
            return TaskState.failed
        case TaskResultType.failedWorkerDied:
            return TaskState.failedWorkerDied
        case _:
            assert_never(result_type)


class VanillaTaskController(TaskController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._worker_controller: Optional[WorkerController] = None

        self._graph_controller: Optional[GraphTaskController] = None

        self._task_id_to_task: dict[TaskID, Task] = dict()
        self._task_state_manager: TaskStateManager = TaskStateManager(debug=True)

        self._unassigned: deque[TaskID] = deque()  # type: ignore[misc]

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncPublisher,
        connector_storage: AsyncObjectStorageConnector,
        client_controller: ClientController,
        object_controller: ObjectController,
        worker_controller: WorkerController,
        graph_controller: GraphTaskController,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._connector_storage = connector_storage

        self._client_controller = client_controller
        self._object_controller = object_controller
        self._worker_controller = worker_controller
        self._graph_controller = graph_controller

    async def routine(self):
        # TODO: we don't need loop task anymore, but I will leave this routine API here in case we need in the future
        pass

    async def on_task_new(self, task: Task):
        task.capabilities = capabilities_to_dict(task.capabilities)
        if self._task_state_manager.get_state_machine(task.taskId) is not None:
            logger.error(
                f"{task.taskId!r}: state machine already exists: "
                f"{self._task_state_manager.get_state_machine(task.taskId)}"
            )
            return

        # the first entry into inactive takes no transition, the machine starts there by construction
        self._task_state_manager.add_state_machine(task.taskId)

        self._client_controller.on_task_begin(task.source, task.taskId)
        self._task_id_to_task[task.taskId] = task

        worker_id = self._worker_controller.acquire_worker(task)
        if not worker_id.is_valid():
            # put task on hold until a worker is added or a task is finished/canceled (means have capacity)
            self._unassigned.append(task.taskId)
            await self.__send_monitor(
                task.taskId, TaskState.inactive, self._object_controller.get_object_name(task.funcObjectId)
            )
            return

        await self.__route(HasCapacity(task_id=task.taskId, worker_id=worker_id))

    async def on_task_cancel(self, client_id: ClientID, task_cancel: TaskCancel):
        if self._task_state_manager.get_state_machine(task_cancel.taskId) is None:
            logger.error(f"{task_cancel.taskId!r}: task not exists while received TaskCancel, send TaskCancelConfirm")

            task_cancel_confirm = TaskCancelConfirm(
                taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelNotFound
            )

            if self._graph_controller.is_graph_subtask(task_cancel.taskId):
                await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

            await self._binder.send(client_id, task_cancel_confirm, detached=True)
            return

        await self.__route(
            TaskCancelRequested(task_id=task_cancel.taskId, client_id=client_id, task_cancel=task_cancel)
        )

    async def on_task_balance_cancel(self, task_id: TaskID):
        await self.__route(BalanceCancelRequested(task_id=task_id))

    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        task_id = task_cancel_confirm.taskId
        cancel_confirm_type = TaskCancelConfirmType(task_cancel_confirm.cancelConfirmType.value)

        event: TaskEvent
        match cancel_confirm_type:
            case TaskCancelConfirmType.canceled:
                event = CancelConfirmCanceled(task_id=task_id, task_cancel_confirm=task_cancel_confirm)
            case TaskCancelConfirmType.cancelFailed:
                event = CancelConfirmFailed(task_id=task_id, task_cancel_confirm=task_cancel_confirm)
            case TaskCancelConfirmType.cancelNotFound:
                event = CancelConfirmNotFound(task_id=task_id, task_cancel_confirm=task_cancel_confirm)
            case _:
                raise ValueError(f"unknown TaskCancelConfirmType: {task_cancel_confirm.cancelConfirmType}")

        await self.__route(event)

    async def on_task_result(self, task_result: TaskResult):
        await self.__route(TaskResultReceived(task_id=task_result.taskId, task_result=task_result))

    async def on_worker_connect(self, worker_id: WorkerID):
        await self.__retry_unassignable()

    async def on_worker_disconnect(self, task_id: TaskID, worker_id: WorkerID):
        await self.__route(WorkerDisconnected(task_id=task_id, worker_id=worker_id))

    def get_status(self) -> TaskManagerStatus:
        statistics = self._task_state_manager.get_statistics()

        return TaskManagerStatus(
            stateToCount=[
                TaskManagerStatus.Pair(state=task_state.value, count=count) for task_state, count in statistics.items()
            ]
        )

    async def __acquire_machine(self, event: TaskEvent) -> Optional[TaskStateMachine]:
        """Take the lock of the machine for an event, or return None with the reason logged.

        Returning None always means no lock is held, including for the refusal that can only be decided after the
        acquire, which releases before it returns. The caller therefore owns the lock if and only if this returns a
        machine.
        """

        state_machine = self._task_state_manager.get_state_machine(event.task_id)
        if state_machine is None:
            logger.error(f"{event.task_id!r}: received {type(event).__name__} for non-existent state machine")
            return None

        try:
            await asyncio.wait_for(state_machine.lock.acquire(), timeout=LOCK_ACQUIRE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.error(
                f"{event.task_id!r}: lock not acquired in {LOCK_ACQUIRE_TIMEOUT_SECONDS}s, dropping "
                f"{type(event).__name__}, path: {state_machine.get_path()}"
            )
            return None

        # it's possible that the state machine for this task was changed (or removed) by the last lock-holder
        # we do one more check just to be sure
        if self._task_state_manager.get_state_machine(event.task_id) is not state_machine:
            logger.info(f"{event.task_id!r}: {type(event).__name__} arrived after the task was removed")
            state_machine.lock.release()
            return None

        return state_machine

    @contextlib.asynccontextmanager
    async def __locked_machine(self, event: TaskEvent) -> AsyncGenerator[Optional[TaskStateMachine], None]:
        """Yield the machine for an event with its lock held, or None if the event cannot be applied."""

        state_machine = await self.__acquire_machine(event)
        try:
            yield state_machine
        finally:
            if state_machine is not None:
                state_machine.lock.release()

    async def __route(self, event: TaskEvent) -> None:
        """Run the action of an event and transition the state.
        The machine's lock is held for the whole transition.
        """

        async with self.__locked_machine(event) as state_machine:
            if state_machine is None:
                return

            source = state_machine.current_state()  # read inside the lock, never before it

            target: Optional[TaskState]
            try:
                match event:
                    case HasCapacity():
                        target = await self.__on_has_capacity(source, event)
                    case TaskCancelRequested():
                        target = await self.__on_task_cancel(source, event)
                    case BalanceCancelRequested():
                        target = await self.__on_balance_task_cancel(source, event)
                    case TaskResultReceived():
                        target = await self.__on_task_result(source, event)
                    case CancelConfirmCanceled():
                        target = await self.__on_cancel_confirm_canceled(source, event)
                    case CancelConfirmFailed():
                        target = await self.__on_cancel_confirm_failed(source, event)
                    case CancelConfirmNotFound():
                        target = await self.__on_cancel_confirm_not_found(source, event)
                    case WorkerDisconnected():
                        target = await self.__on_worker_disconnect(source, event)
                    case _:
                        assert_never(event)
            except Exception:
                # the exception is not re-raised: __route runs from both timer loops and message handlers, and an
                # exception that escapes either one propagates through asyncio.gather and stops the scheduler
                logger.exception(
                    f"{event.task_id!r}: exception happened, event: {type(event).__name__} from {source.name}, "
                    f"path: {state_machine.get_path()}"
                )
                await self.__fail_task_to_client(event, source)
                return

            if target is None:
                logger.info(f"{event.task_id!r}: {type(event).__name__} is not permitted from {source.name}")
                return

            self._task_state_manager.commit(event.task_id, type(event), target)

            if target in TERMINAL_TASK_STATES:
                self._task_state_manager.remove_state_machine(event.task_id)
                self._task_id_to_task.pop(event.task_id, None)

    async def __fail_task_to_client(self, event: TaskEvent, source: TaskState) -> None:
        """Fail a task whose state machine action raised.
        Sends the client a failed result carrying a SchedulerError.
        This must not raise, it runs from the ``except`` of the router.
        """

        try:
            await self.__report_fault_to_client(event, source)
        except Exception:
            logger.exception(f"{event.task_id!r}: could not report the scheduler fault to the client")

        try:
            # the action may or may not have released the worker before it raised. a second release logs that the
            # task is not in the worker queue, which is noise on a path that is already an error
            await self._worker_controller.on_task_done(event.task_id)
        except Exception:
            logger.exception(f"{event.task_id!r}: could not release the worker of a faulted task")

        # commit before removing: the machine is being dropped either way, and without a commit the source state keeps
        # its count in _statistics forever. failed is what the client was just told
        self._task_state_manager.commit(event.task_id, type(event), TaskState.failed)
        self._task_state_manager.remove_state_machine(event.task_id)

        self._task_id_to_task.pop(event.task_id, None)
        if event.task_id in self._unassigned:
            # the payload is gone, so an id left in the queue would raise in __acquire_workers on every later drain
            self._unassigned.remove(event.task_id)

        try:
            await self.__retry_unassignable()
        except Exception:
            logger.exception(f"{event.task_id!r}: could not redistribute capacity after a faulted task")

    async def __report_fault_to_client(self, event: TaskEvent, source: TaskState) -> None:
        client = self._client_controller.on_task_finish(event.task_id)
        if client is None:
            logger.warning(
                f"{event.task_id!r}: dropping scheduler fault report, the action already replied or the owning "
                f"client is no longer registered"
            )
            return

        object_id = ObjectID.generate_object_id(client)
        payload = serialize_failure(
            SchedulerError(
                f"scheduler failed to apply {type(event).__name__} from {source.name} for task {event.task_id.hex()}"
            )
        )
        await self._connector_storage.set_object(object_id, payload)
        self._object_controller.on_add_object(
            client, object_id, ObjectMetadata.ObjectContentType.object, b"<scheduler error>"
        )

        task_result = TaskResult(
            taskId=event.task_id, resultType=TaskResultType.failed, metadata=b"", results=[object_id]
        )
        await self._binder.send(client, task_result, detached=True)
        await self.__send_monitor(event.task_id, TaskState.failed, b"")

        if self._graph_controller.is_graph_subtask(event.task_id):
            await self._graph_controller.on_graph_sub_task_result(task_result)

    async def __on_has_capacity(self, source: TaskState, event: HasCapacity) -> Optional[HasCapacityTargetStates]:
        match source:
            case TaskState.inactive:
                await self.__send_task_to_worker(event.worker_id, event.task_id)
                return TaskState.running
            case (
                TaskState.running
                | TaskState.canceling
                | TaskState.balanceCanceling
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # the task already holds a worker, or it finished before the acquired worker could be used
                return None
            case _:
                assert_never(source)

    async def __on_task_cancel(self, source: TaskState, event: TaskCancelRequested) -> Optional[TaskCancelTargetStates]:
        match source:
            case TaskState.inactive:
                # the task sits in the queue, so the scheduler can confirm the cancel itself
                if event.task_id in self._unassigned:
                    self._unassigned.remove(event.task_id)
                else:
                    await self._worker_controller.on_task_done(event.task_id)

                await self.__send_task_cancel_confirm_to_client(
                    TaskCancelConfirm(taskId=event.task_id, cancelConfirmType=TaskCancelConfirmType.canceled),
                    TaskState.canceled,
                )
                return TaskState.canceled
            case TaskState.running:
                # in case the task being canceled has no task in the scheduler, so we know which client to confirm to
                self._client_controller.on_task_begin(event.client_id, event.task_id)

                if await self.__send_task_cancel_to_worker(event.task_cancel, TaskState.canceling):
                    return TaskState.canceling

                await self.__send_task_cancel_confirm_to_client(
                    TaskCancelConfirm(taskId=event.task_id, cancelConfirmType=TaskCancelConfirmType.cancelNotFound),
                    TaskState.canceledNotFound,
                )
                return TaskState.canceledNotFound
            case TaskState.balanceCanceling:
                # a TaskCancel is already on its way to the worker, so we must not send a second one. The confirm of
                # the balance cancel then completes this client cancel.
                return TaskState.canceling
            case (
                TaskState.canceling
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # a cancel is already in flight, or the task already finished
                return None
            case _:
                assert_never(source)

    async def __on_balance_task_cancel(
        self, source: TaskState, event: BalanceCancelRequested
    ) -> Optional[BalanceCancelTargetStates]:
        match source:
            case TaskState.running:
                task_cancel = TaskCancel(taskId=event.task_id, flags=TaskCancel.TaskCancelFlags(force=False))
                # FIXME: when no worker holds the task, the balance cannot complete and the task is stranded here with
                # no cancel in flight and no exit path. this preserves the behavior of the transition table, which
                # rejected the cancelNotFound that this case used to raise. the recovery is a behavior change, either
                # terminate the task towards the client or reschedule it, so it needs its own review
                await self.__send_task_cancel_to_worker(task_cancel, TaskState.balanceCanceling)
                return TaskState.balanceCanceling
            case (
                TaskState.inactive
                | TaskState.canceling
                | TaskState.balanceCanceling
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # balance advice is stale: the task no longer runs where the balancer believed it did
                return None
            case _:
                assert_never(source)

    async def __on_task_result(self, source: TaskState, event: TaskResultReceived) -> Optional[TaskResultTargetStates]:
        match source:
            case TaskState.running | TaskState.balanceCanceling:
                target = task_result_target(TaskResultType(event.task_result.resultType.value))
                if target == TaskState.failedWorkerDied:
                    logger.warning(f"{event.task_id!r}: reporting failedWorkerDied to the client")
                await self.__send_task_result_to_client(event.task_result, target)
                return target
            case (
                TaskState.inactive
                | TaskState.canceling
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # the task has no worker, or it already reported its outcome to the client
                return None
            case _:
                assert_never(source)

    async def __on_cancel_confirm_canceled(
        self, source: TaskState, event: CancelConfirmCanceled
    ) -> Optional[CancelConfirmCanceledTargetStates]:
        match source:
            case TaskState.canceling:
                await self._worker_controller.on_task_done(event.task_id)
                await self.__send_task_cancel_confirm_to_client(event.task_cancel_confirm, TaskState.canceled)
                return TaskState.canceled
            case TaskState.balanceCanceling:
                # the worker released the task, deregister it from that worker and reschedule it
                await self._worker_controller.on_task_done(event.task_id)
                return await self.__acquire_and_dispatch(event.task_id)
            case (
                TaskState.inactive
                | TaskState.running
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # no cancel is in flight, or the task already reported its outcome to the client
                return None
            case _:
                assert_never(source)

    async def __on_cancel_confirm_failed(
        self, source: TaskState, event: CancelConfirmFailed
    ) -> Optional[CancelConfirmFailedTargetStates]:
        match source:
            case TaskState.canceling | TaskState.balanceCanceling:
                # the worker refused the cancel because the task is already running, wait for the real result
                return TaskState.running
            case (
                TaskState.inactive
                | TaskState.running
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # no cancel is in flight, or the task already reported its outcome to the client
                return None
            case _:
                assert_never(source)

    async def __on_cancel_confirm_not_found(
        self, source: TaskState, event: CancelConfirmNotFound
    ) -> Optional[CancelConfirmNotFoundTargetStates]:
        match source:
            case TaskState.canceling:
                # the worker does not hold the task, but the scheduler still maps it to that worker
                await self._worker_controller.on_task_done(event.task_id)
                await self.__send_task_cancel_confirm_to_client(event.task_cancel_confirm, TaskState.canceledNotFound)
                return TaskState.canceledNotFound
            case TaskState.balanceCanceling:
                # FIXME: strands the task, it has no worker, no cancel in flight and no exit path. this preserves the
                # behavior of the transition table, which never accepted this transition from balanceCanceling. the
                # recovery is a behavior change, either terminate the task towards the client or reschedule it, so it
                # needs its own review
                return None
            case (
                TaskState.inactive
                | TaskState.running
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # no cancel is in flight, or the task already reported its outcome to the client
                return None
            case _:
                assert_never(source)

    async def __on_worker_disconnect(
        self, source: TaskState, event: WorkerDisconnected
    ) -> Optional[DisconnectTargetStates]:
        match source:
            case TaskState.running | TaskState.balanceCanceling:
                return await self.__acquire_and_dispatch(event.task_id)
            case TaskState.canceling:
                # remove_worker already released the capacity, so there is no on_task_done here
                await self.__send_task_cancel_confirm_to_client(
                    TaskCancelConfirm(taskId=event.task_id, cancelConfirmType=TaskCancelConfirmType.canceled),
                    TaskState.canceled,
                )
                return TaskState.canceled
            case (
                TaskState.inactive
                | TaskState.success
                | TaskState.failed
                | TaskState.failedWorkerDied
                | TaskState.canceled
                | TaskState.canceledNotFound
            ):
                # the task holds no worker to lose, or it already reported its outcome to the client
                return None
            case _:
                assert_never(source)

    async def __acquire_and_dispatch(self, task_id: TaskID) -> DispatchTargetStates:
        """Look for a worker for a task that has none, and send the task to it if one is free."""

        task = self._task_id_to_task[task_id]
        function_name = self._object_controller.get_object_name(task.funcObjectId)

        worker_id = self._worker_controller.acquire_worker(task)
        if not worker_id.is_valid():
            # put task on hold until a worker is added or a task is finished/canceled (means have capacity)
            self._unassigned.append(task_id)
            await self.__send_monitor(task_id, TaskState.inactive, function_name)
            return TaskState.inactive

        await self.__send_task_to_worker(worker_id, task_id)
        return TaskState.running

    async def __send_task_to_worker(self, worker_id: WorkerID, task_id: TaskID) -> None:
        task = self._task_id_to_task[task_id]
        await self._binder.send(worker_id, task, detached=True)
        await self.__send_monitor(
            task_id, TaskState.running, self._object_controller.get_object_name(task.funcObjectId)
        )

    async def __send_task_cancel_to_worker(self, task_cancel: TaskCancel, task_state: TaskState) -> bool:
        """Send a cancel to the worker that holds the task, return False if no worker holds it anymore."""

        worker = await self._worker_controller.on_task_cancel(task_cancel)
        assert isinstance(worker, WorkerID)
        if not worker.is_valid():
            logger.error(f"{task_cancel.taskId!r}: cannot find task in worker to cancel")
            return False

        await self._binder.send(worker, task_cancel, detached=True)
        await self.__send_monitor(task_cancel.taskId, task_state, b"")
        return True

    async def __send_task_result_to_client(self, task_result: TaskResult, task_state: TaskState) -> None:
        await self._worker_controller.on_task_done(task_result.taskId)
        client = self._client_controller.on_task_finish(task_result.taskId)
        if client is None:
            logger.warning(
                f"{task_result.taskId!r}: dropping {task_result.resultType} result, owning client is no longer "
                f"registered (likely disconnected via client_timeout_seconds while the task was running)"
            )
        else:
            await self._binder.send(client, task_result, detached=True)

        func_name = b""
        task = self._task_id_to_task.get(task_result.taskId)
        if task:
            func_name = self._object_controller.get_object_name(task.funcObjectId)
        await self.__send_monitor(task_result.taskId, task_state, func_name, task_result.metadata)

        if self._graph_controller.is_graph_subtask(task_result.taskId):
            await self._graph_controller.on_graph_sub_task_result(task_result)

        await self.__retry_unassignable()

    async def __send_task_cancel_confirm_to_client(
        self, task_cancel_confirm: TaskCancelConfirm, task_state: TaskState
    ) -> None:
        client = self._client_controller.on_task_finish(task_cancel_confirm.taskId)
        if client is None:
            logger.warning(
                f"{task_cancel_confirm.taskId!r}: dropping task cancel confirm, owning client is no "
                f"longer registered"
            )
        else:
            await self._binder.send(client, task_cancel_confirm, detached=True)
        await self.__send_monitor(task_cancel_confirm.taskId, task_state, b"")

        if self._graph_controller.is_graph_subtask(task_cancel_confirm.taskId):
            await self._graph_controller.on_graph_sub_task_cancel_confirm(task_cancel_confirm)

        await self.__retry_unassignable()

    async def __send_monitor(
        self, task_id: TaskID, task_state: TaskState, function_name: bytes, metadata: bytes = b""
    ) -> None:
        worker = self._worker_controller.get_worker_by_task_id(task_id)
        capabilities = self._task_id_to_task[task_id].capabilities if task_id in self._task_id_to_task else []
        await self._binder_monitor.send(
            StateTask(
                taskId=task_id,
                functionName=function_name,
                state=task_state,
                worker=worker,
                capabilities=dict_to_capabilities(capabilities),
                metadata=metadata,
            )
        )

    async def __retry_unassignable(self):
        futures = [
            self.__route(HasCapacity(task_id=task_id, worker_id=worker_id))
            for task_id, worker_id in self.__acquire_workers()
        ]

        await asyncio.gather(*futures)

    def __acquire_workers(self) -> list[tuple[TaskID, WorkerID]]:
        """please note this function has to be atomic, means no async decorated in order to make unassigned queue to be
        synced, also this function should return as list not generator because of atomic
        """

        ready_to_assign = list()
        while len(self._unassigned) > 0:
            worker_id = self._worker_controller.acquire_worker(self._task_id_to_task[self._unassigned[0]])
            if not worker_id.is_valid():
                break

            task_id = self._unassigned.popleft()
            ready_to_assign.append((task_id, worker_id))

        return ready_to_assign
