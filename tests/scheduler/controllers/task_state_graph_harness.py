"""Drives ``VanillaTaskController`` with stubbed collaborators.

The controller is the whole task state machine: legality and destination both live in the action that handles an
event. There is no table to read, so the tests derive the graph by driving every (state, event) pair through the real
router and recording the state that comes back.
"""

import dataclasses
from typing import Optional
from unittest.mock import create_autospec

from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher
from scaler.protocol.capnp import (
    BaseMessage,
    StateTask,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
    TaskState,
)
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    GraphTaskController,
    ObjectController,
    WorkerController,
)
from scaler.scheduler.controllers.task_controller import VanillaTaskController
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
from scaler.scheduler.task.task_state_machine import TaskStateMachine
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID

TASK_ID = TaskID(b"task-under-test")
CLIENT_ID = ClientID(b"client-under-test")
WORKER_ID = WorkerID(b"worker-under-test")
REPLACEMENT_WORKER_ID = WorkerID(b"replacement-worker")
FUNCTION_OBJECT_ID = ObjectID(b"function-object-id-padded-to-32b")
FUNCTION_NAME = b"function"

NO_WORKER = WorkerID.invalid_worker_id()

# The states a live machine can be in. A terminal state removes the machine, so no event can ever reach one.
LIVE_TASK_STATES = (TaskState.inactive, TaskState.running, TaskState.canceling, TaskState.balanceCanceling)

REJECTED = "(rejected)"


def make_task(task_id: TaskID = TASK_ID) -> Task:
    return Task(
        taskId=task_id,
        source=CLIENT_ID,
        metadata=b"",
        funcObjectId=FUNCTION_OBJECT_ID,
        functionArgs=[],
        capabilities=[],
    )


def make_task_cancel(force: bool = False) -> TaskCancel:
    return TaskCancel(taskId=TASK_ID, flags=TaskCancel.TaskCancelFlags(force=force))


def make_task_result(result_type: TaskResultType) -> TaskResult:
    return TaskResult(taskId=TASK_ID, resultType=result_type, metadata=b"", results=[])


def make_task_cancel_confirm(confirm_type: TaskCancelConfirmType) -> TaskCancelConfirm:
    return TaskCancelConfirm(taskId=TASK_ID, cancelConfirmType=confirm_type)


@dataclasses.dataclass(frozen=True)
class Scenario:
    """One event, together with the collaborator answers that make its outcome deterministic."""

    name: str
    event: TaskEvent
    worker_holds_task: bool = True
    capacity_available: bool = True


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("HasCapacity", HasCapacity(task_id=TASK_ID, worker_id=REPLACEMENT_WORKER_ID)),
    Scenario(
        "TaskCancelRequested", TaskCancelRequested(task_id=TASK_ID, client_id=CLIENT_ID, task_cancel=make_task_cancel())
    ),
    Scenario(
        "TaskCancelRequested (worker gone)",
        TaskCancelRequested(task_id=TASK_ID, client_id=CLIENT_ID, task_cancel=make_task_cancel()),
        worker_holds_task=False,
    ),
    Scenario("BalanceCancelRequested", BalanceCancelRequested(task_id=TASK_ID)),
    Scenario("BalanceCancelRequested (worker gone)", BalanceCancelRequested(task_id=TASK_ID), worker_holds_task=False),
    Scenario(
        "TaskResultReceived (success)",
        TaskResultReceived(task_id=TASK_ID, task_result=make_task_result(TaskResultType.success)),
    ),
    Scenario(
        "TaskResultReceived (failed)",
        TaskResultReceived(task_id=TASK_ID, task_result=make_task_result(TaskResultType.failed)),
    ),
    Scenario(
        "TaskResultReceived (worker died)",
        TaskResultReceived(task_id=TASK_ID, task_result=make_task_result(TaskResultType.failedWorkerDied)),
    ),
    Scenario(
        "CancelConfirmCanceled",
        CancelConfirmCanceled(
            task_id=TASK_ID, task_cancel_confirm=make_task_cancel_confirm(TaskCancelConfirmType.canceled)
        ),
    ),
    Scenario(
        "CancelConfirmCanceled (no capacity)",
        CancelConfirmCanceled(
            task_id=TASK_ID, task_cancel_confirm=make_task_cancel_confirm(TaskCancelConfirmType.canceled)
        ),
        capacity_available=False,
    ),
    Scenario(
        "CancelConfirmFailed",
        CancelConfirmFailed(
            task_id=TASK_ID, task_cancel_confirm=make_task_cancel_confirm(TaskCancelConfirmType.cancelFailed)
        ),
    ),
    Scenario(
        "CancelConfirmNotFound",
        CancelConfirmNotFound(
            task_id=TASK_ID, task_cancel_confirm=make_task_cancel_confirm(TaskCancelConfirmType.cancelNotFound)
        ),
    ),
    Scenario("WorkerDisconnected", WorkerDisconnected(task_id=TASK_ID, worker_id=WORKER_ID)),
    Scenario(
        "WorkerDisconnected (no capacity)",
        WorkerDisconnected(task_id=TASK_ID, worker_id=WORKER_ID),
        capacity_available=False,
    ),
)


class TaskControllerHarness:
    """A ``VanillaTaskController`` whose collaborators are recording stubs."""

    def __init__(self) -> None:
        self.binder = create_autospec(AsyncBinder, instance=True)
        self.binder_monitor = create_autospec(AsyncPublisher, instance=True)
        self.connector_storage = create_autospec(AsyncObjectStorageConnector, instance=True)
        self.client_controller = create_autospec(ClientController, instance=True)
        self.object_controller = create_autospec(ObjectController, instance=True)
        self.worker_controller = create_autospec(WorkerController, instance=True)
        self.graph_controller = create_autospec(GraphTaskController, instance=True)

        self.client_controller.on_task_finish.return_value = CLIENT_ID
        self.client_controller.get_client_id.return_value = CLIENT_ID
        self.object_controller.get_object_name.return_value = FUNCTION_NAME
        self.graph_controller.is_graph_subtask.return_value = False

        self.worker_controller.acquire_worker.return_value = WORKER_ID
        self.worker_controller.on_task_cancel.return_value = WORKER_ID
        self.worker_controller.get_worker_by_task_id.return_value = WORKER_ID

        self.controller = VanillaTaskController(create_autospec(VanillaConfigController, instance=True))
        self.controller.register(
            binder=self.binder,
            binder_monitor=self.binder_monitor,
            connector_storage=self.connector_storage,
            client_controller=self.client_controller,
            object_controller=self.object_controller,
            worker_controller=self.worker_controller,
            graph_controller=self.graph_controller,
        )

    def set_worker_holds_task(self, holds: bool) -> None:
        self.worker_controller.on_task_cancel.return_value = WORKER_ID if holds else NO_WORKER

    def set_capacity_available(self, available: bool) -> None:
        self.worker_controller.acquire_worker.return_value = REPLACEMENT_WORKER_ID if available else NO_WORKER

    def apply(self, scenario: Scenario) -> None:
        self.set_worker_holds_task(scenario.worker_holds_task)
        self.set_capacity_available(scenario.capacity_available)

    def reset_recorded_calls(self) -> None:
        for stub in (
            self.binder,
            self.binder_monitor,
            self.client_controller,
            self.object_controller,
            self.worker_controller,
            self.graph_controller,
        ):
            stub.reset_mock()

        self.client_controller.on_task_finish.return_value = CLIENT_ID
        self.object_controller.get_object_name.return_value = FUNCTION_NAME
        self.graph_controller.is_graph_subtask.return_value = False

    def get_state_machine(self) -> Optional[TaskStateMachine]:
        return self.controller._task_state_manager.get_state_machine(TASK_ID)

    def messages_sent_to(self, peer: bytes) -> list[BaseMessage]:
        return [call.args[1] for call in self.binder.send.await_args_list if call.args[0] == peer]

    def cancel_confirms_sent_to(self, peer: bytes) -> list[TaskCancelConfirm]:
        return [message for message in self.messages_sent_to(peer) if isinstance(message, TaskCancelConfirm)]

    def task_results_sent_to(self, peer: bytes) -> list[TaskResult]:
        return [message for message in self.messages_sent_to(peer) if isinstance(message, TaskResult)]

    def monitored_task_states(self) -> list[TaskState]:
        return [
            call.args[0].state
            for call in self.binder_monitor.send.await_args_list
            if isinstance(call.args[0], StateTask)
        ]

    async def route(self, event: TaskEvent) -> None:
        await self.controller._VanillaTaskController__route(event)  # type: ignore[attr-defined]

    async def enter_state(self, state: TaskState) -> TaskStateMachine:
        """Drive a fresh task into ``state`` through the real message handlers."""

        task = make_task()

        if state == TaskState.inactive:
            self.set_capacity_available(False)
            await self.controller.on_task_new(task)
            self.set_capacity_available(True)
        else:
            await self.controller.on_task_new(task)
            if state == TaskState.canceling:
                await self.controller.on_task_cancel(CLIENT_ID, make_task_cancel())
            elif state == TaskState.balanceCanceling:
                await self.controller.on_task_balance_cancel(TASK_ID)
            elif state != TaskState.running:
                raise ValueError(f"cannot drive a task into {state.name}, it is not a live state")

        state_machine = self.get_state_machine()
        assert state_machine is not None
        assert state_machine.current_state() == state, f"setup reached {state_machine.current_state().name}"

        self.reset_recorded_calls()
        return state_machine


async def drive(harness: TaskControllerHarness, source: TaskState, scenario: Scenario) -> str:
    """Apply one scenario to a task in ``source`` and return the name of the state that it lands in."""

    state_machine = await harness.enter_state(source)
    harness.apply(scenario)

    path_before = state_machine.get_path()
    await harness.route(scenario.event)

    if state_machine.get_path() == path_before:
        return REJECTED

    return state_machine.current_state().name
