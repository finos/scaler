import logging
from typing import Optional

from scaler.protocol.capnp import TaskState
from scaler.scheduler.task.task_event import TaskEvent
from scaler.scheduler.task.task_state_machine import TaskStateMachine
from scaler.utility.identifiers import TaskID

logger = logging.getLogger(__name__)


class TaskStateManager:
    # every state carries a counter, so a new member of TaskState needs no edit here
    TASK_STATES: tuple[TaskState, ...] = tuple(TaskState)

    def __init__(self, debug: bool):
        self._debug = debug
        self._task_id_to_state_machine: dict[TaskID, TaskStateMachine] = dict()
        self._statistics: dict[TaskState, int] = {state: 0 for state in self.TASK_STATES}

    def add_state_machine(self, task_id: TaskID) -> TaskStateMachine:
        """Create new task state machine, the machine starts in the inactive state"""
        assert task_id not in self._task_id_to_state_machine

        state_machine = TaskStateMachine(self._debug)
        self._task_id_to_state_machine[task_id] = state_machine
        self._statistics[state_machine.current_state()] += 1
        return state_machine

    def remove_state_machine(self, task_id: TaskID) -> None:
        """Remove a machine. The caller must hold its lock, or the machine could vanish under a transition that
        already checked for it. This logs rather than asserts, since asserts are stripped under -O.
        """

        state_machine = self._task_id_to_state_machine.pop(task_id, None)
        if state_machine is not None and not state_machine.lock.locked():
            logger.error(f"{task_id!r}: state machine removed without its lock held")

    def get_state_machine(self, task_id: TaskID) -> Optional[TaskStateMachine]:
        return self._task_id_to_state_machine.get(task_id, None)

    def commit(self, task_id: TaskID, event_type: type[TaskEvent], target: TaskState) -> None:
        """Write the state that the action of ``event_type`` returned, or the state a faulted task is torn down into.

        This is the only place that moves a task from one state to another, so it is also the only place the state
        counts stay balanced. The action already ran, so this cannot fail: an event that the source state does not
        accept never reaches this point.
        """

        state_machine = self._task_id_to_state_machine.get(task_id, None)
        if state_machine is None:
            logger.error(f"{task_id!r}: cannot commit {event_type.__name__} for non-existent state machine")
            return

        if not state_machine.lock.locked():
            logger.error(
                f"{task_id!r}: attempted to commit {event_type.__name__} for a state machine without holding its lock"
            )
            return

        source = state_machine.current_state()
        state_machine.commit(event_type.__name__, target)

        self._statistics[source] -= 1
        self._statistics[target] += 1

    def get_statistics(self) -> dict[TaskState, int]:
        return self._statistics

    def get_debug_paths(self) -> str:
        return "\n".join(
            f"{task_id!r}: {state_machine.get_path()}"
            for task_id, state_machine in self._task_id_to_state_machine.items()
        )
