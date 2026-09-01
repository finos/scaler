from typing import Optional

from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.agent.mixins import ObjectManager, TaskManager
from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import GraphTask, Task, TaskCancel, TaskCancelConfirm, TaskResult


class ClientTaskManager(TaskManager):
    def __init__(self):
        self._task_ids: set[bytes] = set()

        self._connector_external: Optional[AsyncConnector] = None
        self._object_manager: Optional[ObjectManager] = None
        self._future_manager: Optional[ClientFutureManager] = None

    def register(
        self, connector_external: AsyncConnector, object_manager: ObjectManager, future_manager: ClientFutureManager
    ):
        self._connector_external = connector_external
        self._object_manager = object_manager
        self._future_manager = future_manager

    async def on_new_task(self, task: Task):
        self._task_ids.add(task.taskId)
        await self._connector_external.send(task, detached=True)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        # We might receive a cancel task event on a previously finished task if:
        # -	The scheduler sends a TaskResult message to the client agent
        # - The client sends a TaskCancel message to the client agent, as it's not yet aware the task finished.
        # - The client agent processes the TaskResult message and removes the task from self._task_ids
        # - The client agent processes the TaskCancel message (that was already queued before processing the
        #   TaskResult), and fails on self._task_ids.remove() as the task_id no longer exists.

        if task_cancel.taskId not in self._task_ids:
            return

        await self._connector_external.send(task_cancel, detached=True)

    async def on_new_graph_task(self, task: GraphTask):
        self._task_ids.add(task.taskId)
        self._task_ids.update(set(task.targets))
        await self._connector_external.send(task, detached=True)

    async def on_task_result(self, result: TaskResult):
        # All task result objects must be propagated to the object manager, even if we do not track the task anymore
        # (e.g. if it got cancelled). If we don't, we might lose track of these result objects and not properly clear
        # them.
        self._object_manager.on_task_result(result)

        if result.taskId not in self._task_ids:
            return

        self._task_ids.remove(result.taskId)

        self._future_manager.on_task_result(result)

    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        if task_cancel_confirm.taskId not in self._task_ids:
            return

        self._task_ids.remove(task_cancel_confirm.taskId)
        self._future_manager.on_task_cancel_confirm(task_cancel_confirm)
