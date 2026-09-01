import abc
from typing import Any, Optional

from scaler.protocol.capnp import (
    ClientDisconnect,
    ClientHeartbeat,
    GraphTask,
    InformationRequest,
    ObjectInstruction,
    ObjectMetadata,
    ScalingManagerStatus,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskResult,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
    WorkerManagerCommand,
    WorkerManagerHeartbeat,
)
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.mixins import Reporter
from scaler.utility.snapshot import InformationSnapshot


class ConfigController(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_config(self, path: str) -> Any:
        raise NotImplementedError()

    @abc.abstractmethod
    def update_config(self, path: str, value: Any):
        raise NotImplementedError()


class ObjectController(Reporter):
    @abc.abstractmethod
    async def on_object_instruction(self, source: bytes, request: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_add_object(
        self,
        client_id: ClientID,
        object_id: ObjectID,
        object_type: ObjectMetadata.ObjectContentType,
        object_name: bytes,
    ):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_del_objects(self, client_id: ClientID, object_ids: set[ObjectID]):
        raise NotImplementedError()

    @abc.abstractmethod
    def clean_client(self, client_id: ClientID):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_object(self, object_id: ObjectID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_name(self, object_id: ObjectID) -> bytes:
        raise NotImplementedError()


class ClientController(Reporter):
    @abc.abstractmethod
    def get_client_task_ids(self, client_id: ClientID) -> set[TaskID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def has_client_id(self, client_id: ClientID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_client_id(self, task_id: TaskID) -> Optional[ClientID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_begin(self, client_id: ClientID, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_finish(self, task_id: TaskID) -> Optional[ClientID]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, client_id: ClientID, info: ClientHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    def notice_client_activity(self, client_id: ClientID):
        """Refresh the liveness timestamp of an already-tracked client.

        Called for every inbound message from a known client so that the
        heartbeat-based timeout treats any client-bound traffic as liveness,
        not only ``ClientHeartbeat``. Browser / emscripten clients in
        particular can stall their asyncio loop for tens of seconds during
        heavy synchronous user code (cloudpickle of large constants, pargraph
        graph construction, etc.) and would otherwise be wrongly evicted.
        Must be a no-op for unknown clients.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_disconnect(self, client_id: ClientID, request: ClientDisconnect):
        raise NotImplementedError()


class GraphTaskController(Reporter):
    @abc.abstractmethod
    async def on_graph_task(self, client_id: ClientID, graph_task: GraphTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_task_cancel(self, graph_task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_sub_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_sub_task_result(self, result: TaskResult) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_graph_subtask(self, task_id: TaskID) -> bool:
        raise NotImplementedError()


class TaskController(Reporter):
    @abc.abstractmethod
    async def on_task_new(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client_id: ClientID, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_balance_cancel(self, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_worker_connect(self, worker_id: WorkerID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_worker_disconnect(self, task_id: TaskID, worker_id: WorkerID):
        raise NotImplementedError()


class WorkerController(Reporter):
    @abc.abstractmethod
    def acquire_worker(self, task: Task) -> Optional[WorkerID]:
        """this acquires worker should be atomic, means it cannot be async decorated, otherwise it will create gap that
        get worker but task is not send to worker, and cannot find task in the worker state"""

        # TODO: this function should return things that expose 3 kinds of information:
        # TODO: 1. worker id as bytes if have capacity and able to assign to worker id
        # TODO: 2. capacity is full, and unable to add new task
        # TODO: 3. capacity is not full, but all the workers are busy right now, so tasks will be queued
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, task_cancel: TaskCancel) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker_id: WorkerID, info: WorkerHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_shutdown(self, client_id: ClientID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_disconnect_notification(self, worker_id: WorkerID, notification: WorkerDisconnectNotification):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> set[WorkerID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_workers_by_manager_id(self, manager_id: bytes) -> list[WorkerID]:
        """get all worker ids belonging to a specific worker manager"""
        raise NotImplementedError()


class InformationController(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_request(self, request: InformationRequest):
        raise NotImplementedError()


class PolicyController(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: WorkerID, capabilities: dict[str, int], queue_size: int) -> bool:
        """add worker to worker collection"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: WorkerID) -> list[TaskID]:
        """remove worker to worker collection, and return list of task_ids of removed worker"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> set[WorkerID]:
        """get all worker ids as list"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        """get worker that been assigned to this task_id, return an invalid worker ID if it cannot find the worker
        assigned to this task id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def balance(self) -> dict[WorkerID, list[TaskID]]:
        """balance worker, it should return list of task ids for over burdened worker, represented as worker
        identity to list of task ids dictionary"""
        raise NotImplementedError()

    @abc.abstractmethod
    def assign_task(self, task: Task) -> WorkerID:
        """assign task in allocator, return an invalid worker ID if available worker, otherwise will return worker been
        assigned to"""
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_task(self, task_id: TaskID) -> WorkerID:
        """remove task in allocator, return an invalid worker ID if it did not find any worker, otherwise will return
        worker associate with the removed task_id"""
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self, capabilities: Optional[dict[str, int]] = None) -> bool:
        """has available worker or not, possibly constrained to the requested task capabilities"""
        raise NotImplementedError()

    @abc.abstractmethod
    def statistics(self) -> dict:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        """Pure function: state in, commands out (a single declarative setDesiredTaskConcurrency)."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_scaling_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        """Pure function: state in, status out."""
        raise NotImplementedError()
