import abc
from typing import Optional

from scaler.protocol.capnp import ScalingManagerStatus, Task, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.snapshot import InformationSnapshot


class ScalerPolicy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: WorkerID, capabilities: dict[str, int], queue_size: int) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_worker(self, worker: WorkerID) -> list[TaskID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> set[WorkerID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def balance(self) -> dict[WorkerID, list[TaskID]]:
        raise NotImplementedError()

    @abc.abstractmethod
    def assign_task(self, task: Task) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_task(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self, capabilities: Optional[dict[str, int]] = None) -> bool:
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
        raise NotImplementedError()

    @abc.abstractmethod
    def get_scaling_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        raise NotImplementedError()
