import abc
from typing import Optional

from scaler.protocol.capnp import Task
from scaler.utility.identifiers import TaskID, WorkerID


class TaskAllocatePolicy(metaclass=abc.ABCMeta):
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
