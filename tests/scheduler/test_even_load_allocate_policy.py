import unittest

from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.policies.simple_policy.allocation.even_load_allocate_policy import (
    EvenLoadAllocatePolicy,
)
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import TaskFlags
from tests.utility.utility import logging_test_name

MAX_TASKS_PER_WORKER = 10


class TestEvenLoadAllocatePolicy(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_nested_task_is_assigned_when_every_queue_is_full(self):
        """A nested task must be assignable even when every worker's queue is full.

        Its parent already occupies one of those queue slots and cannot release it until the nested task
        runs, so refusing the nested task deadlocks the cluster.
        """
        allocator = EvenLoadAllocatePolicy()

        allocator.add_worker(WorkerID(b"worker_1"), {}, MAX_TASKS_PER_WORKER)
        allocator.add_worker(WorkerID(b"worker_2"), {}, MAX_TASKS_PER_WORKER)

        for i in range(MAX_TASKS_PER_WORKER * 2):
            self.assertTrue(allocator.assign_task(self.__create_task(TaskID(f"task_{i}".encode()))).is_valid())

        self.assertFalse(allocator.has_available_worker())

        # A regular task is turned away, and waits in the scheduler's unassigned queue.
        self.assertFalse(allocator.assign_task(self.__create_task(TaskID(b"task_top_level"))).is_valid())

        # A nested one is admitted anyway.
        nested_task = self.__create_task(TaskID(b"task_nested"), priority=1)
        assigned_worker = allocator.assign_task(nested_task)
        self.assertTrue(assigned_worker.is_valid())
        self.assertEqual(allocator.get_worker_by_task_id(nested_task.taskId), assigned_worker)

        # The over-committed worker reports no free slots rather than a negative count, and the pool still
        # refuses regular tasks.
        self.assertEqual(allocator.statistics()[assigned_worker]["free"], 0)
        self.assertFalse(allocator.has_available_worker())
        self.assertFalse(allocator.assign_task(self.__create_task(TaskID(b"task_top_level_2"))).is_valid())

    @staticmethod
    def __create_task(task_id: TaskID, priority: int = 0) -> Task:
        return Task(
            taskId=task_id,
            source=ClientID(b"client_id"),
            metadata=TaskFlags(priority=priority).serialize(),
            funcObjectId=b"",
            functionArgs=[],
            capabilities={},
        )
