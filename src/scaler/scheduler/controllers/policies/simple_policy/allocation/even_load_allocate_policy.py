import logging
import math
from typing import Dict, List, Optional, Set, Tuple, cast

from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.policies.simple_policy.allocation.mixins import TaskAllocatePolicy
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.metadata.task_flags import is_nested_task
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.queues.indexed_queue import IndexedQueue

logger = logging.getLogger(__name__)


class EvenLoadAllocatePolicy(TaskAllocatePolicy):
    """This Allocator policy is trying to make all workers load as equal as possible"""

    def __init__(self):
        self._workers_to_queue_size: Dict[bytes, int] = dict()
        self._workers_to_task_ids: Dict[WorkerID, IndexedQueue] = dict()
        self._task_id_to_worker: Dict[TaskID, WorkerID] = {}

        self._worker_queue: AsyncPriorityQueue = AsyncPriorityQueue()

    def add_worker(self, worker: WorkerID, capabilities: Dict[str, int], queue_size: int) -> bool:
        # TODO: handle uneven queue size for each worker
        if worker in self._workers_to_task_ids:
            return False

        if len(capabilities) > 0:
            logger.warning(f"allocate policy ignores worker capabilities: {capabilities!r}.")

        self._workers_to_task_ids[worker] = IndexedQueue()
        self._workers_to_queue_size[worker] = queue_size

        self._worker_queue.put_nowait([0, worker])
        return True

    def remove_worker(self, worker: WorkerID) -> List[TaskID]:
        if worker not in self._workers_to_task_ids:
            return []

        self._worker_queue.remove(worker)
        self._workers_to_queue_size.pop(worker, None)

        task_ids = self._workers_to_task_ids.pop(worker).to_list()
        for task_id in task_ids:
            self._task_id_to_worker.pop(task_id)
        return task_ids

    def get_worker_ids(self) -> Set[WorkerID]:
        return set(self._workers_to_task_ids.keys())

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._task_id_to_worker.get(task_id, WorkerID.invalid_worker_id())

    def balance(self) -> Dict[WorkerID, List[TaskID]]:
        """Returns, for every worker, the list of tasks to balance out."""

        # TODO: handle uneven queue size for each worker
        balance_count = self.__get_balance_count_by_worker()

        balance_result = {}

        for worker, count in balance_count.items():
            if count == 0:
                continue

            tasks = self._workers_to_task_ids[worker].to_list()
            balance_result[worker] = tasks[-count:]  # balance out the most recently queued tasks

        return balance_result

    def __get_balance_count_by_worker(self) -> Dict[WorkerID, int]:
        """Returns, for every worker, the number of tasks to balance out."""

        queued_tasks_per_worker = {
            worker: max(0, len(tasks) - 1) for worker, tasks in self._workers_to_task_ids.items()
        }

        any_worker_has_queued_task = any(queued_tasks_per_worker.values())

        if not any_worker_has_queued_task:
            return {}

        number_of_idle_workers = sum(1 for tasks in self._workers_to_task_ids.values() if len(tasks) == 0)

        if number_of_idle_workers == 0:
            return {}

        mean_queued = math.ceil(sum(queued_tasks_per_worker.values()) / len(queued_tasks_per_worker))

        balance_count = {worker: max(0, count - mean_queued) for worker, count in queued_tasks_per_worker.items()}

        over_mean_advice_total = sum(balance_count.values())
        minimal_allocate = min(number_of_idle_workers, sum(queued_tasks_per_worker.values()))

        if over_mean_advice_total >= minimal_allocate:
            return balance_count

        total_to_be_balance = minimal_allocate - over_mean_advice_total
        for worker, count in queued_tasks_per_worker.items():
            assert total_to_be_balance >= 0, "total_to_be_balance must be positive"
            if total_to_be_balance == 0:
                break

            leftover = count - balance_count[worker]
            if leftover < 1:
                continue

            to_to_balance = min(leftover, total_to_be_balance)
            balance_count[worker] += to_to_balance
            total_to_be_balance -= to_to_balance

        return balance_count

    def assign_task(self, task: Task) -> WorkerID:
        if len(task.capabilities) > 0:
            logger.warning(f"allocate policy ignores task capabilities: {task.capabilities!r}.")

        task_id = task.taskId

        if task_id in self._task_id_to_worker:
            return self._task_id_to_worker[task_id]

        if self._worker_queue.empty():
            return WorkerID.invalid_worker_id()

        count, worker = self._worker_queue.get_nowait()

        # A nested task is exempt from the per-worker queue size. The queue size is admission control for new
        # work, but a nested task is not new work: a task that already holds a queue slot is blocked until it
        # finishes. Turning it away when every queue is full deadlocks the cluster, because the slots can only
        # be released by the very tasks that are waiting. It does not add concurrency either -- the worker
        # suspends the lower-priority parent to run it.
        if count >= self._workers_to_queue_size[worker] and not is_nested_task(task):
            self._worker_queue.put_nowait([count, worker])
            return WorkerID.invalid_worker_id()

        self._workers_to_task_ids[worker].put(task_id)
        self._task_id_to_worker[task_id] = worker
        self._worker_queue.put_nowait([count + 1, worker])
        return worker

    def remove_task(self, task_id: TaskID) -> WorkerID:
        if task_id not in self._task_id_to_worker:
            return WorkerID.invalid_worker_id()

        worker = self._task_id_to_worker.pop(task_id)
        self._workers_to_task_ids[worker].remove(task_id)

        self._worker_queue.decrease_priority(worker)
        return worker

    def has_available_worker(self, capabilities: Optional[Dict[str, int]] = None) -> bool:
        if not len(self._worker_queue):
            return False

        # this queue's priorities are the workers' queued task counts, which a nested task can push past
        # the queue size
        count, worker = cast(Tuple[int, WorkerID], self._worker_queue.max_priority_item())
        if count >= self._workers_to_queue_size[worker]:
            return False

        return True

    def statistics(self) -> Dict:
        return {
            # a nested task can push a worker past its queue size
            worker: {"free": max(0, self._workers_to_queue_size[worker] - len(tasks)), "sent": len(tasks)}
            for worker, tasks in self._workers_to_task_ids.items()
        }
