from typing import Optional

from scaler.protocol.capnp import ScalingManagerStatus, Task, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.mixins import PolicyController
from scaler.scheduler.controllers.policies.library.utility import create_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.snapshot import InformationSnapshot


class VanillaPolicyController(PolicyController):
    def __init__(self, policy_engine_type: str, policy_content: str):
        self._policy = create_policy(policy_engine_type, policy_content)

    def add_worker(self, worker: WorkerID, capabilities: dict[str, int], queue_size: int) -> bool:
        return self._policy.add_worker(worker, capabilities, queue_size)

    def remove_worker(self, worker: WorkerID) -> list[TaskID]:
        return self._policy.remove_worker(worker)

    def get_worker_ids(self) -> set[WorkerID]:
        return self._policy.get_worker_ids()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._policy.get_worker_by_task_id(task_id)

    def balance(self) -> dict[WorkerID, list[TaskID]]:
        return self._policy.balance()

    def assign_task(self, task: Task) -> WorkerID:
        return self._policy.assign_task(task)

    def remove_task(self, task_id: TaskID) -> WorkerID:
        return self._policy.remove_task(task_id)

    def has_available_worker(self, capabilities: Optional[dict[str, int]] = None) -> bool:
        return self._policy.has_available_worker(capabilities)

    def statistics(self) -> dict:
        return self._policy.statistics()

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        return self._policy.get_scaling_commands(
            information_snapshot, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
        )

    def get_scaling_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        return self._policy.get_scaling_status(managed_workers)
