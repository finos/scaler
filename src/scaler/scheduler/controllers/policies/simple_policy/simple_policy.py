from typing import Optional

from scaler.protocol.capnp import ScalingManagerStatus, Task, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.mixins import ScalerPolicy
from scaler.scheduler.controllers.policies.simple_policy.allocation.mixins import TaskAllocatePolicy
from scaler.scheduler.controllers.policies.simple_policy.allocation.types import AllocatePolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.allocation.utility import create_allocate_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    ScalingPolicyStrategy,
    WorkerManagerSnapshot,
)
from scaler.scheduler.controllers.policies.simple_policy.scaling.utility import create_scaling_policy
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.snapshot import InformationSnapshot


class SimplePolicy(ScalerPolicy):
    def __init__(self, policy_content: str):
        policy_kv = {
            k.strip(): v.strip() for item in policy_content.split(";") if "=" in item for k, v in [item.split("=", 1)]
        }

        required_keys = {"allocate", "scaling"}
        if policy_kv.keys() != required_keys:
            raise ValueError(f"simple policy_content requires keys {required_keys}, got {set(policy_kv.keys())}")

        self._allocation_policy: TaskAllocatePolicy = create_allocate_policy(
            AllocatePolicyStrategy(policy_kv["allocate"])
        )
        self._scaling_policy: ScalingPolicy = create_scaling_policy(ScalingPolicyStrategy(policy_kv["scaling"]))

    def add_worker(self, worker: WorkerID, capabilities: dict[str, int], queue_size: int) -> bool:
        return self._allocation_policy.add_worker(worker, capabilities, queue_size)

    def remove_worker(self, worker: WorkerID) -> list[TaskID]:
        return self._allocation_policy.remove_worker(worker)

    def get_worker_ids(self) -> set[WorkerID]:
        return self._allocation_policy.get_worker_ids()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._allocation_policy.get_worker_by_task_id(task_id)

    def balance(self) -> dict[WorkerID, list[TaskID]]:
        return self._allocation_policy.balance()

    def assign_task(self, task: Task) -> WorkerID:
        return self._allocation_policy.assign_task(task)

    def remove_task(self, task_id: TaskID) -> WorkerID:
        return self._allocation_policy.remove_task(task_id)

    def has_available_worker(self, capabilities: Optional[dict[str, int]] = None) -> bool:
        return self._allocation_policy.has_available_worker(capabilities)

    def statistics(self) -> dict:
        return self._allocation_policy.statistics()

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        return self._scaling_policy.get_scaling_commands(
            information_snapshot, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
        )

    def get_scaling_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        return self._scaling_policy.get_status(managed_workers)
