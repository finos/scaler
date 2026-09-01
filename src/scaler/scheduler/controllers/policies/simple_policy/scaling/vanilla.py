import logging
from math import ceil

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import (
    build_scaling_manager_status,
    build_set_desired_command,
    forget_departed_managers,
)
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot

logger = logging.getLogger(__name__)


class VanillaScalingPolicy(ScalingPolicy):
    """
    Scaling policy that scales workers based on task-to-worker ratio.

    Decisions depend only on the current observation. The last count logged per manager is kept for logging.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10
        self._logged_desired_by_manager: dict[bytes, int] = {}

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        # the manager being answered counts as live even if no snapshot was built for it
        live_manager_ids = set(worker_manager_snapshots) | {worker_manager_heartbeat.workerManagerID}
        forget_departed_managers(self._logged_desired_by_manager, live_manager_ids)

        desired = self._compute_desired_worker_count(information_snapshot, worker_manager_heartbeat, managed_worker_ids)
        desired_per_capset: list[tuple[dict[str, int], int]] = [({}, desired)]
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _compute_desired_worker_count(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
    ) -> int:
        """Compute the target worker count for this manager from current task and worker observations."""
        current = len(managed_worker_ids)
        task_count = len(information_snapshot.tasks)
        worker_count = len(information_snapshot.workers)

        if worker_count == 0:
            desired = current + 1 if task_count > 0 else current
        else:
            task_ratio = task_count / worker_count
            if task_ratio > self._upper_task_ratio:
                desired = current + 1
            elif task_ratio < self._lower_task_ratio:
                desired = 0 if task_count == 0 else max(1, ceil(task_count / self._upper_task_ratio))
            else:
                desired = current

        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        if max_concurrency != -1:
            desired = min(desired, max_concurrency)

        desired = max(0, desired)
        self.__log_decision(worker_manager_heartbeat.workerManagerID, task_count, worker_count, current, desired)
        return desired

    def __log_decision(self, manager_id: bytes, task_count: int, worker_count: int, current: int, desired: int) -> None:
        """Logs a request to change a manager's worker count, once per run of that same request.

        The request repeats every heartbeat until the manager reaches the count, and forever if it cannot.
        """

        if desired == current:
            self._logged_desired_by_manager.pop(manager_id, None)
            return

        if self._logged_desired_by_manager.get(manager_id) == desired:
            return

        self._logged_desired_by_manager[manager_id] = desired
        logger.info(
            f"scaling {manager_id!r}: tasks={task_count}, workers={worker_count}, current={current} -> "
            f"desired={desired}"
        )
