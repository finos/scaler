
from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class NoScalingPolicy(ScalingPolicy):
    def __init__(self):
        pass

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        return []

    def get_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)
