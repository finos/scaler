import abc

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class ScalingPolicy:
    """
    Stateless scaling policy interface.

    All state (managed workers) is owned by WorkerManagerController and passed in as parameters.
    Policies return a single declarative setDesiredTaskConcurrency command.
    """

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        """Pure function: state in, declarative scaling command out."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        """Pure function: state in, status out."""
        raise NotImplementedError()
