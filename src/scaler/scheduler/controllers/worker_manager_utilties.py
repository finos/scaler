from typing import Iterable, Optional, TypeVar

from scaler.protocol import capnp
from scaler.protocol.capnp import ScalingManagerStatus, TaskCapability, WorkerManagerCommand

StateT = TypeVar("StateT")


def forget_departed_managers(state_by_manager: dict[bytes, StateT], live_manager_ids: Iterable[bytes]) -> None:
    """Drops per-manager state for managers the scheduler no longer knows about.

    A policy is only ever told about a manager that is heartbeating, so anything it remembers per manager id
    otherwise stays for the lifetime of the scheduler -- growing without bound wherever ids are not reused
    across restarts, e.g. one derived from a pod or instance name.
    """

    live_ids = set(live_manager_ids)
    for departed_id in [manager_id for manager_id in state_by_manager if manager_id not in live_ids]:
        del state_by_manager[departed_id]


def build_scaling_manager_status(
    managed_workers: dict[bytes, list], worker_manager_details: Optional[list[dict]] = None
) -> ScalingManagerStatus:
    details = worker_manager_details or []
    return capnp.ScalingManagerStatus(
        managedWorkers=[
            capnp.ScalingManagerStatus.Pair(
                workerManagerID=worker_manager_id, workerIDs=[bytes(worker_id) for worker_id in worker_ids]
            )
            for worker_manager_id, worker_ids in managed_workers.items()
        ],
        workerManagerDetails=[
            capnp.ScalingManagerStatus.WorkerManagerDetail(
                workerManagerID=d["worker_manager_id"],
                identity=d["identity"],
                lastSeenS=d["last_seen_s"],
                maxTaskConcurrency=d["max_task_concurrency"],
                capabilities=d.get("capabilities", ""),
                pendingWorkers=d.get("pending_workers", 0),
            )
            for d in details
        ],
    )


def build_set_desired_command(desired_per_capset: list[tuple[dict[str, int], int]]) -> WorkerManagerCommand:
    """Build a declarative setDesiredTaskConcurrency command.

    Each entry in desired_per_capset maps a capability set (as dict[str, int]) to a
    desired worker count. An empty list is valid and yields a command whose requests
    list is empty (declarative "no opinion").
    """
    requests = [
        WorkerManagerCommand.DesiredTaskConcurrencyRequest(
            taskConcurrency=max(0, count),
            capabilities=[TaskCapability(name=name, value=value) for name, value in caps.items()],
        )
        for caps, count in desired_per_capset
    ]
    return WorkerManagerCommand(setDesiredTaskConcurrencyRequests=requests)
