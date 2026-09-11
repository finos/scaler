import logging
import time
from typing import Optional

from scaler.config.defaults import DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
from scaler.io.mixins import AsyncBinder
from scaler.protocol.capnp import (
    ScalingManagerStatus,
    WorkerManagerCommand,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import PolicyController, TaskController, WorkerController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status
from scaler.utility.identifiers import WorkerID
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.snapshot import InformationSnapshot

logger = logging.getLogger(__name__)

UINT16_MAX = 2**16 - 1


class WorkerManagerController(Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, policy_controller: PolicyController):
        self._config_controller = config_controller
        self._policy_controller = policy_controller

        self._binder: Optional[AsyncBinder] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None

        # Track worker manager heartbeats: source -> (last_seen_time, heartbeat)
        self._manager_alive_since: dict[bytes, tuple[float, WorkerManagerHeartbeat]] = {}

        # Reverse map: worker_manager_id -> source (for duplicate detection)
        self._manager_id_to_source: dict[bytes, bytes] = {}

        # Per-manager total desired worker count from the latest setDesiredTaskConcurrency command.
        # Used to report pendingWorkers = max(0, last_desired - connected_count) to the monitor.
        self._last_desired_total: dict[bytes, int] = {}

    def register(self, binder: AsyncBinder, task_controller: TaskController, worker_controller: WorkerController):
        self._binder = binder
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    async def on_heartbeat(self, source: bytes, heartbeat: WorkerManagerHeartbeat):
        heartbeat.capabilities = capabilities_to_dict(heartbeat.capabilities)
        if source not in self._manager_alive_since:
            manager_id = heartbeat.workerManagerID
            existing_source = self._manager_id_to_source.get(manager_id)
            if existing_source is not None and existing_source != source:
                logger.warning(
                    f"Duplicate worker_manager_id {manager_id!r}: source {source!r} rejected, "
                    f"already registered by source {existing_source!r}"
                )
                return
            self._manager_id_to_source[manager_id] = source

            logger.info(f"WorkerManager {manager_id!r} connected")

        self._manager_alive_since[source] = (time.time(), heartbeat)

        await self._binder.send(source, WorkerManagerHeartbeatEcho(), detached=True)

        information_snapshot = self._build_snapshot()
        managed_worker_ids = self._worker_controller.get_workers_by_manager_id(heartbeat.workerManagerID)
        worker_manager_snapshots = self._build_manager_snapshots()

        commands = self._policy_controller.get_scaling_commands(
            information_snapshot, heartbeat, managed_worker_ids, worker_manager_snapshots
        )

        for command in commands:
            await self._send_command(source, command)

        self._last_desired_total[source] = _sum_desired_for_manager(commands, heartbeat.capabilities)

    async def routine(self):
        await self._clean_managers()

    def get_status(self) -> ScalingManagerStatus:
        managed_workers = self.get_managed_workers()

        now = time.time()
        details = []
        for source, (last_seen, heartbeat) in self._manager_alive_since.items():
            caps = heartbeat.capabilities
            caps_str = " ".join(sorted(capabilities_to_dict(caps).keys())) if caps else ""
            connected = len(self._worker_controller.get_workers_by_manager_id(heartbeat.workerManagerID))
            pending = max(0, self._last_desired_total.get(source, 0) - connected)
            details.append(
                {
                    "worker_manager_id": heartbeat.workerManagerID,
                    "identity": source.decode(errors="replace"),
                    "last_seen_s": min(int(now - last_seen), UINT16_MAX),
                    "max_task_concurrency": heartbeat.maxTaskConcurrency,
                    "capabilities": caps_str,
                    "pending_workers": pending,
                }
            )

        return build_scaling_manager_status(managed_workers, details)

    def get_managed_workers(self) -> dict[bytes, list[WorkerID]]:
        """Return managed workers keyed by worker_manager_id (from heartbeat)."""
        result: dict[bytes, list[WorkerID]] = {}
        for source, (_, heartbeat) in self._manager_alive_since.items():
            manager_id = heartbeat.workerManagerID
            result[manager_id] = self._worker_controller.get_workers_by_manager_id(manager_id)
        return result

    async def _send_command(self, source: bytes, command: WorkerManagerCommand):
        await self._binder.send(source, command, detached=True)

    def _build_manager_snapshots(self) -> dict[bytes, WorkerManagerSnapshot]:
        """Build cross-manager snapshots from all known managers, keyed by worker_manager_id."""
        snapshots: dict[bytes, WorkerManagerSnapshot] = {}
        for source, (last_seen, heartbeat) in self._manager_alive_since.items():
            manager_id = heartbeat.workerManagerID
            worker_count = len(self._worker_controller.get_workers_by_manager_id(manager_id))
            snapshots[manager_id] = WorkerManagerSnapshot(
                worker_manager_id=manager_id,
                max_task_concurrency=heartbeat.maxTaskConcurrency,
                worker_count=worker_count,
                last_seen_s=last_seen,
                capabilities=heartbeat.capabilities,
            )
        return snapshots

    def _build_snapshot(self) -> InformationSnapshot:
        tasks = self._task_controller._task_id_to_task  # type: ignore # noqa
        workers = {
            worker_id: worker_heartbeat
            for worker_id, (
                _,
                worker_heartbeat,
            ) in self._worker_controller._worker_alive_since.items()  # type: ignore # noqa
        }
        return InformationSnapshot(tasks=tasks, workers=workers)

    async def _clean_managers(self):
        """Clean up dead worker managers that have not sent heartbeats."""
        now = time.time()
        timeout_seconds = DEFAULT_WORKER_MANAGER_TIMEOUT_SECONDS
        dead_managers = [
            source
            for source, (alive_since, _) in self._manager_alive_since.items()
            if now - alive_since > timeout_seconds
        ]
        for dead_manager in dead_managers:
            await self._disconnect_manager(dead_manager)

    async def _disconnect_manager(self, source: bytes):
        if source not in self._manager_alive_since:
            return

        _, heartbeat = self._manager_alive_since[source]
        manager_id = heartbeat.workerManagerID
        self._manager_id_to_source.pop(manager_id, None)

        logger.info(f"WorkerManager {source!r} disconnected")
        self._manager_alive_since.pop(source)
        self._last_desired_total.pop(source, None)


def _sum_desired_for_manager(commands: list[WorkerManagerCommand], manager_capabilities: dict[str, int]) -> int:
    """Sum taskConcurrency across requests whose capability set is a subset of the manager's capabilities.

    Mirrors the rule the worker manager itself applies to translate the declarative command into a
    concrete worker count. An empty capability set on a request acts as a wildcard.
    """
    manager_items = manager_capabilities.items()
    total = 0
    for command in commands:
        for request in getattr(command, "setDesiredTaskConcurrencyRequests", ()):
            req_caps = capabilities_to_dict(request.capabilities)
            if req_caps.items() <= manager_items:
                total += request.taskConcurrency
    return total
