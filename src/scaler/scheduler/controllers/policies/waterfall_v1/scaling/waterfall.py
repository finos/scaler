import logging
from math import ceil
from typing import Optional

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot

logger = logging.getLogger(__name__)


class WaterfallScalingPolicy(ScalingPolicy):
    """
    Stateless declarative scaling policy that cascades worker allocation across prioritized
    worker managers.

    For each capability set (including the generic empty set) the total target worker count
    is computed from observed task volume, then assigned to managers in priority order.
    Higher-priority managers absorb capacity first; overflow spills to the next priority.

    Capability-aware allocation: only managers whose advertised capabilities are a superset
    of a task capset participate in that capset's allocation chain.
    """

    def __init__(self, rules: list[WaterfallRule]):
        self._rules = sorted(rules, key=lambda r: r.priority)
        self._rule_by_manager_id: dict[bytes, WaterfallRule] = {r.worker_manager_id: r for r in self._rules}
        # Scale up when tasks/workers > 10 (tasks significantly outnumber workers, overloaded)
        self._upper_task_ratio = 10

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: list[WorkerID],
        worker_manager_snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[WorkerManagerCommand]:
        manager_id = worker_manager_heartbeat.workerManagerID
        rule = self._find_rule(manager_id)

        if rule is None:
            logger.warning("Worker manager %r not found in waterfall rules, skipping scaling", manager_id)
            return []

        desired_per_capset = self._compute_desired_per_capset(
            rule, information_snapshot, worker_manager_heartbeat, worker_manager_snapshots
        )
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: dict[bytes, list[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _find_rule(self, manager_id: bytes) -> Optional[WaterfallRule]:
        """Find the rule whose worker manager ID matches *manager_id*."""
        return self._rule_by_manager_id.get(manager_id)

    def _find_matching_snapshot(
        self, rule: WaterfallRule, snapshots: dict[bytes, WorkerManagerSnapshot]
    ) -> Optional[WorkerManagerSnapshot]:
        """Return the manager snapshot matching *rule*'s worker manager ID, or None."""
        return snapshots.get(rule.worker_manager_id)

    def _compute_desired_per_capset(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> list[tuple[dict[str, int], int]]:
        """Compute desired worker count per capability set for this manager only.

        Generic (empty-cap) tasks fill higher-priority managers first; this manager's share is
        whatever overflows from higher priorities, clamped by its effective capacity.

        Capability-specific requests are emitted only for capsets this manager owns: the
        highest-priority manager whose advertised capabilities are a superset of the capset.
        """
        result: list[tuple[dict[str, int], int]] = []

        generic_desired = self._allocate_generic_desired(
            current_rule, information_snapshot, current_heartbeat, snapshots
        )
        result.append(({}, generic_desired))

        for required_keys, capability_dict in self._tasks_by_capability(information_snapshot).items():
            cap_desired = self._allocate_capset_desired(
                current_rule, required_keys, information_snapshot, current_heartbeat, snapshots
            )
            if cap_desired > 0:
                result.append((capability_dict, cap_desired))

        return result

    def _allocate_generic_desired(
        self,
        current_rule: WaterfallRule,
        information_snapshot: InformationSnapshot,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> int:
        """Allocate this manager's share of the cluster-wide generic worker target."""
        task_count = len(information_snapshot.tasks)
        if task_count == 0:
            # Drain lower-priority managers first: if any lower-priority manager still has
            # connected workers, this manager holds its current count rather than draining.
            for rule in self._rules:
                if rule.priority <= current_rule.priority:
                    continue
                snap = snapshots.get(rule.worker_manager_id)
                if snap is not None and snap.worker_count > 0:
                    own_snap = snapshots.get(current_rule.worker_manager_id)
                    return own_snap.worker_count if own_snap is not None else 0
            return 0
        total_desired = max(1, ceil(task_count / self._upper_task_ratio))

        remaining = total_desired
        for rule in self._rules:
            cap = self._effective_capacity(rule, current_rule, current_heartbeat, snapshots)
            if cap is None:
                continue
            if rule.worker_manager_id == current_rule.worker_manager_id:
                return min(remaining, cap)
            remaining = max(0, remaining - cap)
        return 0

    def _allocate_capset_desired(
        self,
        current_rule: WaterfallRule,
        required_keys: frozenset[str],
        information_snapshot: InformationSnapshot,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> int:
        """Allocate this manager's share of the target for one capability set."""
        task_count = sum(
            1 for task in information_snapshot.tasks.values() if frozenset(task.capabilities.keys()) == required_keys
        )
        if task_count == 0:
            return 0
        total_desired = max(1, ceil(task_count / self._upper_task_ratio))

        remaining = total_desired
        for rule in self._rules:
            snap = self._find_matching_snapshot(rule, snapshots)
            if rule.worker_manager_id == current_rule.worker_manager_id:
                manager_capabilities = frozenset(current_heartbeat.capabilities.keys())
            elif snap is not None:
                manager_capabilities = frozenset(snap.capabilities.keys())
            else:
                continue
            if not required_keys <= manager_capabilities:
                continue

            cap = self._effective_capacity(rule, current_rule, current_heartbeat, snapshots)
            if cap is None:
                continue
            if rule.worker_manager_id == current_rule.worker_manager_id:
                return min(remaining, cap)
            remaining = max(0, remaining - cap)
        return 0

    def _effective_capacity(
        self,
        rule: WaterfallRule,
        current_rule: WaterfallRule,
        current_heartbeat: WorkerManagerHeartbeat,
        snapshots: dict[bytes, WorkerManagerSnapshot],
    ) -> Optional[int]:
        """Return effective worker capacity for *rule*.

        When the rule specifies a cap, returns min(cap, heartbeat max). When omitted, returns the
        heartbeat-reported max directly. Returns None if the manager is offline.
        """
        if rule.worker_manager_id == current_rule.worker_manager_id:
            reported = current_heartbeat.maxTaskConcurrency
            return min(rule.max_task_concurrency, reported) if rule.max_task_concurrency is not None else reported
        snap = self._find_matching_snapshot(rule, snapshots)
        if snap is None:
            return None
        return (
            min(rule.max_task_concurrency, snap.max_task_concurrency)
            if rule.max_task_concurrency is not None
            else snap.max_task_concurrency
        )

    def _tasks_by_capability(self, information_snapshot: InformationSnapshot) -> dict[frozenset[str], dict[str, int]]:
        """Group tasks with non-empty capabilities, mapping capset keys to a representative dict."""
        result: dict[frozenset[str], dict[str, int]] = {}
        for task in information_snapshot.tasks.values():
            if not task.capabilities:
                continue
            required_keys = frozenset(task.capabilities.keys())
            if required_keys not in result:
                result[required_keys] = dict(task.capabilities)
        return result
