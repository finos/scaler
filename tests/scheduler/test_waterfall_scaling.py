import asyncio
import time
import unittest
from typing import Optional

from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat, WorkerManagerHeartbeat
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.policies.library.utility import create_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.utility import parse_waterfall_rules
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.waterfall import WaterfallScalingPolicy
from scaler.scheduler.controllers.policies.waterfall_v1.waterfall_v1_policy import WaterfallV1Policy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.snapshot import InformationSnapshot


def _generic_request(command):
    """Return the empty-capabilities (generic) DesiredTaskConcurrencyRequest from a command."""
    for r in command.setDesiredTaskConcurrencyRequests:
        if not capabilities_to_dict(r.capabilities):
            return r
    return None


def _capability_requests(command):
    """Return only capability-bearing requests as (caps_dict, taskConcurrency) tuples."""
    return [
        (capabilities_to_dict(r.capabilities), r.taskConcurrency)
        for r in command.setDesiredTaskConcurrencyRequests
        if capabilities_to_dict(r.capabilities)
    ]


class TestWaterfallScalingPolicy(unittest.TestCase):
    """Unit tests for declarative WaterfallScalingPolicy emission."""

    def setUp(self):
        setup_logger()
        self.rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=20),
        ]
        self.policy = WaterfallScalingPolicy(self.rules)

    def test_unknown_manager_emits_no_commands(self):
        """Manager with unknown worker_manager_id receives no scaling commands."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {b"unknown": _create_manager_snapshot(b"unknown", max_task_concurrency=10, worker_count=0)}
        heartbeat = _create_worker_manager_heartbeat(b"unknown", max_task_concurrency=10)

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(len(commands), 0)

    def test_higher_priority_takes_full_load_when_within_capacity(self):
        """When tasks fit in the higher-priority manager's capacity, lower-priority desired is 0."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0),
        }

        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = self.policy.get_scaling_commands(snapshot, heartbeat_a, [], manager_snapshots)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 1)

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = self.policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)
        # Manager B's share is 0 (chain consumes the whole 1 worker at manager_a); emits setDesired(0).
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

    def test_overflow_to_lower_priority_when_higher_at_capacity(self):
        """When higher priority is at full capacity, the overflow lands on lower-priority desired."""
        # Many tasks (50) so total_desired = ceil(50/10) = 5 spread across managers.
        tasks = _create_tasks(150)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        # manager_a is full so its capacity (10) is consumed; remainder (5) goes to manager_b.
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=10),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0),
        }

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = self.policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)

        # total_desired = ceil(150/10) = 15. Manager_a's cap is 10 -> consumed 10. Remaining for B = 5.
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 5)

    def test_higher_priority_offline_routes_load_to_lower(self):
        """When the higher-priority manager is offline, a lower-priority manager picks up the load."""
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0)
        }

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = self.policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)

        # Higher-priority is offline (absent) so its capacity contributes 0; lower picks up all of it.
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 1)

    def test_no_tasks_emits_desired_zero(self):
        """No tasks: desired is 0 -> policy unconditionally emits setDesired(0)."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_snapshots = {b"manager_a": _create_manager_snapshot(b"manager_a")}
        heartbeat = _create_worker_manager_heartbeat(b"manager_a")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(len(commands), 1)
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 0)

    def test_max_task_concurrency_clamps_via_heartbeat(self):
        """The manager's heartbeat-reported maxTaskConcurrency clamps its allocation."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=20)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(50)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=3, worker_count=3)
        }
        heartbeat = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=3)

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        # min(rule cap 20, heartbeat cap 3) = 3 clamps the desired worker count.
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 3)

    def test_same_priority_share_capacity(self):
        """Two managers at the same priority each absorb a portion of the desired load."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=b"manager_b", max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)
        # 100 tasks -> total_desired = 10
        tasks = _create_tasks(100)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=10, worker_count=0),
        }

        # Sorting by priority is stable; with equal priority the iteration order is the rules' input order.
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = policy.get_scaling_commands(snapshot, heartbeat_a, [], manager_snapshots)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 10)

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=10)
        commands_b = policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)
        # First manager consumed all of its capacity; nothing left for the second at same priority.
        # Desired is 0 -> emits setDesired(0).
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

    def test_lower_priority_drains_first(self):
        """When demand drops to zero, the lower-priority manager drains first;
        the higher-priority manager holds its workers until the lower-priority manager is empty."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=3),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=2),
        }

        # manager_b (lower priority) is told to drain to 0
        managed_b = [WorkerID(b"worker-3"), WorkerID(b"worker-4")]
        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = policy.get_scaling_commands(snapshot, heartbeat_b, managed_b, manager_snapshots)
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

        # manager_a (higher priority) holds its count (waiting for lower priority to drain) -> emits setDesired(3).
        managed_a = [WorkerID(b"worker-0"), WorkerID(b"worker-1"), WorkerID(b"worker-2")]
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = policy.get_scaling_commands(snapshot, heartbeat_a, managed_a, manager_snapshots)
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 3)

    def test_higher_priority_can_drain_when_lower_offline(self):
        """If the lower-priority manager is offline (not in snapshots), the higher-priority
        manager is allowed to drain immediately -- there is no lower priority to wait on."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        snapshot = InformationSnapshot(tasks={}, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=3)
        }

        managed_a = [WorkerID(b"worker-0"), WorkerID(b"worker-1"), WorkerID(b"worker-2")]
        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = policy.get_scaling_commands(snapshot, heartbeat_a, managed_a, manager_snapshots)

        # No lower-priority manager to wait on -- emit setDesired(0) to drain.
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 0)

    def test_unknown_manager_id_returns_no_commands(self):
        """A heartbeat from a worker manager whose ID is not in the waterfall rules
        receives no scaling commands at all (not even an empty setDesired)."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"unknown", max_task_concurrency=10)
        manager_snapshots = {b"unknown": _create_manager_snapshot(b"unknown", max_task_concurrency=10, worker_count=0)}

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(commands, [])

    def test_exact_matching_with_runtime_ids(self):
        """Worker manager IDs like NAT|12345 must match rules by exact bytes; a heartbeat
        from a manager whose ID matches a rule receives the allocation for that rule."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"NAT|12345", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"ECS|67890", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(50)  # ceil(50/10) = 5; first manager absorbs all of it
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"NAT|12345": _create_manager_snapshot(b"NAT|12345", max_task_concurrency=10, worker_count=0),
            b"ECS|67890": _create_manager_snapshot(b"ECS|67890", max_task_concurrency=20, worker_count=0),
        }

        # NAT manager gets the allocation since it has capacity and the priority chain
        # consumes it first.
        heartbeat_nat = _create_worker_manager_heartbeat(b"NAT|12345", max_task_concurrency=10)
        commands_nat = policy.get_scaling_commands(snapshot, heartbeat_nat, [], manager_snapshots)
        self.assertEqual(_generic_request(commands_nat[0]).taskConcurrency, 5)

        # ECS manager gets nothing since NAT has absorbed everything; desired=0 -> emits setDesired(0).
        heartbeat_ecs = _create_worker_manager_heartbeat(b"ECS|67890", max_task_concurrency=20)
        commands_ecs = policy.get_scaling_commands(snapshot, heartbeat_ecs, [], manager_snapshots)
        self.assertEqual(len(commands_ecs), 1)
        self.assertEqual(_generic_request(commands_ecs[0]).taskConcurrency, 0)

    def test_blocked_when_any_higher_priority_has_room(self):
        """When multiple worker managers share the same higher priority, the lower-priority
        manager is allocated nothing until *all* higher-priority capacity is consumed."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"NAT|111", max_task_concurrency=10),
            WaterfallRule(priority=1, worker_manager_id=b"NAT|222", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"ECS|333", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        # 5 tasks -> total_desired = ceil(5/10) = 1. Chain absorbs the 1 at NAT|111 entirely.
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"NAT|111": _create_manager_snapshot(b"NAT|111", max_task_concurrency=10, worker_count=10),
            b"NAT|222": _create_manager_snapshot(b"NAT|222", max_task_concurrency=10, worker_count=5),
            b"ECS|333": _create_manager_snapshot(b"ECS|333", max_task_concurrency=20, worker_count=0),
        }

        heartbeat_ecs = _create_worker_manager_heartbeat(b"ECS|333", max_task_concurrency=20)
        commands = policy.get_scaling_commands(snapshot, heartbeat_ecs, [], manager_snapshots)

        # ECS allocation = 0 (higher-priority chain absorbs all demand); emits setDesired(0).
        self.assertEqual(len(commands), 1)
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 0)

    def test_greedy_shutdown_partial_with_tasks(self):
        """With a small task count and many workers, the policy targets the ratio-based
        minimum and emits a setDesired with that smaller count to drain excess workers."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=20)]
        policy = WaterfallScalingPolicy(rules)
        # 5 tasks; ratio yields ceil(5/10) = 1 desired total.
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=20, worker_count=10)
        }
        managed = [WorkerID(f"worker-{i}".encode()) for i in range(10)]
        heartbeat = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=20)

        commands = policy.get_scaling_commands(snapshot, heartbeat, managed, manager_snapshots)

        self.assertEqual(len(commands), 1)
        self.assertEqual(_generic_request(commands[0]).taskConcurrency, 1)

    def test_declarative_desired_zero_on_non_owning_manager(self):
        """When the priority chain has already absorbed all demand, a lower-priority manager
        that still has connected workers is told setDesired(0) for the generic capset."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=20),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=3),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=2),
        }

        managed_b = [WorkerID(b"w0"), WorkerID(b"w1")]
        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = policy.get_scaling_commands(snapshot, heartbeat_b, managed_b, manager_snapshots)

        # 5 tasks -> total_desired=1, absorbed by manager_a entirely -> manager_b's share=0;
        # but manager_b has 2 connected workers, so the command is not a no-op.
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)


class TestWaterfallCapabilities(unittest.TestCase):
    """Capability-aware declarative emission for WaterfallScalingPolicy."""

    def setUp(self):
        setup_logger()

    def test_capable_manager_emits_capability_request(self):
        """A capable manager emits a per-capset DesiredTaskConcurrencyRequest with that capset."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_gpu", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"manager_gpu", capabilities={"gpu": 4})
        manager_snapshots = {
            b"manager_gpu": _create_manager_snapshot(b"manager_gpu", worker_count=0, capabilities={"gpu": 4})
        }

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        cap_requests = _capability_requests(commands[0])
        self.assertEqual(len(cap_requests), 1)
        self.assertEqual(cap_requests[0], ({"gpu": 1}, 1))

    def test_incapable_manager_emits_no_capability_request(self):
        """A manager that can't satisfy a capset emits no request for that capset."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_cpu", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"manager_cpu", capabilities={})
        manager_snapshots = {b"manager_cpu": _create_manager_snapshot(b"manager_cpu", worker_count=0, capabilities={})}

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(_capability_requests(commands[0]), [])

    def test_higher_priority_capable_owns_capset(self):
        """When two managers can satisfy the capset, only the higher-priority one allocates it."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", worker_count=0, capabilities={"gpu": 4}),
            b"manager_b": _create_manager_snapshot(b"manager_b", worker_count=0, capabilities={"gpu": 4}),
        }

        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", capabilities={"gpu": 4})
        commands_a = policy.get_scaling_commands(snapshot, heartbeat_a, [], manager_snapshots)
        self.assertEqual(_capability_requests(commands_a[0]), [({"gpu": 1}, 1)])

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", capabilities={"gpu": 4})
        commands_b = policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)
        # Higher-priority A owns the gpu capset; B's share is 0 -> emits setDesired(0) for generic capset.
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)
        self.assertEqual(_capability_requests(commands_b[0]), [])

    def test_capset_overflow_to_lower_priority_when_higher_full(self):
        """When the higher-priority capable manager is full for the capset, overflow goes to the next."""
        rules = [
            WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=2),
            WaterfallRule(priority=2, worker_manager_id=b"manager_b", max_task_concurrency=10),
        ]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(50, capabilities={"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(
                b"manager_a", max_task_concurrency=2, worker_count=2, capabilities={"gpu": 4}
            ),
            b"manager_b": _create_manager_snapshot(
                b"manager_b", max_task_concurrency=10, worker_count=0, capabilities={"gpu": 4}
            ),
        }

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", capabilities={"gpu": 4})
        commands_b = policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)

        # total_desired = ceil(50/10) = 5. manager_a cap=2 absorbs 2, manager_b allocates remaining 3.
        self.assertEqual(_capability_requests(commands_b[0]), [({"gpu": 1}, 3)])

    def test_capability_request_carries_concrete_dict(self):
        """The request's capabilities field carries the exact dict from the originating tasks."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"mgr", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"gpu": 2, "nvlink": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": 8, "nvlink": 4})
        manager_snapshots = {
            b"mgr": _create_manager_snapshot(b"mgr", worker_count=0, capabilities={"gpu": 8, "nvlink": 4})
        }

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(_capability_requests(commands[0]), [({"gpu": 2, "nvlink": 1}, 1)])

    def test_capability_request_emitted_even_when_overall_ratio_is_in_band(self):
        """A capability-specific request must still be allocated for tasks that need
        unique capabilities, even when the cluster-wide task/worker ratio doesn't warrant
        scaling generic workers."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"manager_a", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        # 3 generic + 1 gpu task; 3 workers running -> generic ratio is in band (no scale)
        # but a gpu request should still be emitted.
        generic_tasks = _create_tasks(3)
        gpu_tasks = _create_tasks(1, capabilities={"gpu": 1})
        all_tasks = {**generic_tasks, **gpu_tasks}
        workers = _create_workers(3)
        snapshot = InformationSnapshot(tasks=all_tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"manager_a", capabilities={"gpu": 4})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", worker_count=0, capabilities={"gpu": 4})
        }

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(len(commands), 1)
        cap_requests = _capability_requests(commands[0])
        self.assertIn("gpu", {next(iter(caps.keys()), None) for caps, _ in cap_requests})

    def test_generic_capset_present_with_no_capability_tasks(self):
        """When all tasks have no capabilities, the manager receives a generic request
        (empty capabilities dict) carrying the ratio-derived allocation."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"mgr", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr")
        manager_snapshots = {b"mgr": _create_manager_snapshot(b"mgr", worker_count=0)}

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        self.assertEqual(len(commands), 1)
        # The generic (empty capset) request is present.
        generic = _generic_request(commands[0])
        self.assertIsNotNone(generic)
        # ceil(5/10) = 1 generic worker desired
        self.assertEqual(generic.taskConcurrency, 1)
        # No capability-specific requests since no capability tasks exist.
        self.assertEqual(_capability_requests(commands[0]), [])

    def test_capability_match_is_key_only(self):
        """Capability matching is name-only: a task requiring {a: 3} is considered serviceable
        by a manager that advertises {a: 5} -- numeric values are not used for subsumption."""
        rules = [WaterfallRule(priority=1, worker_manager_id=b"mgr", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        tasks = _create_tasks(1, capabilities={"a": 3})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        # Manager's advertised "a" value (5) differs from the task's required "a" value (3).
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"a": 5})
        manager_snapshots = {b"mgr": _create_manager_snapshot(b"mgr", worker_count=0, capabilities={"a": 5})}

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], manager_snapshots)

        # The manager is considered capable despite the value mismatch, so a request for
        # the {a} capset is allocated to it.
        self.assertEqual(len(commands), 1)
        cap_requests = _capability_requests(commands[0])
        self.assertEqual(len(cap_requests), 1)
        self.assertEqual(set(cap_requests[0][0].keys()), {"a"})


class TestWaterfallV1Policy(unittest.TestCase):
    """Unit tests for WaterfallV1Policy config parsing and scaling delegation."""

    def setUp(self):
        setup_logger()
        # EvenLoadAllocatePolicy creates an AsyncPriorityQueue which requires an event loop.
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def test_config_parsing_via_factory(self):
        """Verify the factory parses waterfall_v1 policy config correctly."""
        policy = create_policy("waterfall_v1", "1,manager_a,10\n2,manager_b,20")
        self.assertIsInstance(policy, WaterfallV1Policy)

    def test_config_parsing_with_comments(self):
        """Comments and blank lines should be ignored."""
        policy_content = "\n".join(
            [
                "#priority,worker_manager_id[,max_task_concurrency]",
                "1,manager_a,10",
                "",
                "2,manager_b,20  # overflow tier",
            ]
        )
        policy = WaterfallV1Policy(policy_content)
        self.assertIsInstance(policy, WaterfallV1Policy)

    def test_config_parsing_without_max_task_concurrency(self):
        """max_task_concurrency may be omitted; the rule's field is then None."""
        rules = parse_waterfall_rules("1,manager_a\n2,manager_b,20")
        self.assertIsNone(rules[0].max_task_concurrency)
        self.assertEqual(rules[1].max_task_concurrency, 20)

    def test_invalid_config_empty(self):
        """Empty policy content should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("")

    def test_invalid_config_comments_only(self):
        """Policy content with only comments should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("# just a comment\n# another comment")

    def test_invalid_config_wrong_field_count(self):
        """Lines with too few or too many fields should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("manager_a_only")
        with self.assertRaises(ValueError):
            WaterfallV1Policy("1,manager_a,10,extra")

    def test_invalid_config_non_integer_priority(self):
        """Non-integer priority should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("high,manager_a,10")

    def test_invalid_config_non_integer_max_task_concurrency(self):
        """Non-integer max_task_concurrency should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("1,manager_a,many")

    def test_invalid_config_empty_worker_manager_id(self):
        """Empty worker_manager_id should raise ValueError."""
        with self.assertRaises(ValueError):
            WaterfallV1Policy("1,,10")

    def test_invalid_config_duplicate_worker_manager_id(self):
        """Duplicate worker_manager_id should raise ValueError."""
        with self.assertRaisesRegex(ValueError, "duplicate worker_manager_id"):
            WaterfallV1Policy("1,mgr_a,10\n2,mgr_a,20")

    def test_policy_delegates_to_scaling_policy(self):
        """Policy controller delegates declarative emission to its scaling policy."""
        policy = WaterfallV1Policy("1,manager_a,10\n2,manager_b,20")

        tasks = _create_tasks(5)
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        manager_snapshots = {
            b"manager_a": _create_manager_snapshot(b"manager_a", max_task_concurrency=10, worker_count=0),
            b"manager_b": _create_manager_snapshot(b"manager_b", max_task_concurrency=20, worker_count=0),
        }

        heartbeat_a = _create_worker_manager_heartbeat(b"manager_a", max_task_concurrency=10)
        commands_a = policy.get_scaling_commands(snapshot, heartbeat_a, [], manager_snapshots)
        self.assertEqual(len(commands_a), 1)
        self.assertEqual(_generic_request(commands_a[0]).taskConcurrency, 1)

        heartbeat_b = _create_worker_manager_heartbeat(b"manager_b", max_task_concurrency=20)
        commands_b = policy.get_scaling_commands(snapshot, heartbeat_b, [], manager_snapshots)
        # Higher-priority A absorbs all 1 desired worker; B's share is 0 -> emits setDesired(0).
        self.assertEqual(len(commands_b), 1)
        self.assertEqual(_generic_request(commands_b[0]).taskConcurrency, 0)

    def test_scaling_status(self):
        """get_scaling_status should return a ScalingManagerStatus."""
        policy = WaterfallV1Policy("1,manager_a,10")

        from scaler.protocol.capnp import ScalingManagerStatus

        managed_workers = {b"mgr-1": [WorkerID(b"worker-1")]}
        status = policy.get_scaling_status(managed_workers)
        self.assertIsInstance(status, ScalingManagerStatus)


class TestWaterfallV1PolicyAssignmentWithCapabilities(unittest.TestCase):
    """Tests that WaterfallV1Policy.assign_task respects task capabilities."""

    def setUp(self):
        setup_logger()
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    def test_task_with_capability_assigned_to_capable_worker(self):
        """Task requiring {"gpu": -1} must land on a worker that has gpu."""
        policy = WaterfallV1Policy("1,manager_a,10\n2,manager_b,20")

        worker_no_gpu = WorkerID(b"worker-no-gpu")
        worker_gpu = WorkerID(b"worker-gpu")

        # Insertion order matters: the no-gpu worker is queued first, so an
        # even-load allocator will hand it out first when both have count 0.
        self.assertTrue(policy.add_worker(worker_no_gpu, capabilities={}, queue_size=10))
        self.assertTrue(policy.add_worker(worker_gpu, capabilities={"gpu": -1}, queue_size=10))

        task = _create_mock_task(TaskID.generate_task_id(), capabilities={"gpu": -1})
        assigned = policy.assign_task(task)

        self.assertEqual(
            assigned, worker_gpu, f"task requiring gpu was assigned to {assigned!r}, expected {worker_gpu!r}"
        )

    def test_task_with_capability_not_assigned_when_no_capable_worker(self):
        """Task requiring gpu must not be assigned if no worker has gpu."""
        policy = WaterfallV1Policy("1,manager_a,10")

        worker_no_gpu = WorkerID(b"worker-no-gpu")
        self.assertTrue(policy.add_worker(worker_no_gpu, capabilities={}, queue_size=10))

        task = _create_mock_task(TaskID.generate_task_id(), capabilities={"gpu": -1})
        assigned = policy.assign_task(task)

        self.assertEqual(
            assigned,
            WorkerID.invalid_worker_id(),
            f"task requiring gpu was incorrectly assigned to {assigned!r} that has no gpu",
        )

    def test_has_available_worker_respects_capabilities(self):
        """has_available_worker({"gpu": -1}) must be False if no worker has gpu."""
        policy = WaterfallV1Policy("1,manager_a,10")

        worker_no_gpu = WorkerID(b"worker-no-gpu")
        self.assertTrue(policy.add_worker(worker_no_gpu, capabilities={}, queue_size=10))

        self.assertFalse(
            policy.has_available_worker({"gpu": -1}),
            "has_available_worker should return False when no worker has the requested capability",
        )


def _create_mock_task(task_id: TaskID, capabilities: Optional[dict[str, int]] = None) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=task_id,
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities or {},
    )


def _create_mock_worker_heartbeat(
    queued_tasks: int = 0, capabilities: Optional[dict[str, int]] = None
) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=queued_tasks,
        latencyUS=100,
        taskLock=False,
        processors=[],
        capabilities=capabilities or {},
        workerManagerID=b"test",
    )


def _create_worker_manager_heartbeat(
    worker_manager_id: bytes, max_task_concurrency: int = 10, capabilities: Optional[dict[str, int]] = None
) -> WorkerManagerHeartbeat:
    return WorkerManagerHeartbeat(
        maxTaskConcurrency=max_task_concurrency, capabilities=capabilities or {}, workerManagerID=worker_manager_id
    )


def _create_manager_snapshot(
    worker_manager_id: bytes,
    max_task_concurrency: int = 10,
    worker_count: int = 0,
    last_seen: Optional[float] = None,
    capabilities: Optional[dict[str, int]] = None,
) -> WorkerManagerSnapshot:
    return WorkerManagerSnapshot(
        worker_manager_id=worker_manager_id,
        max_task_concurrency=max_task_concurrency,
        worker_count=worker_count,
        last_seen_s=last_seen if last_seen is not None else time.time(),
        capabilities=capabilities or {},
    )


def _create_tasks(count: int, capabilities: Optional[dict[str, int]] = None) -> dict[TaskID, Task]:
    tasks = {}
    for _ in range(count):
        task_id = TaskID.generate_task_id()
        tasks[task_id] = _create_mock_task(task_id, capabilities=capabilities)
    return tasks


def _create_workers(
    count: int, queued_tasks: int = 0, capabilities: Optional[dict[str, int]] = None
) -> dict[WorkerID, WorkerHeartbeat]:
    workers = {}
    for i in range(count):
        worker_id = WorkerID(f"worker-{i}".encode())
        workers[worker_id] = _create_mock_worker_heartbeat(queued_tasks, capabilities=capabilities)
    return workers
