import os
import signal
import sys
import time
import unittest
from multiprocessing import Process
from typing import Dict, Optional

from scaler import Client
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat, WorkerManagerHeartbeat
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.snapshot import InformationSnapshot
from tests.utility.utility import logging_test_name


class TestScaling(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_address = AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}")

    @unittest.skipIf(
        sys.platform == "win32",
        "Declarative scale-down calls stop_units mid-test, which on POSIX uses os.kill(pid, SIGINT) "
        "so the worker sends WorkerDisconnectNotification on shutdown. Windows has no equivalent "
        "for delivering SIGINT to a multiprocessing.spawn child (Python's os.kill on Windows maps SIGINT "
        "to TerminateProcess, and CTRL_C_EVENT requires CREATE_NEW_PROCESS_GROUP), so any scaled-down "
        "worker is killed without notice and the scheduler waits ~60s for heartbeat timeout. The scaling "
        "policy logic itself is covered by TestVanillaScalingPolicy below.",
    )
    def test_scaling_basic(self):
        object_storage = ObjectStorageServerProcess(
            bind_address=self.object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_address,
            advertised_object_storage_address=None,
            monitor_address=None,
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=vanilla"),
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        manager_process = Process(target=_run_native_worker_manager, args=(self.scheduler_address,))
        manager_process.start()

        with Client(self.scheduler_address) as client:
            client.map(time.sleep, [0.1] * 100)

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        manager_process.terminate()
        manager_process.join()

    @unittest.skipIf(
        sys.platform == "win32",
        "See test_scaling_basic: declarative scale-down has no graceful path on Windows. "
        "Capability policy logic is covered by TestCapabilityScalingPolicy below.",
    )
    def test_capability_scaling_basic(self):
        """Test that capability scaling starts workers with the correct capabilities."""
        object_storage = ObjectStorageServerProcess(
            bind_address=self.object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_address,
            advertised_object_storage_address=None,
            monitor_address=None,
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=capability"),
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        manager_process = Process(target=_run_native_worker_manager, args=(self.scheduler_address,))
        manager_process.start()

        with Client(self.scheduler_address) as client:
            # Submit tasks without capabilities (should work like vanilla)
            client.map(time.sleep, [0.1] * 50)

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        manager_process.terminate()
        manager_process.join()


class TestVanillaScalingPolicy(unittest.TestCase):
    """Unit tests for VanillaScalingPolicy declarative emission."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy()

    def _single_request(self, snapshot, heartbeat, managed):
        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})
        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        return requests[0]

    def test_idle_emits_desired_zero(self):
        """Empty state: ratio computes desired=0 -> policy unconditionally emits setDesired(0)."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, [])

        self.assertEqual(request.taskConcurrency, 0)

    def test_tasks_with_no_workers_targets_one(self):
        """Tasks present but no workers: desired = 1 to bootstrap."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, [])

        self.assertEqual(request.taskConcurrency, 1)

    def test_high_task_ratio_targets_current_plus_one(self):
        """Task/worker ratio above upper threshold scales by +1 toward equilibrium."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(20)}
        managed = [WorkerID(b"w0")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=5) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=10)

        request = self._single_request(snapshot, heartbeat, managed)

        self.assertEqual(request.taskConcurrency, 2)

    def test_no_tasks_with_workers_targets_zero(self):
        """No tasks but managers exist: desired drains to 0."""
        workers = {
            WorkerID(b"w0"): _create_mock_worker_heartbeat({}, queued_tasks=0),
            WorkerID(b"w1"): _create_mock_worker_heartbeat({}, queued_tasks=1),
            WorkerID(b"w2"): _create_mock_worker_heartbeat({}, queued_tasks=2),
        }
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, managed)

        self.assertEqual(request.taskConcurrency, 0)

    def test_few_tasks_with_many_workers_targets_min_keep(self):
        """Low task ratio shrinks toward ceil(tasks / upper_task_ratio) min_keep."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(10)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, managed)

        # ceil(5 / 10) = 1 minimum to keep
        self.assertEqual(request.taskConcurrency, 1)

    def test_max_concurrency_clamps_then_emits(self):
        """Ratio asks for current+1 but cap clamps to current -> emits setDesired(current)."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(50)}
        managed = [WorkerID(b"w0"), WorkerID(b"w1")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=20) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=2)

        request = self._single_request(snapshot, heartbeat, managed)

        # Ratio would say desired=3, cap clamps to 2; emits setDesired(2).
        self.assertEqual(request.taskConcurrency, 2)


class TestAtCapacityEmission(unittest.TestCase):
    """Policies unconditionally emit setDesired even when the computed desired equals the
    manager's current connected worker count -- the worker manager always receives the
    authoritative desired count."""

    def setUp(self):
        setup_logger()

    def test_vanilla_at_cap_emits_current(self):
        """Vanilla: ratio asks for current+1, cap clamps to current -> emits setDesired(current)."""
        policy = VanillaScalingPolicy()
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(100)}
        managed = [WorkerID(f"w{i}".encode()) for i in range(10)]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=10) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=10)

        commands = policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 10)

    def test_vanilla_changes_emit(self):
        """Vanilla: when ratio's desired differs from current connected, emit."""
        policy = VanillaScalingPolicy()
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(100)}
        managed = [WorkerID(b"w0")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=10) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=10)

        commands = policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 2)

    def test_capability_at_cap_emits_current(self):
        """Capability: ratio's per-capset desired equals current connected -> emits setDesired(current)."""
        policy = CapabilityScalingPolicy()
        # ceil(50/5) = 10 desired for gpu capset; clamp at cap=10; emits setDesired(10).
        tasks = {}
        for _ in range(50):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {"gpu": 1})
        managed = [WorkerID(f"w{i}".encode()) for i in range(10)]
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=10, capabilities={"gpu": -1})

        commands = policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].taskConcurrency, 10)

    def test_capability_unservable_capset_emits(self):
        """Capability: a request whose capabilities aren't a subset of the manager's caps
        is still forwarded -- the policy emits the capset command regardless of serviceability."""
        policy = CapabilityScalingPolicy()
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={})

        commands = policy.get_scaling_commands(snapshot, heartbeat, [], {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_waterfall_at_cap_emits_current(self):
        """Waterfall: when the manager is full per the priority chain, it still emits setDesired(current)."""
        from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
        from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule
        from scaler.scheduler.controllers.policies.waterfall_v1.scaling.waterfall import WaterfallScalingPolicy

        rules = [WaterfallRule(priority=1, worker_manager_id=b"mgr", max_task_concurrency=10)]
        policy = WaterfallScalingPolicy(rules)
        # ceil(100/10) = 10; cap=10; desired=10 -> emits setDesired(10).
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(100)}
        managed = [WorkerID(f"w{i}".encode()) for i in range(10)]
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=10)
        manager_snapshots = {
            b"mgr": WorkerManagerSnapshot(
                worker_manager_id=b"mgr", max_task_concurrency=10, worker_count=10, last_seen_s=0.0, capabilities={}
            )
        }

        commands = policy.get_scaling_commands(snapshot, heartbeat, managed, manager_snapshots)

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 10)


class TestCapabilityScalingPolicy(unittest.TestCase):
    """Unit tests for CapabilityScalingPolicy declarative emission."""

    def setUp(self):
        setup_logger()
        self.policy = CapabilityScalingPolicy()

    def _commands(self, snapshot, heartbeat, managed):
        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})
        self.assertEqual(len(commands), 1)
        return commands[0]

    def test_no_tasks_emits_empty_requests(self):
        """No tasks: per-capset list is empty -> emits a command with no capset requests."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, [], {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 0)

    def test_one_capset_targets_at_least_one_worker(self):
        """A single capset with one task targets one worker for that capset."""
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": -1})

        command = self._commands(snapshot, heartbeat, [])

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        self.assertEqual(capabilities_to_dict(requests[0].capabilities), {"gpu": 1})
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_two_capsets_get_separate_requests(self):
        """Two capsets with tasks each: declarative has one request per capset."""
        gpu_id = TaskID.generate_task_id()
        tpu_id = TaskID.generate_task_id()
        tasks = {gpu_id: _create_mock_task(gpu_id, {"gpu": 1}), tpu_id: _create_mock_task(tpu_id, {"tpu": 1})}
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": -1, "tpu": -1})

        command = self._commands(snapshot, heartbeat, [])

        requests = list(command.setDesiredTaskConcurrencyRequests)
        caps_in_requests = {frozenset(capabilities_to_dict(r.capabilities).keys()) for r in requests}
        self.assertEqual(len(requests), 2)
        self.assertIn(frozenset({"gpu"}), caps_in_requests)
        self.assertIn(frozenset({"tpu"}), caps_in_requests)

    def test_high_task_count_scales_per_capset(self):
        """Many tasks for one capset: desired = ceil(task_count / upper_task_ratio)."""
        tasks = {}
        for _ in range(20):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": -1})

        command = self._commands(snapshot, heartbeat, [])

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        # ceil(20 / 5) = 4
        self.assertEqual(requests[0].taskConcurrency, 4)

    def test_max_concurrency_clamps_per_capset(self):
        """Per-capset desired clamped by manager's maxTaskConcurrency."""
        tasks = {}
        for _ in range(50):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {"gpu": 1})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=3, capabilities={"gpu": -1})

        command = self._commands(snapshot, heartbeat, [])

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 3)

    def test_capability_desired_omits_empty_capset(self):
        """A capability set with zero observed tasks must not appear in the emitted requests."""
        # 1 gpu task; tpu capset has no tasks even if the manager advertises tpu.
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": -1, "tpu": -1})

        command = self._commands(snapshot, heartbeat, [])

        requests = list(command.setDesiredTaskConcurrencyRequests)
        capsets = [set(capabilities_to_dict(r.capabilities).keys()) for r in requests]
        self.assertIn({"gpu"}, capsets)
        self.assertNotIn({"tpu"}, capsets)

    def test_starts_worker_request_when_no_capable_workers_yet(self):
        """A task requiring a capability that no manager-side worker yet provides
        still produces a per-capset request so the manager can spawn one."""
        task_id = TaskID.generate_task_id()
        snapshot = InformationSnapshot(tasks={task_id: _create_mock_task(task_id, {"gpu": 1})}, workers={})
        # Manager advertises gpu so the request is serviceable; no managed workers yet.
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": -1})

        command = self._commands(snapshot, heartbeat, [])

        requests = list(command.setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        self.assertEqual(set(capabilities_to_dict(requests[0].capabilities).keys()), {"gpu"})
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_get_status_returns_scaling_manager_status(self):
        """CapabilityScalingPolicy.get_status returns a ScalingManagerStatus object."""
        from scaler.protocol.capnp import ScalingManagerStatus

        managed_workers = {b"mgr": [WorkerID(b"w0")]}
        status = self.policy.get_status(managed_workers)
        self.assertIsInstance(status, ScalingManagerStatus)


class TestVanillaDeclarativeEquivalents(unittest.TestCase):
    """Declarative equivalents of the master-branch vanilla shutdown tests."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy()

    def test_drain_all_when_idle(self):
        """With workers connected and no tasks, the policy targets desired=0 and emits
        setDesired(0) so the manager can drain its workers."""
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=0) for i in range(4)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 0)

    def test_shrink_to_ratio_floor(self):
        """With few tasks relative to workers (ratio below lower threshold), the policy
        targets ceil(tasks/upper_task_ratio) and emits setDesired with that smaller count."""
        # 5 tasks, 10 connected workers -> ratio 0.5 < 1; floor = max(1, ceil(5/10)) = 1.
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(10)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_no_action_when_ratio_is_in_band(self):
        """With the task/worker ratio inside [lower, upper], the policy targets the current
        worker count and emits setDesired(current) unconditionally."""
        # 15 tasks, 5 workers -> ratio 3, in [1, 10]; desired = current = 5.
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(15)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(5)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 5)

    def test_get_status_returns_scaling_manager_status(self):
        """VanillaScalingPolicy.get_status returns a ScalingManagerStatus object."""
        from scaler.protocol.capnp import ScalingManagerStatus

        managed_workers = {b"mgr": [WorkerID(b"w0")]}
        status = self.policy.get_status(managed_workers)
        self.assertIsInstance(status, ScalingManagerStatus)


class TestCapabilityDeclarativeEquivalents(unittest.TestCase):
    """Declarative equivalents of the master-branch capability shutdown tests."""

    def setUp(self):
        setup_logger()
        self.policy = CapabilityScalingPolicy()

    def test_drain_capset_when_no_tasks(self):
        """With 3 connected workers and no tasks, the policy emits setDesired with an
        empty request list -- effective desired (0) differs from current (3) so the manager
        is told to drain (via extract_desired_count returning 0 for the empty list)."""
        managed = [WorkerID(f"w{i}".encode()) for i in range(3)]
        snapshot = InformationSnapshot(tasks={}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", capabilities={"gpu": -1})

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        # No per-capset requests because there are no tasks -- the manager interprets this
        # as "desired 0" and drains its workers.
        self.assertEqual(list(commands[0].setDesiredTaskConcurrencyRequests), [])


class TestPendingWorkersStatus(unittest.IsolatedAsyncioTestCase):
    """Pending-worker reporting in WorkerManagerController.get_status."""

    def setUp(self):
        from unittest.mock import AsyncMock, MagicMock

        from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController

        setup_logger()
        config_controller = MagicMock()
        policy_controller = MagicMock()
        policy_controller.get_scaling_status.return_value = MagicMock(managed_workers={})

        self.controller = WorkerManagerController(config_controller, policy_controller)
        self.policy_controller = policy_controller

        binder = AsyncMock()
        task_controller = MagicMock()
        task_controller._task_id_to_task = {}
        self.worker_controller = MagicMock()
        self.worker_controller._worker_alive_since = {}

        self.controller.register(binder, task_controller, self.worker_controller)

    async def test_pending_equals_desired_minus_connected(self):
        """pendingWorkers = max(0, last_desired_total - connected_count)."""
        from scaler.scheduler.controllers.worker_manager_utilties import build_set_desired_command

        source = b"mgr-src"
        manager_id = b"mgr-id"
        heartbeat = _create_worker_manager_heartbeat(manager_id)

        # Policy returns a setDesired command totaling 5 workers for this manager (empty caps).
        self.policy_controller.get_scaling_commands.return_value = [build_set_desired_command([({}, 5)])]
        # 2 workers are currently connected to this manager.
        self.worker_controller.get_workers_by_manager_id.return_value = [WorkerID(b"w0"), WorkerID(b"w1")]

        await self.controller.on_heartbeat(source, heartbeat)
        status = self.controller.get_status()

        detail = next(d for d in status.workerManagerDetails if d.workerManagerID == manager_id)
        self.assertEqual(detail.pendingWorkers, 3)  # 5 desired - 2 connected

    async def test_pending_clamped_at_zero(self):
        """If more workers are connected than desired, pendingWorkers is 0 (never negative)."""
        from scaler.scheduler.controllers.worker_manager_utilties import build_set_desired_command

        source = b"mgr-src"
        manager_id = b"mgr-id"
        heartbeat = _create_worker_manager_heartbeat(manager_id)

        self.policy_controller.get_scaling_commands.return_value = [build_set_desired_command([({}, 1)])]
        self.worker_controller.get_workers_by_manager_id.return_value = [WorkerID(b"w0"), WorkerID(b"w1")]

        await self.controller.on_heartbeat(source, heartbeat)
        status = self.controller.get_status()

        detail = next(d for d in status.workerManagerDetails if d.workerManagerID == manager_id)
        self.assertEqual(detail.pendingWorkers, 0)

    async def test_pending_only_counts_capsets_this_manager_can_serve(self):
        """A capset whose capabilities are not a subset of the manager's capabilities is excluded."""
        from scaler.scheduler.controllers.worker_manager_utilties import build_set_desired_command

        source = b"mgr-src"
        manager_id = b"mgr-id"
        # Manager advertises only "cpu" capability.
        heartbeat = WorkerManagerHeartbeat(maxTaskConcurrency=10, capabilities={"cpu": -1}, workerManagerID=manager_id)

        # Generic (empty caps, wildcard) -> 2; gpu-only -> 4 (not servable); cpu-only -> 3 (servable).
        self.policy_controller.get_scaling_commands.return_value = [
            build_set_desired_command([({}, 2), ({"gpu": -1}, 4), ({"cpu": -1}, 3)])
        ]
        self.worker_controller.get_workers_by_manager_id.return_value = []

        await self.controller.on_heartbeat(source, heartbeat)
        status = self.controller.get_status()

        detail = next(d for d in status.workerManagerDetails if d.workerManagerID == manager_id)
        # 2 (empty wildcard) + 3 (cpu subset) - 0 connected = 5; gpu (4) is excluded.
        self.assertEqual(detail.pendingWorkers, 5)


def _create_mock_task(task_id: TaskID, capabilities: dict) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=task_id,
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities,
    )


def _create_mock_worker_heartbeat(capabilities: dict, queued_tasks: int = 0) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=queued_tasks,
        latencyUS=100,
        taskLock=False,
        processors=[],
        capabilities=capabilities,
        workerManagerID=b"test",
    )


def _create_worker_manager_heartbeat(
    worker_manager_id: bytes, max_task_concurrency: int = 10, capabilities: Optional[Dict[str, int]] = None
) -> WorkerManagerHeartbeat:
    return WorkerManagerHeartbeat(
        maxTaskConcurrency=max_task_concurrency, capabilities=capabilities or {}, workerManagerID=worker_manager_id
    )


def _run_native_worker_manager(
    scheduler_address: str, max_task_concurrency: int = 4, worker_manager_id: str = "test_manager"
) -> None:
    from scaler.worker_manager.native.worker_manager import NativeWorkerManager

    manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_manager_id=worker_manager_id,
                object_storage_address=None,
                max_task_concurrency=max_task_concurrency,
            ),
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=10,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
                io_threads=DEFAULT_IO_THREADS,
                event_loop="builtin",
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )

    manager.run()
