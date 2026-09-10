"""
Unit tests for KubernetesWorkerProvisioner.

The kubernetes API client and in-cluster config loading are stubbed out with
unittest.mock so no real cluster or credentials are required.

Heavy transitive dependencies of the module under test (the compiled
``scaler.protocol.capnp`` C extension and several IO subsystems) are also
pre-stubbed before the first import of the module, keeping this test suite
self-contained and fast.

Pod spec construction, labels, tolerations, and template deep-merge are
tested against a real KinD cluster in test_kind_integration.py.  This file
focuses on pure logic, error handling, and code paths that cannot be
exercised through a live cluster (API errors, FIFO ordering, etc.).
"""

from __future__ import annotations

import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ---------------------------------------------------------------------------
# Pre-stub modules that cannot be imported in a plain unit-test environment:
#   * scaler.protocol.capnp   - compiled C extension (not built in CI)
#   * scaler.io.*             - depends on native zmq extension
#   * several utility modules imported transitively by worker_manager_runner
# ---------------------------------------------------------------------------
_STUBS = [
    "scaler.protocol.capnp",
    "scaler.utility.identifiers",
    "scaler.io",
    "scaler.io.ymq",
    "scaler.io.ymq._ymq",
    "scaler.io.mixins",
    "scaler.io.network_backends",
    "scaler.io.utility",
    "scaler.protocol.helpers",
    "scaler.utility.event_loop",
    "scaler.utility.signal_handler",
    "scaler.config.common.security",
]
for _mod in _STUBS:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

import asyncio  # noqa: E402  (must come after stubs so asyncio itself is real)

import kubernetes.client.exceptions  # noqa: E402

# ---------------------------------------------------------------------------
# The module under test - imported *after* stubs are in place so that every
# transitive import resolves cleanly.
# ---------------------------------------------------------------------------
from scaler.config.common.worker_manager import WorkerManagerConfig  # noqa: E402
from scaler.config.section.kubernetes_worker_manager import KubernetesWorkerManagerConfig  # noqa: E402
from scaler.config.types.address import AddressConfig  # noqa: E402
from scaler.worker_manager_adapter.kubernetes.worker_manager import _deep_merge  # noqa: E402

# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> KubernetesWorkerManagerConfig:
    """Return a minimal KubernetesWorkerManagerConfig suitable for unit tests."""
    wm_cfg = WorkerManagerConfig(
        scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
        worker_manager_id="test-wm",
        max_task_concurrency=8,
    )
    defaults: dict = dict(
        worker_manager_config=wm_cfg,
        pod_image="test-image:latest",
        workers_per_pod=2,
        namespace="test-ns",
        delete_grace_period_seconds=0,
    )
    defaults.update(overrides)
    return KubernetesWorkerManagerConfig(**defaults)


def _make_request(task_concurrency: int, capabilities: dict | None = None) -> MagicMock:
    """Return a fake DesiredTaskConcurrencyRequest."""
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    caps = capabilities or {}
    request.capabilities = [MagicMock(name=k, value=v) for k, v in caps.items()]
    return request


def _make_provisioner(config: KubernetesWorkerManagerConfig | None = None, **config_overrides):
    """
    Construct a KubernetesWorkerProvisioner with a mocked CoreV1Api.

    Returns ``(provisioner, core_v1_mock)`` where *core_v1_mock* is the
    ``MagicMock`` instance wired up as ``provisioner._core_v1``.
    """
    from scaler.worker_manager_adapter.kubernetes.worker_manager import KubernetesWorkerProvisioner

    cfg = config or _make_config(**config_overrides)
    workers_per_pod = cfg.workers_per_pod
    mtc = cfg.worker_manager_config.max_task_concurrency
    import math

    max_pods = math.ceil(mtc / workers_per_pod) if mtc != -1 else -1

    with patch("kubernetes.config.load_incluster_config"), patch("kubernetes.client.CoreV1Api") as mock_cls:
        core_v1 = MagicMock()
        core_v1.create_namespaced_pod.return_value = MagicMock()
        core_v1.delete_namespaced_pod.return_value = MagicMock()
        mock_cls.return_value = core_v1
        provisioner = KubernetesWorkerProvisioner(cfg, max_pods=max_pods)

    provisioner._core_v1 = core_v1
    return provisioner, core_v1


def _get_command(core_v1: MagicMock, call_index: int = 0) -> str:
    """Return the COMMAND env-var value from the *call_index*-th created pod."""
    pod = core_v1.create_namespaced_pod.call_args_list[call_index].args[1]
    for env_var in pod["spec"]["containers"][0]["env"]:
        if env_var["name"] == "COMMAND":
            return env_var["value"]
    raise AssertionError("COMMAND env var not found in pod spec")


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


class TestAPIErrorHandling(unittest.IsolatedAsyncioTestCase):
    """Tests for API error handling during pod creation."""

    async def test_start_units_api_error_does_not_add_to_pods(self) -> None:
        """An ApiException during pod creation must not add the pod to the tracked list."""
        provisioner, core_v1 = _make_provisioner()
        core_v1.create_namespaced_pod.side_effect = kubernetes.client.exceptions.ApiException(status=500)

        await provisioner.start_units(1)

        self.assertEqual(provisioner.active_unit_count(), 0)


class TestPodWatch(unittest.IsolatedAsyncioTestCase):
    """Tests for the pod watch that detects externally deleted pods."""

    async def test_deleted_event_removes_tracked_pod(self) -> None:
        """A DELETED watch event for a tracked pod must remove it from _pods."""
        provisioner, _ = _make_provisioner()
        await provisioner.start_units(2)
        deleted_name = provisioner._pods[0]
        surviving_name = provisioner._pods[1]

        deleted_pod = MagicMock()
        deleted_pod.metadata.name = deleted_name
        event = {"type": "DELETED", "object": deleted_pod}
        stop_event = provisioner._watch_stop

        def fake_stream(*args, **kwargs):
            yield event
            stop_event.set()

        with patch("kubernetes.watch.Watch") as mock_watch_cls:
            mock_watch_cls.return_value.stream = fake_stream
            provisioner._loop = asyncio.get_running_loop()
            provisioner._pod_watch_loop()

        self.assertEqual(provisioner._pods, [surviving_name])

    async def test_deleted_event_for_unknown_pod_is_ignored(self) -> None:
        """A DELETED event for a pod not in _pods must be ignored."""
        provisioner, _ = _make_provisioner()
        await provisioner.start_units(1)
        original_pods = list(provisioner._pods)

        unknown_pod = MagicMock()
        unknown_pod.metadata.name = "some-other-pod"
        event = {"type": "DELETED", "object": unknown_pod}
        stop_event = provisioner._watch_stop

        def fake_stream(*args, **kwargs):
            yield event
            stop_event.set()

        with patch("kubernetes.watch.Watch") as mock_watch_cls:
            mock_watch_cls.return_value.stream = fake_stream
            provisioner._loop = asyncio.get_running_loop()
            provisioner._pod_watch_loop()

        self.assertEqual(provisioner._pods, original_pods)

    async def test_non_deleted_events_do_not_remove_pods(self) -> None:
        """ADDED and MODIFIED events must not remove tracked pods."""
        provisioner, _ = _make_provisioner()
        await provisioner.start_units(1)
        pod_name = provisioner._pods[0]

        pod_obj = MagicMock()
        pod_obj.metadata.name = pod_name
        stop_event = provisioner._watch_stop

        def fake_stream(*args, **kwargs):
            yield {"type": "ADDED", "object": pod_obj}
            yield {"type": "MODIFIED", "object": pod_obj}
            stop_event.set()

        with patch("kubernetes.watch.Watch") as mock_watch_cls:
            mock_watch_cls.return_value.stream = fake_stream
            provisioner._loop = asyncio.get_running_loop()
            provisioner._pod_watch_loop()

        self.assertEqual(provisioner.active_unit_count(), 1)

    async def test_watch_started_once(self) -> None:
        """_start_pod_watch must only start the watch thread once."""
        provisioner, _ = _make_provisioner()
        provisioner._start_pod_watch()
        first_thread = provisioner._watch_thread
        provisioner._start_pod_watch()
        self.assertIs(provisioner._watch_thread, first_thread)
        provisioner._stop_pod_watch()


class TestStopUnits(unittest.IsolatedAsyncioTestCase):
    """Tests for KubernetesWorkerProvisioner.stop_units."""

    async def test_stop_units_deletes_pods(self) -> None:
        """stop_units(2) after starting 3 must delete the first two pods."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(3)
        first_two = provisioner._pods[:2]

        await provisioner.stop_units(2)

        self.assertEqual(core_v1.delete_namespaced_pod.call_count, 2)
        deleted_names = [c.args[0] for c in core_v1.delete_namespaced_pod.call_args_list]
        self.assertEqual(deleted_names, first_two)
        self.assertEqual(provisioner.active_unit_count(), 1)

    async def test_stop_units_404_is_tolerated(self) -> None:
        """A 404 ApiException during deletion must not propagate and must remove the pod."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(1)
        core_v1.delete_namespaced_pod.side_effect = kubernetes.client.exceptions.ApiException(status=404)

        await provisioner.stop_units(1)

        self.assertEqual(provisioner.active_unit_count(), 0)

    async def test_stop_units_other_api_error_logs_and_keeps_pod(self) -> None:
        """A non-404 ApiException must not propagate but must leave the pod tracked."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(1)
        core_v1.delete_namespaced_pod.side_effect = kubernetes.client.exceptions.ApiException(status=500)

        await provisioner.stop_units(1)

        self.assertEqual(provisioner.active_unit_count(), 1)


class TestTerminate(unittest.IsolatedAsyncioTestCase):
    """Tests for KubernetesWorkerProvisioner.terminate."""

    async def test_terminate_cancels_coordinator_and_stops_all_pods(self) -> None:
        """terminate() must stop all running pods and empty the tracked list."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(2)

        await provisioner.terminate()

        self.assertEqual(core_v1.delete_namespaced_pod.call_count, 2)
        self.assertEqual(provisioner.active_unit_count(), 0)


class TestDesiredTaskConcurrency(unittest.IsolatedAsyncioTestCase):
    """Tests for KubernetesWorkerProvisioner.set_desired_task_concurrency."""

    async def test_set_desired_task_concurrency_divides_by_workers_per_pod(self) -> None:
        """taskConcurrency=8 with workers_per_pod=4 should yield desired_unit_count=2."""
        wm_cfg = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="test-wm",
            max_task_concurrency=16,
        )
        cfg = _make_config(worker_manager_config=wm_cfg, workers_per_pod=4)
        provisioner, _ = _make_provisioner(config=cfg)
        request = _make_request(task_concurrency=8)

        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])

        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 2)

    async def test_set_desired_task_concurrency_rounds_up(self) -> None:
        """taskConcurrency=5 with workers_per_pod=4 should yield desired_unit_count=2 (ceil)."""
        wm_cfg = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="test-wm",
            max_task_concurrency=16,
        )
        cfg = _make_config(worker_manager_config=wm_cfg, workers_per_pod=4)
        provisioner, _ = _make_provisioner(config=cfg)
        request = _make_request(task_concurrency=5)

        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])

        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 2)  # ceil(5/4)


class TestCommandEnvVar(unittest.IsolatedAsyncioTestCase):
    """Tests for the COMMAND env var injected into the pod spec."""

    async def test_command_contains_worker_manager_id(self) -> None:
        """COMMAND must include '--worker-manager-id test-wm'."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(1)
        self.assertIn("--worker-manager-id test-wm", _get_command(core_v1))

    async def test_command_contains_mode_fixed(self) -> None:
        """COMMAND must include '--mode fixed'."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(1)
        self.assertIn("--mode fixed", _get_command(core_v1))

    async def test_command_contains_worker_type_kubernetes(self) -> None:
        """COMMAND must include '--worker-type KUBERNETES'."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(1)
        self.assertIn("--worker-type KUBERNETES", _get_command(core_v1))


# ---------------------------------------------------------------------------
# _deep_merge utility tests
# ---------------------------------------------------------------------------


class TestDeepMerge(unittest.TestCase):
    """Unit tests for the _deep_merge utility function."""

    def test_scalar_override_wins(self) -> None:
        result = _deep_merge({"a": 1}, {"a": 99})
        self.assertEqual(result["a"], 99)

    def test_nested_dict_merged_recursively(self) -> None:
        base = {"spec": {"restartPolicy": "Never", "hostNetwork": False}}
        result = _deep_merge(base, {"spec": {"hostNetwork": True}})
        self.assertEqual(result["spec"]["restartPolicy"], "Never")
        self.assertEqual(result["spec"]["hostNetwork"], True)

    def test_list_replaced_entirely(self) -> None:
        result = _deep_merge({"volumes": [{"name": "a"}, {"name": "b"}]}, {"volumes": [{"name": "c"}]})
        self.assertEqual(result["volumes"], [{"name": "c"}])

    def test_empty_override_is_noop(self) -> None:
        base = {"a": 1, "b": {"c": 2}}
        self.assertEqual(_deep_merge(base, {}), base)

    def test_new_key_added(self) -> None:
        result = _deep_merge({"a": 1}, {"b": 2})
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 2)

    def test_base_is_not_mutated(self) -> None:
        base = {"a": {"x": 1}}
        _deep_merge(base, {"a": {"y": 2}})
        self.assertNotIn("y", base["a"])


# ---------------------------------------------------------------------------
# Config validation tests
# ---------------------------------------------------------------------------


class TestConfigValidation(unittest.TestCase):
    """Tests for KubernetesWorkerManagerConfig.__post_init__ validation."""

    def test_empty_image_raises(self) -> None:
        with self.assertRaises(ValueError, msg="pod_image cannot be empty"):
            _make_config(pod_image="")

    def test_whitespace_image_raises(self) -> None:
        with self.assertRaises(ValueError, msg="pod_image cannot be empty"):
            _make_config(pod_image="   ")

    def test_workers_per_pod_zero_raises(self) -> None:
        with self.assertRaises(ValueError, msg="workers_per_pod must be >= 1"):
            _make_config(workers_per_pod=0)

    def test_negative_grace_period_raises(self) -> None:
        with self.assertRaises(ValueError, msg="delete_grace_period_seconds must be >= 0"):
            _make_config(delete_grace_period_seconds=-1)

    def test_valid_config_does_not_raise(self) -> None:
        cfg = _make_config()
        self.assertEqual(cfg.pod_image, "test-image:latest")


# ---------------------------------------------------------------------------
# Kubeconfig loading tests
# ---------------------------------------------------------------------------


class TestKubeconfigLoading(unittest.TestCase):
    """Tests for kubeconfig vs in-cluster auth selection."""

    def test_empty_kubeconfig_uses_incluster(self) -> None:
        """kubeconfig_path='' must call load_incluster_config."""
        with (
            patch("kubernetes.config.load_incluster_config") as mock_incluster,
            patch("kubernetes.config.load_kube_config") as mock_kubeconfig,
            patch("kubernetes.client.CoreV1Api"),
        ):
            from scaler.worker_manager_adapter.kubernetes.worker_manager import KubernetesWorkerProvisioner

            KubernetesWorkerProvisioner(_make_config(kubeconfig_path=""), max_pods=4)
            mock_incluster.assert_called_once()
            mock_kubeconfig.assert_not_called()

    def test_kubeconfig_path_loads_file(self) -> None:
        """A non-empty kubeconfig_path must call load_kube_config with that path."""
        with (
            patch("kubernetes.config.load_incluster_config") as mock_incluster,
            patch("kubernetes.config.load_kube_config") as mock_kubeconfig,
            patch("kubernetes.client.CoreV1Api"),
        ):
            from scaler.worker_manager_adapter.kubernetes.worker_manager import KubernetesWorkerProvisioner

            KubernetesWorkerProvisioner(_make_config(kubeconfig_path="/home/user/.kube/config"), max_pods=4)
            mock_kubeconfig.assert_called_once_with("/home/user/.kube/config")
            mock_incluster.assert_not_called()


# ---------------------------------------------------------------------------
# Command builder edge-case tests
# ---------------------------------------------------------------------------


class TestCommandBuilderEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Tests for _build_worker_command covering optional flags."""

    async def test_command_includes_object_storage_address(self) -> None:
        """COMMAND must include --object-storage-address when configured."""
        wm_cfg = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="test-wm",
            max_task_concurrency=8,
            object_storage_address=AddressConfig.from_string("tcp://10.0.0.5:8517"),
        )
        provisioner, core_v1 = _make_provisioner(config=_make_config(worker_manager_config=wm_cfg))
        await provisioner.start_units(1)
        cmd = _get_command(core_v1)
        self.assertIn("--object-storage-address", cmd)
        self.assertIn("10.0.0.5:8517", cmd)

    async def test_command_includes_max_task_concurrency_from_workers_per_pod(self) -> None:
        """--max-task-concurrency must match workers_per_pod, not the global max."""
        provisioner, core_v1 = _make_provisioner(workers_per_pod=4)
        await provisioner.start_units(1)
        cmd = _get_command(core_v1)
        self.assertIn("--max-task-concurrency 4", cmd)

    async def test_command_uses_effective_worker_scheduler_address(self) -> None:
        """COMMAND must use worker_scheduler_address when set, not scheduler_address."""
        wm_cfg = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="test-wm",
            max_task_concurrency=8,
            worker_scheduler_address=AddressConfig.from_string("tcp://10.0.0.1:8516"),
        )
        provisioner, core_v1 = _make_provisioner(config=_make_config(worker_manager_config=wm_cfg))
        await provisioner.start_units(1)
        cmd = _get_command(core_v1)
        self.assertIn("10.0.0.1:8516", cmd)
        self.assertNotIn("127.0.0.1:8516", cmd)


# ---------------------------------------------------------------------------
# Stop units edge-case tests
# ---------------------------------------------------------------------------


class TestStopUnitsEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Edge cases for stop_units."""

    async def test_stop_more_than_available(self) -> None:
        """Requesting to stop more pods than exist must not raise and must stop all available."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(2)

        await provisioner.stop_units(5)

        self.assertEqual(core_v1.delete_namespaced_pod.call_count, 2)
        self.assertEqual(provisioner.active_unit_count(), 0)

    async def test_stop_zero_is_noop(self) -> None:
        """stop_units(0) must not delete anything."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(2)

        await provisioner.stop_units(0)

        core_v1.delete_namespaced_pod.assert_not_called()
        self.assertEqual(provisioner.active_unit_count(), 2)

    async def test_stop_units_fifo_order(self) -> None:
        """stop_units must delete the oldest (first-created) pods first."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(4)
        all_pods = list(provisioner._pods)

        await provisioner.stop_units(2)

        deleted = [c.args[0] for c in core_v1.delete_namespaced_pod.call_args_list]
        self.assertEqual(deleted, all_pods[:2])
        self.assertEqual(provisioner._pods, all_pods[2:])


# ---------------------------------------------------------------------------
# Pod uniqueness tests
# ---------------------------------------------------------------------------


class TestPodNameUniqueness(unittest.IsolatedAsyncioTestCase):
    """Tests for pod name generation."""

    async def test_pod_names_are_unique(self) -> None:
        """Each created pod must have a unique name."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(10)
        self.assertEqual(len(set(provisioner._pods)), 10)

    async def test_pod_names_have_scaler_prefix(self) -> None:
        """Pod names must start with 'scaler-worker-'."""
        provisioner, core_v1 = _make_provisioner()
        await provisioner.start_units(3)
        for name in provisioner._pods:
            self.assertTrue(name.startswith("scaler-worker-"), f"Unexpected pod name: {name}")


# ---------------------------------------------------------------------------
# KubernetesWorkerManager constructor tests
# ---------------------------------------------------------------------------


class TestKubernetesWorkerManager(unittest.TestCase):
    """Tests for KubernetesWorkerManager max_pods calculation."""

    def test_max_pods_calculated_from_concurrency(self) -> None:
        """max_pods must be ceil(max_task_concurrency / workers_per_pod)."""
        wm_cfg = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="test-wm",
            max_task_concurrency=10,
        )
        cfg = _make_config(worker_manager_config=wm_cfg, workers_per_pod=4)
        with (
            patch("kubernetes.config.load_incluster_config"),
            patch("kubernetes.client.CoreV1Api"),
            patch("scaler.worker_manager_adapter.kubernetes.worker_manager.WorkerManagerRunner") as mock_runner_cls,
        ):
            from scaler.worker_manager_adapter.kubernetes.worker_manager import KubernetesWorkerManager

            KubernetesWorkerManager(cfg)
            call_kwargs = mock_runner_cls.call_args.kwargs
            self.assertEqual(call_kwargs["max_provisioner_units"], 3)  # ceil(10/4)
            self.assertEqual(call_kwargs["workers_per_provisioner_unit"], 4)

    def test_unlimited_concurrency_passes_negative_one(self) -> None:
        """max_task_concurrency=-1 must pass max_provisioner_units=-1."""
        wm_cfg = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="test-wm",
            max_task_concurrency=-1,
        )
        cfg = _make_config(worker_manager_config=wm_cfg, workers_per_pod=2)
        with (
            patch("kubernetes.config.load_incluster_config"),
            patch("kubernetes.client.CoreV1Api"),
            patch("scaler.worker_manager_adapter.kubernetes.worker_manager.WorkerManagerRunner") as mock_runner_cls,
        ):
            from scaler.worker_manager_adapter.kubernetes.worker_manager import KubernetesWorkerManager

            KubernetesWorkerManager(cfg)
            call_kwargs = mock_runner_cls.call_args.kwargs
            self.assertEqual(call_kwargs["max_provisioner_units"], -1)


if __name__ == "__main__":
    unittest.main()
