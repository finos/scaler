"""
KinD (Kubernetes-in-Docker) integration tests for the k8s_raw worker manager.

These tests require:
  - ``kind`` CLI installed and on PATH
  - ``kubectl`` CLI installed and on PATH
  - Docker daemon running
  - The scaler worker image built and loaded (done by scripts/kind-test.sh)
  - opengris-scaler installed in the active Python environment

Skipped automatically unless SCALER_KIND_TESTS=1 is set.

Run via::

    ./scripts/kind-test.sh
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from typing import IO, List, Optional

if os.environ.get("SCALER_KIND_TESTS") != "1":
    raise unittest.SkipTest("KinD integration tests skipped: set SCALER_KIND_TESTS=1 to enable")

# ---------------------------------------------------------------------------
# Configuration from environment (injected by kind-test.sh)
# ---------------------------------------------------------------------------
_IMAGE = os.environ.get("SCALER_KIND_IMAGE", "scaler-worker:kind-test")
_KUBECONFIG = os.environ.get("SCALER_KIND_KUBECONFIG", os.path.expanduser("~/.kube/config"))
_NAMESPACE = os.environ.get("SCALER_KIND_NAMESPACE", "scaler-test")
_HOST = os.environ.get("SCALER_SCHEDULER_HOST", "172.18.0.1")
_PYTHON = sys.executable

# Locate CLI entry points in the same bin/ dir as the active interpreter,
# so we always use the venv that has opengris-scaler installed.
_BIN_DIR = os.path.dirname(_PYTHON)
_SCHEDULER_BIN = shutil.which("scaler_scheduler", path=_BIN_DIR) or "scaler_scheduler"
_OBJ_STORE_BIN = shutil.which("scaler_object_storage_server", path=_BIN_DIR) or "scaler_object_storage_server"
_WM_BIN = shutil.which("scaler_worker_manager", path=_BIN_DIR) or "scaler_worker_manager"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _kubectl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl", "--kubeconfig", _KUBECONFIG, *args], capture_output=True, text=True)


def _delete_all_pods() -> None:
    _kubectl("delete", "pods", "--all", "-n", _NAMESPACE)


# ---------------------------------------------------------------------------
# Base class -- starts scheduler + object storage as subprocesses via the
# installed ``scaler_*`` CLI entry points, writes a TOML config for the worker
# manager, and tears everything down in tearDown.
# ---------------------------------------------------------------------------


class KindIntegrationBase(unittest.TestCase):

    scheduler_proc: Optional[subprocess.Popen] = None
    obj_storage_proc: Optional[subprocess.Popen] = None
    worker_manager_proc: Optional[subprocess.Popen] = None
    _config_file: Optional[IO[str]] = None

    def setUp(self) -> None:
        _delete_all_pods()
        self._task_procs: List[subprocess.Popen] = []

        self.scheduler_port = _free_port()
        self.obj_storage_port = _free_port()
        self.scheduler_bind = f"tcp://0.0.0.0:{self.scheduler_port}"
        self.obj_storage_bind = f"tcp://0.0.0.0:{self.obj_storage_port}"
        self.scheduler_addr = f"tcp://127.0.0.1:{self.scheduler_port}"
        self.obj_storage_addr = f"tcp://127.0.0.1:{self.obj_storage_port}"
        self.worker_sched_addr = f"tcp://{_HOST}:{self.scheduler_port}"
        self.worker_obj_addr = f"tcp://{_HOST}:{self.obj_storage_port}"

        self.obj_storage_proc = subprocess.Popen(
            [_OBJ_STORE_BIN, self.obj_storage_bind], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        self.scheduler_proc = subprocess.Popen(
            [_SCHEDULER_BIN, self.scheduler_bind, "--object-storage-address", self.obj_storage_bind],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if not _wait_for_port("127.0.0.1", self.scheduler_port, timeout=15):
            sched_err = self.scheduler_proc.stderr.read().decode(errors="replace") if self.scheduler_proc.stderr else ""
            self.fail(f"Scheduler did not start within 15s.\nstderr:\n{sched_err}")

    def tearDown(self) -> None:
        for proc in self._task_procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            for pipe in (proc.stdout, proc.stderr):
                if pipe:
                    pipe.close()

        for proc in (self.worker_manager_proc, self.scheduler_proc, self.obj_storage_proc):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
            if proc:
                for pipe in (proc.stdout, proc.stderr):
                    if pipe:
                        pipe.close()

        if self._config_file:
            try:
                self._config_file.close()
            except Exception:
                pass

        self._assert_pods_cleaned_up()

    # ------------------------------------------------------------------
    # Infrastructure helpers
    # ------------------------------------------------------------------

    def _assert_pods_cleaned_up(self, timeout: float = 60.0) -> None:
        """Assert that the worker manager cleaned up all pods on shutdown."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = _kubectl("get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "--no-headers")
            if not r.stdout.strip():
                return
            time.sleep(2)

        r = _kubectl("get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "--no-headers")
        self.assertEqual(
            r.stdout.strip(), "", f"Worker manager failed to clean up pods after shutdown:\n{r.stdout.strip()}"
        )

    def _write_config(
        self,
        workers_per_pod: int = 1,
        max_task_concurrency: int = 4,
        wm_id: str = "wm-kind-test",
        pod_template: str = "",
    ) -> str:
        """Write a TOML config and return the file path."""
        toml = textwrap.dedent(f"""\
            [[worker_manager]]
            type = "k8s_raw"
            scheduler_address = "{self.scheduler_addr}"
            worker_scheduler_address = "{self.worker_sched_addr}"
            object_storage_address = "{self.worker_obj_addr}"
            worker_manager_id = "{wm_id}"
            max_task_concurrency = {max_task_concurrency}

            kubeconfig_path = "{_KUBECONFIG}"
            namespace = "{_NAMESPACE}"
            pod_image = "{_IMAGE}"
            workers_per_pod = {workers_per_pod}
            delete_grace_period_seconds = 5
            image_pull_policy = "Never"
        """)
        if pod_template:
            toml += f'\npod_template = """\n{pod_template}"""\n'
        self._config_file = tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False)
        self._config_file.write(toml)
        self._config_file.flush()
        return self._config_file.name

    def _start_worker_manager(self, config_path: str) -> None:
        self.worker_manager_proc = subprocess.Popen(
            [_WM_BIN, "--config", config_path, "k8s_raw", self.scheduler_addr, "--worker-manager-id", "wm-kind-test"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    # ------------------------------------------------------------------
    # Pod observation helpers
    # ------------------------------------------------------------------

    def _wait_for_pods(self, expected: int, timeout: float = 180.0) -> bool:
        """Wait until at least ``expected`` Running pods exist in the namespace."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = _kubectl(
                "get",
                "pods",
                "-n",
                _NAMESPACE,
                "-l",
                "app=scaler-worker",
                "--field-selector",
                "status.phase=Running",
                "--no-headers",
            )
            running = [line for line in r.stdout.strip().splitlines() if line]
            if len(running) >= expected:
                return True
            time.sleep(3)
        return False

    def _wait_for_pod_exists(self, timeout: float = 180.0) -> bool:
        """Wait until at least one pod (any phase) exists in the namespace."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = _kubectl("get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "--no-headers")
            if r.stdout.strip():
                return True
            time.sleep(3)
        return False

    def _wait_for_no_pods(self, timeout: float = 120.0) -> bool:
        """Wait until no pods with label app=scaler-worker exist."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = _kubectl("get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "--no-headers")
            if not r.stdout.strip():
                return True
            time.sleep(3)
        return False

    def _get_pod_names(self) -> List[str]:
        """Return names of pods with label app=scaler-worker."""
        r = _kubectl(
            "get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "-o", "jsonpath={.items[*].metadata.name}"
        )
        names = r.stdout.strip().split()
        return [n for n in names if n]

    def _pod_diagnostics(self) -> str:
        """Return a string with current pod state for failure messages."""
        pods = _kubectl("get", "pods", "-n", _NAMESPACE, "-o", "wide", "--no-headers")
        events = _kubectl("get", "events", "-n", _NAMESPACE, "--sort-by=.lastTimestamp", "--no-headers")
        wm_err = ""
        if self.worker_manager_proc and self.worker_manager_proc.stderr:
            try:
                self.worker_manager_proc.stderr.fileno()
                import select

                if select.select([self.worker_manager_proc.stderr], [], [], 0)[0]:
                    wm_err = self.worker_manager_proc.stderr.read(4096).decode(errors="replace")
            except Exception:
                pass
        return f"\n--- pods ---\n{pods.stdout or '(none)'}" f"\n--- events ---\n{events.stdout or '(none)'}" + (
            f"\n--- worker manager stderr ---\n{wm_err}" if wm_err else ""
        )

    # ------------------------------------------------------------------
    # Task submission helpers
    # ------------------------------------------------------------------

    def _submit_task(self, timeout: float = 60.0) -> object:
        """Submit a trivial task synchronously and return the result."""
        script = textwrap.dedent(f"""\
            from scaler import Client
            with Client(address="tcp://127.0.0.1:{self.scheduler_port}") as c:
                f = c.submit(round, 3.14)
                print(f.result(timeout=240))
        """)
        r = subprocess.run([_PYTHON, "-c", script], capture_output=True, text=True, timeout=timeout)
        self.assertEqual(r.returncode, 0, f"Task submission failed:\n{r.stderr}")
        return int(r.stdout.strip())

    def _start_background_task(self) -> subprocess.Popen:
        """Submit round(3.14) in a background subprocess, tracked for auto-cleanup."""
        proc = subprocess.Popen(
            [
                _PYTHON,
                "-c",
                textwrap.dedent(f"""\
                from scaler import Client
                with Client(address="tcp://127.0.0.1:{self.scheduler_port}") as c:
                    f = c.submit(round, 3.14)
                    print(f.result(timeout=240))
            """),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._task_procs.append(proc)
        return proc

    def _start_long_running_tasks(self, count: int = 1, seconds: int = 120) -> subprocess.Popen:
        """Submit ``count`` sleep-and-return tasks in a background subprocess."""
        proc = subprocess.Popen(
            [
                _PYTHON,
                "-c",
                textwrap.dedent(f"""\
                from scaler import Client

                def sleep_and_return(s):
                    import time
                    time.sleep(s)
                    return s

                with Client(address="tcp://127.0.0.1:{self.scheduler_port}") as c:
                    futures = [c.submit(sleep_and_return, {seconds}) for _ in range({count})]
                    for f in futures:
                        f.result(timeout=300)
            """),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._task_procs.append(proc)
        return proc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestKindTaskExecution(KindIntegrationBase):

    def test_single_task_returns_correct_result(self) -> None:
        """A task submitted to the scheduler should execute in a KinD pod and return the correct result."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1)
        self._start_worker_manager(cfg)
        result = self._submit_task(timeout=300)
        self.assertEqual(result, 3)

    def test_multiple_tasks_return_correct_results(self) -> None:
        """Multiple tasks submitted concurrently should all return correct results."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=4)
        self._start_worker_manager(cfg)

        script = textwrap.dedent(f"""\
            from scaler import Client
            with Client(address="tcp://127.0.0.1:{self.scheduler_port}") as c:
                futures = [c.submit(round, i + 0.14) for i in range(4)]
                results = [f.result(timeout=240) for f in futures]
                for r in results:
                    print(r)
        """)
        r = subprocess.run([_PYTHON, "-c", script], capture_output=True, text=True, timeout=360)
        self.assertEqual(r.returncode, 0, f"Multi-task submission failed:\n{r.stderr}")
        results = [int(line.strip()) for line in r.stdout.strip().splitlines()]
        self.assertEqual(results, [0, 1, 2, 3])


class TestKindScaling(KindIntegrationBase):

    def test_multiple_pods_created(self) -> None:
        """Submitting many concurrent tasks should scale up multiple pods.

        The vanilla scaling policy only scales up when task_ratio > 10
        (upper_task_ratio), so we submit 12 long-running tasks to exceed
        that threshold once the first worker connects.
        """
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=4)
        self._start_worker_manager(cfg)
        self._start_long_running_tasks(count=12, seconds=120)

        self.assertTrue(
            self._wait_for_pods(expected=2, timeout=180), "Expected at least 2 Running pods" + self._pod_diagnostics()
        )

    def test_pods_deleted_when_manager_stops(self) -> None:
        """Pods should be removed after the worker manager process is terminated."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1)
        self._start_worker_manager(cfg)
        self._start_background_task()

        self.assertTrue(
            self._wait_for_pod_exists(timeout=180), "No pods appeared before scale-down test" + self._pod_diagnostics()
        )

        self.worker_manager_proc.terminate()
        self.worker_manager_proc.wait(timeout=15)
        self.worker_manager_proc = None

        self.assertTrue(
            self._wait_for_no_pods(timeout=60), "Pods not deleted after WM shutdown" + self._pod_diagnostics()
        )

    def test_scale_down_to_zero_then_back_up(self) -> None:
        """After tasks complete and pods scale down, new tasks should trigger new pods."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1)
        self._start_worker_manager(cfg)

        result1 = self._submit_task(timeout=300)
        self.assertEqual(result1, 3)

        self.assertTrue(
            self._wait_for_no_pods(timeout=120), "Pods did not scale down to zero" + self._pod_diagnostics()
        )

        result2 = self._submit_task(timeout=300)
        self.assertEqual(result2, 3)


class TestKindLifecycle(KindIntegrationBase):

    def test_pod_restart_policy_is_never(self) -> None:
        """Every pod must be created with restartPolicy: Never."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1)
        self._start_worker_manager(cfg)
        self._start_background_task()

        self.assertTrue(
            self._wait_for_pod_exists(timeout=180), "No pods appeared within 180s" + self._pod_diagnostics()
        )

        r = _kubectl(
            "get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "-o", "jsonpath={.items[0].spec.restartPolicy}"
        )
        self.assertEqual(
            r.stdout.strip(), "Never", f"Unexpected restartPolicy: {r.stdout.strip()!r}" + self._pod_diagnostics()
        )

    def test_pods_have_correct_labels(self) -> None:
        """Pods must have app=scaler-worker and scaler/worker-manager-id labels."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1)
        self._start_worker_manager(cfg)
        self._start_background_task()

        self.assertTrue(self._wait_for_pod_exists(timeout=180), "No pods appeared" + self._pod_diagnostics())

        r = _kubectl("get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "-o", "json")
        pods = json.loads(r.stdout)
        self.assertGreater(len(pods["items"]), 0, "No pods found in JSON output")
        labels = pods["items"][0]["metadata"]["labels"]

        self.assertEqual(labels.get("app"), "scaler-worker", f"Expected label app=scaler-worker, got labels: {labels}")
        self.assertEqual(
            labels.get("scaler/worker-manager-id"),
            "wm-kind-test",
            f"Expected label scaler/worker-manager-id=wm-kind-test, got labels: {labels}",
        )

    def test_pod_template_annotations_and_tolerations_applied(self) -> None:
        """Annotations and tolerations from pod_template should appear on created pods."""
        pod_template = textwrap.dedent("""\
            metadata:
              annotations:
                test-annotation: integration-test-value
            spec:
              tolerations:
                - key: test-toleration
                  operator: Exists
                  effect: NoSchedule
        """)
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1, pod_template=pod_template)
        self._start_worker_manager(cfg)
        self._start_background_task()

        self.assertTrue(self._wait_for_pod_exists(timeout=180), "No pods appeared" + self._pod_diagnostics())

        r = _kubectl("get", "pods", "-n", _NAMESPACE, "-l", "app=scaler-worker", "-o", "json")
        pods = json.loads(r.stdout)
        self.assertGreater(len(pods["items"]), 0, "No pods found in JSON output")
        pod = pods["items"][0]

        annotations = pod["metadata"].get("annotations", {})
        self.assertEqual(
            annotations.get("test-annotation"),
            "integration-test-value",
            f"Expected annotation test-annotation=integration-test-value, got: {annotations}",
        )

        tolerations = pod["spec"].get("tolerations", [])
        matching = [t for t in tolerations if t.get("key") == "test-toleration"]
        self.assertTrue(len(matching) > 0, f"Expected toleration with key 'test-toleration', got: {tolerations}")


class TestKindPodKilledMidTask(KindIntegrationBase):

    def test_killed_pod_is_replaced(self) -> None:
        """When a pod is killed mid-task, the WM should create a replacement."""
        cfg = self._write_config(workers_per_pod=1, max_task_concurrency=1)
        self._start_worker_manager(cfg)
        self._start_long_running_tasks(count=1, seconds=120)

        self.assertTrue(
            self._wait_for_pods(expected=1, timeout=180), "Pod did not reach Running state" + self._pod_diagnostics()
        )

        original_pods = self._get_pod_names()
        self.assertEqual(len(original_pods), 1, f"Expected 1 pod, got {original_pods}")
        original_name = original_pods[0]

        r = _kubectl("delete", "pod", original_name, "-n", _NAMESPACE, "--grace-period=0", "--force")
        self.assertEqual(r.returncode, 0, f"Failed to delete pod:\n{r.stderr}")

        deadline = time.monotonic() + 180
        replacement_found = False
        while time.monotonic() < deadline:
            names = self._get_pod_names()
            new_names = [n for n in names if n != original_name]
            if new_names:
                replacement_found = True
                break
            time.sleep(3)

        self.assertTrue(
            replacement_found, f"No replacement pod appeared after killing {original_name!r}" + self._pod_diagnostics()
        )


if __name__ == "__main__":
    unittest.main()
