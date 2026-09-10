from __future__ import annotations

import asyncio
import functools
import logging
import math
import shlex
import threading
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

import kubernetes
import kubernetes.client
import kubernetes.client.exceptions
import kubernetes.config
import kubernetes.watch
import yaml
from kubernetes.client import ApiClient, V1Container, V1EnvVar, V1ObjectMeta, V1Pod, V1PodSpec

from scaler.config.section.kubernetes_worker_manager import KubernetesWorkerManagerConfig
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count, format_capabilities
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Return a new dict that is `base` deep-merged with `override`.

    Merge rules: nested dicts are merged recursively; lists replace the base list
    entirely (no element-level merge); scalars use the override value.
    """
    result: Dict[str, Any] = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


class KubernetesWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: KubernetesWorkerManagerConfig, max_pods: int) -> None:
        self._config = config
        self._capabilities: Dict[str, int] = config.worker_config.per_worker_capabilities.capabilities

        # Load Kubernetes auth
        if config.kubeconfig_path:
            kubernetes.config.load_kube_config(config.kubeconfig_path)
        else:
            kubernetes.config.load_incluster_config()
        self._core_v1 = kubernetes.client.CoreV1Api()

        # Pod names owned by this provisioner (oldest first)
        self._pods: List[str] = []

        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=max_pods,
        )

        self._watch_stop = threading.Event()
        self._watch_thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Pod watch
    # ------------------------------------------------------------------

    def _start_pod_watch(self) -> None:
        if self._watch_thread is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._watch_thread = threading.Thread(target=self._pod_watch_loop, daemon=True)
        self._watch_thread.start()

    def _pod_watch_loop(self) -> None:
        label_selector = f"scaler/worker-manager-id={self._config.worker_manager_config.worker_manager_id}"
        watcher = kubernetes.watch.Watch()

        while not self._watch_stop.is_set():
            try:
                for event in watcher.stream(
                    self._core_v1.list_namespaced_pod,
                    self._config.namespace,
                    label_selector=label_selector,
                    timeout_seconds=300,
                ):
                    if self._watch_stop.is_set():
                        watcher.stop()
                        return

                    event_type = event["type"]
                    pod_name = event["object"].metadata.name

                    if event_type == "DELETED" and pod_name in self._pods:
                        logger.warning(f"Watch: pod {pod_name!r} was deleted externally, removing from tracking")
                        self._pods.remove(pod_name)
                        self._loop.call_soon_threadsafe(
                            asyncio.ensure_future, self._capacity_coordinator.request_reconcile()
                        )
            except kubernetes.client.exceptions.ApiException as e:
                if self._watch_stop.is_set():
                    return
                logger.error(f"Pod watch API error (will reconnect): {e}")
            except Exception as e:
                if self._watch_stop.is_set():
                    return
                logger.error(f"Pod watch error (will reconnect): {e}")

    def _stop_pod_watch(self) -> None:
        self._watch_stop.set()
        if self._watch_thread is not None:
            self._watch_thread.join(timeout=5)
            self._watch_thread = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_worker_command(self) -> str:
        config = self._config
        worker_config = config.worker_config
        scheduler_address = str(config.worker_manager_config.effective_worker_scheduler_address)

        command = (
            f"scaler_worker_manager baremetal_native {scheduler_address!r} "
            f"--mode fixed "
            f"--worker-type KUBERNETES "
            f"--max-task-concurrency {config.workers_per_pod} "
            f"--worker-manager-id {config.worker_manager_config.worker_manager_id} "
            f"--per-worker-task-queue-size {worker_config.per_worker_task_queue_size} "
            f"--heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} "
            f"--task-timeout-seconds {worker_config.task_timeout_seconds} "
            f"--garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} "
            f"--death-timeout-seconds {worker_config.death_timeout_seconds} "
            f"--trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} "
            f"--event-loop {worker_config.event_loop} "
            f"--io-threads {worker_config.io_threads}"
        )

        if worker_config.hard_processor_suspend:
            command += " --hard-processor-suspend"

        object_storage_address = config.worker_manager_config.object_storage_address
        if object_storage_address is not None:
            command += f" --object-storage-address {object_storage_address!r}"

        capabilities_str = format_capabilities(self._capabilities).strip()
        if capabilities_str:
            command += f" --per-worker-capabilities {capabilities_str}"

        if worker_config.preload is not None:
            command += f" --preload {shlex.quote(worker_config.preload)}"

        return command

    # ------------------------------------------------------------------
    # DeclarativeWorkerProvisioner interface
    # ------------------------------------------------------------------

    def active_unit_count(self) -> int:
        return len(self._pods)

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        self._start_pod_watch()
        task_concurrency = extract_desired_count(requests, self._capabilities)
        desired_pods = math.ceil(task_concurrency / self._config.workers_per_pod)
        await self._capacity_coordinator.set_desired_unit_count(desired_pods)

    async def start_units(self, count: int) -> None:
        config = self._config
        command = self._build_worker_command()
        loop = asyncio.get_running_loop()

        for _ in range(count):
            pod_name = f"scaler-worker-{uuid.uuid4().hex[:12]}"

            env_vars = [V1EnvVar(name="COMMAND", value=command)]
            pwe = config.python_worker_environment
            if pwe.requirements_txt is not None:
                env_vars.append(V1EnvVar(name="PYTHON_REQUIREMENTS", value=pwe.requirements_txt))
            if pwe.python_version is not None:
                env_vars.append(V1EnvVar(name="PYTHON_VERSION", value=pwe.python_version))

            pod_manifest = V1Pod(
                metadata=V1ObjectMeta(
                    name=pod_name,
                    namespace=config.namespace,
                    labels={
                        "app": "scaler-worker",
                        "scaler/worker-manager-id": config.worker_manager_config.worker_manager_id,
                    },
                ),
                spec=V1PodSpec(
                    restart_policy="Never",
                    containers=[V1Container(name="scaler-worker", image=config.pod_image, env=env_vars)],
                ),
            )

            try:
                pod_dict: Dict[str, Any] = cast(Dict[str, Any], ApiClient().sanitize_for_serialization(pod_manifest))

                # Layer 1: pod_template YAML — deep-merged as a base layer.
                if config.pod_template.strip():
                    template = yaml.safe_load(config.pod_template)
                    if not isinstance(template, dict):
                        raise ValueError(f"pod_template must be a YAML mapping, got {type(template).__name__}")
                    pod_dict = _deep_merge(pod_dict, template)

                # Config fields override the template.
                if config.node_selector:
                    pod_dict["spec"]["nodeSelector"] = config.node_selector

                if config.service_account_name:
                    pod_dict["spec"]["serviceAccountName"] = config.service_account_name

                if config.image_pull_policy:
                    pod_dict["spec"]["containers"][0]["imagePullPolicy"] = config.image_pull_policy

                if config.resource_requests or config.resource_limits:
                    resources = pod_dict["spec"]["containers"][0].setdefault("resources", {})
                    if config.resource_requests:
                        resources["requests"] = config.resource_requests
                    if config.resource_limits:
                        resources["limits"] = config.resource_limits

                # Invariant: restartPolicy must always be Never.
                pod_dict["spec"]["restartPolicy"] = "Never"

                await loop.run_in_executor(
                    None, functools.partial(self._core_v1.create_namespaced_pod, config.namespace, pod_dict)
                )
                self._pods.append(pod_name)
                logger.info(f"Started Kubernetes pod {pod_name!r}")
            except kubernetes.client.exceptions.ApiException as e:
                logger.error(f"Failed to create Kubernetes pod {pod_name!r}: {e}")

    async def stop_units(self, count: int) -> None:
        config = self._config
        loop = asyncio.get_running_loop()

        to_stop = self._pods[:count]
        if len(to_stop) < count:
            logger.warning(f"Requested to stop {count} pod(s) but only {len(to_stop)} available.")

        for pod_name in to_stop:
            try:
                await loop.run_in_executor(
                    None,
                    functools.partial(
                        self._core_v1.delete_namespaced_pod,
                        pod_name,
                        config.namespace,
                        grace_period_seconds=config.delete_grace_period_seconds,
                    ),
                )
                self._pods.remove(pod_name)
                logger.info(f"Stopped Kubernetes pod {pod_name!r}")
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == 404:
                    self._pods.remove(pod_name)
                    logger.warning(f"Pod {pod_name!r} not found during deletion (already gone)")
                else:
                    logger.error(f"Failed to delete pod {pod_name!r}: {e}")

    async def terminate(self) -> None:
        self._stop_pod_watch()
        self._capacity_coordinator.cancel()
        await self.stop_units(len(self._pods))


class KubernetesWorkerManager:
    def __init__(self, config: KubernetesWorkerManagerConfig) -> None:
        workers_per_pod = config.workers_per_pod
        mtc = config.worker_manager_config.max_task_concurrency
        max_pods = math.ceil(mtc / workers_per_pod) if mtc != -1 else -1
        provisioner = KubernetesWorkerProvisioner(config, max_pods)
        self._runner = WorkerManagerRunner(
            address=config.worker_manager_config.scheduler_address,
            name="worker_manager_k8s",
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            capabilities=config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=max_pods,
            worker_manager_id=config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=config.worker_config.io_threads,
            workers_per_provisioner_unit=workers_per_pod,
        )

    def run(self) -> None:
        self._runner.run()
