"""The three worker-manager backends, each grouped with the process entry point that runs it.

A backend supplies only the seams a scenario cannot know: a boot-latency-matched ``Profile``, how to build
its task image, and how to provision one manager and observe its pool. Two of the three run the SHIPPED
AWS worker managers unmodified -- only their boto3 endpoint and a couple of config values differ from a
production deploy -- so the e2e exercises the exact code path a real ECS/EC2 deployment uses.

Each manager runs in its own spawned process, exactly like production.
"""

from __future__ import annotations

import functools
import glob
import http.server
import math
import os
import sys
import threading
from multiprocessing import get_context
from typing import Callable, List, Optional

from scaler.config.defaults import DEFAULT_CLIENT_TIMEOUT_SECONDS, DEFAULT_WORKER_TIMEOUT_SECONDS
from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from tests.integration import AWS_REGION, point_boto3_at_floci
from tests.integration.docker import (
    ECS_IMAGE,
    WORKER_IMAGE,
    DockerRuntime,
    ensure_ec2_base_image,
    ensure_ecs_image,
    ensure_worker_image,
)
from tests.integration.floci import EC2_CONTAINER_PREFIX, ECS_CONTAINER_PREFIX
from tests.integration.framework import ContainerPool, DeployContext, ManagerHandle, Profile

_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _spawn_manager(worker_manager_id: str, prefix: str, target: Callable, args: tuple) -> ManagerHandle:
    """Run one manager entry point in its own process, paired with a prefix-scoped pool observer."""
    process = get_context("spawn").Process(target=target, args=args)
    process.start()
    return ManagerHandle(worker_manager_id, ContainerPool(prefix), process)


# ---------------------------------------------------------------------------------------------------
# Container backend: no cloud at all
# ---------------------------------------------------------------------------------------------------


class ContainerWorkerProvisioner(DeclarativeWorkerProvisioner):
    """A real provisioner driven by the scheduler like the cloud managers, but each "machine" is a container
    running a fixed ``baremetal_native`` worker -- the local, free analog of a booted instance. Each machine
    gets its own IP, so spread is real."""

    def __init__(
        self, worker_scheduler_address: str, workers_per_machine: int, max_machines: int, name_prefix: str
    ) -> None:
        self._runtime = DockerRuntime()
        self._worker_scheduler_address = worker_scheduler_address  # gateway-reachable, for the containers
        self._workers = workers_per_machine
        self._name_prefix = name_prefix
        self._units: List[str] = []
        self._counter = 0
        self._coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=max_machines,
        )

    def active_unit_count(self) -> int:
        return len(self._units)

    async def set_desired_task_concurrency(self, requests) -> None:
        task_concurrency = extract_desired_count(requests, {})
        await self._coordinator.set_desired_unit_count(math.ceil(task_concurrency / self._workers))

    async def start_units(self, count: int) -> None:
        for _ in range(count):
            self._counter += 1
            name = f"{self._name_prefix}-{self._counter}"
            command = [
                "scaler_worker_manager", "baremetal_native", self._worker_scheduler_address,
                "--worker-manager-id", name,
                "--mode", "fixed",
                "--num-of-workers", str(self._workers),
                # Prefetch one task per worker so the queue stays non-empty and the vanilla policy does not
                # tear a machine down mid-burst, keeping this test double a stable scale-up harness. The
                # stock-prefetch send-to-dead-worker churn is exercised by the slow-boot EC2 topologies,
                # where boot latency far exceeds task time.
                "--per-worker-task-queue-size", "1",
            ]  # fmt: skip
            self._units.append(await self._runtime.run(WORKER_IMAGE, name, command, env={"SCALER_IT_MACHINE_ID": name}))

    async def stop_units(self, count: int) -> None:
        for _ in range(min(count, len(self._units))):
            await self._runtime.stop(self._units.pop(0))

    async def terminate(self) -> None:
        self._coordinator.cancel()
        for container in self._units:
            await self._runtime.stop(container)
        self._units.clear()


def run_container_worker_manager(
    scheduler_address: str,
    worker_scheduler_address: str,
    workers_per_machine: int,
    max_machines: int,
    worker_manager_id: str,
    name_prefix: str,
) -> None:
    """Connects to the scheduler over loopback and provisions container machines that reach it via
    ``worker_scheduler_address`` (the docker-bridge gateway)."""
    from scaler.config.defaults import DEFAULT_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_IO_THREADS
    from scaler.config.types.address import AddressConfig
    from scaler.utility.logging.utility import setup_logger
    from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

    setup_logger()
    WorkerManagerRunner(
        address=AddressConfig.from_string(scheduler_address),
        name="worker_manager_container",
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        capabilities={},
        max_provisioner_units=max_machines,
        worker_manager_id=worker_manager_id.encode(),
        worker_provisioner=ContainerWorkerProvisioner(
            worker_scheduler_address, workers_per_machine, max_machines, name_prefix
        ),
        io_threads=DEFAULT_IO_THREADS,
        workers_per_provisioner_unit=workers_per_machine,
    ).run()


class ContainerBackend:
    """Boots in seconds but churns decisively under the vanilla policy, because it is a test-double
    provisioner rather than a shipped cloud manager. A per-instance prefix lets two run at different
    waterfall tiers with distinguishable pools."""

    name = "container"
    needs_floci = False
    needs_wheel = False

    def __init__(self, prefix: str = "scaler-it-machine", workers_per_machine: int = 2, max_machines: int = 3) -> None:
        self._prefix = prefix
        self._workers_per_machine = workers_per_machine
        self._max_machines = max_machines

    def profile(self) -> Profile:
        return Profile(
            task_seconds=0.15,
            burst_tasks=30,
            scale_timeout=120.0,
            drain_timeout=90.0,
            churn_window=45.0,
            churn_tolerance=2,
            client_timeout=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout=DEFAULT_WORKER_TIMEOUT_SECONDS,
            poll=0.5,
        )

    def ensure_image(self) -> None:
        ensure_worker_image()

    def provision(self, ctx: DeployContext, worker_manager_id: str, cap: Optional[int]) -> ManagerHandle:
        # A waterfall rule caps task concurrency; translate that to a machine count.
        max_machines = self._max_machines if cap is None else max(1, math.ceil(cap / self._workers_per_machine))
        args = (
            ctx.harness.scheduler_address,
            ctx.harness.worker_scheduler_address,
            self._workers_per_machine,
            max_machines,
            worker_manager_id,
            self._prefix,
        )
        return _spawn_manager(worker_manager_id, self._prefix, run_container_worker_manager, args)


# ---------------------------------------------------------------------------------------------------
# ECS backend: the shipped ECS worker manager against floci
# ---------------------------------------------------------------------------------------------------

# floci ignores Fargate resource requests and maps every awsvpc task onto the plain bridge, so these names
# are arbitrary identifiers; they only have to be internally consistent.
_ECS_CLUSTER = "scaler-it-ecs-cluster"
_ECS_TASK_DEFINITION = "scaler-it-ecs-taskdef"
_ECS_SUBNETS = ["subnet-e2e"]


def run_ecs_worker_manager(
    scheduler_address: str,
    worker_scheduler_address: str,
    endpoint_url: str,
    ecs_task_cpu: int,
    max_task_concurrency: int,
    worker_manager_id: str,
) -> None:
    """Runs the real ``ECSWorkerManager``: via boto3 aimed at floci it provisions ECS task containers whose
    workers dial back over ``worker_scheduler_address``. ``ecs_task_cpu`` is the workers per task and the
    scale-up divisor, so the pool caps at ``ceil(max_task_concurrency / ecs_task_cpu)`` tasks."""
    point_boto3_at_floci(endpoint_url)

    from scaler.config.common.worker import WorkerConfig
    from scaler.config.common.worker_manager import WorkerManagerConfig
    from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
    from scaler.config.types.address import AddressConfig
    from scaler.config.types.worker import WorkerCapabilities
    from scaler.utility.logging.utility import setup_logger
    from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

    setup_logger()
    ECSWorkerManager(
        ECSWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_scheduler_address=AddressConfig.from_string(worker_scheduler_address),
                worker_manager_id=worker_manager_id,
                max_task_concurrency=max_task_concurrency,
            ),
            worker_config=WorkerConfig(per_worker_capabilities=WorkerCapabilities({})),
            aws_region=AWS_REGION,
            ecs_subnets=list(_ECS_SUBNETS),
            ecs_task_image=ECS_IMAGE,
            ecs_python_requirements="",  # the wheel is prebaked into the image; nothing to install at start
            ecs_task_cpu=ecs_task_cpu,
            # Fargate accepts only specific cpu/memory pairs (floci enforces this); 2 GB per vCPU is the
            # minimum valid memory at every cpu size, so register_task_definition is always accepted.
            ecs_task_memory=ecs_task_cpu * 2,
            ecs_cluster=_ECS_CLUSTER,
            ecs_task_definition=_ECS_TASK_DEFINITION,
        )
    ).run()


class FlociEcsBackend:
    """Launches real ECS ``RunTask`` sibling containers that boot in seconds from a prebaked image, so it
    needs no wheel and the stock liveness timeouts suffice."""

    name = "ecs"
    needs_floci = True
    needs_wheel = False

    def __init__(self, task_cpu: int = 2, max_task_concurrency: int = 6) -> None:
        self._task_cpu = task_cpu
        self._max_task_concurrency = max_task_concurrency

    def profile(self) -> Profile:
        return Profile(
            task_seconds=0.15,
            burst_tasks=30,
            scale_timeout=150.0,
            drain_timeout=90.0,
            churn_window=45.0,
            churn_tolerance=2,
            client_timeout=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout=DEFAULT_WORKER_TIMEOUT_SECONDS,
            poll=0.5,
        )

    def ensure_image(self) -> None:
        ensure_ecs_image()

    def provision(self, ctx: DeployContext, worker_manager_id: str, cap: Optional[int]) -> ManagerHandle:
        args = (
            ctx.harness.scheduler_address,
            ctx.harness.worker_scheduler_address,
            ctx.floci_endpoint,
            self._task_cpu,
            self._max_task_concurrency if cap is None else cap,
            worker_manager_id,
        )
        return _spawn_manager(worker_manager_id, ECS_CONTAINER_PREFIX, run_ecs_worker_manager, args)


# ---------------------------------------------------------------------------------------------------
# EC2 backend: the shipped ORB/EC2 worker manager against floci
# ---------------------------------------------------------------------------------------------------

# Where scripts/e2e/build_cibuildwheel.sh drops the portable current-source wheel.
WHEEL_DIR = os.environ.get("SCALER_IT_MANYLINUX_WHEEL_DIR", os.path.join(_REPO_ROOT, "dist_manylinux"))
# floci's describe_instance_types reports 2 vCPUs for this, which the manager uses as its scale-up divisor.
_INSTANCE_TYPE = "t3.medium"


def manylinux_wheel() -> str:
    """The newest current-source manylinux wheel. The plain `python -m build` wheel is linux_x86_64 and
    will not load on Amazon Linux 2023's older glibc, so the instances need this one."""
    wheels = sorted(glob.glob(os.path.join(WHEEL_DIR, "*manylinux*.whl")))
    if not wheels:
        raise RuntimeError(f"no manylinux wheel under {WHEEL_DIR}; build one with scripts/e2e/run.sh or "
                           f"`CIBW_OUTPUT_DIR={WHEEL_DIR} ./scripts/e2e/build_cibuildwheel.sh`")  # fmt: skip
    return wheels[-1]


def serve_wheel_on_gateway(directory: str, port: int) -> http.server.ThreadingHTTPServer:
    """Serve the local wheel over the bridge gateway so a floci EC2 instance can fetch it."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), handler)
    threading.Thread(target=server.serve_forever, name="ec2-wheel-server", daemon=True).start()
    return server


def _install_floci_ec2_compat() -> None:
    """Bridge the one place floci diverges from real AWS: for a missing launch-template NAME it returns an
    empty ``LaunchTemplates`` list where real AWS raises ``InvalidLaunchTemplateName.NotFoundException``.
    The ORB SDK relies on that exception to fall through to *creating* the template; without it, it indexes
    ``LaunchTemplates[0]`` and dies. Restoring the exception lets the shipped provisioner run unchanged."""
    import botocore.exceptions
    import botocore.handlers

    def stash_requested_names(params, context, **kwargs):
        if params.get("LaunchTemplateNames"):
            context["_requested_lt_names"] = params["LaunchTemplateNames"]

    def raise_not_found_on_empty(parsed, context, **kwargs):
        names = (context or {}).get("_requested_lt_names")
        if names and not parsed.get("LaunchTemplates"):
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "InvalidLaunchTemplateName.NotFoundException",
                           "Message": f"The specified launch template, with name {names}, does not exist."}},
                "DescribeLaunchTemplates",
            )  # fmt: skip

    # BUILTIN_HANDLERS so every later-created client (including the ORB SDK's own) picks it up.
    botocore.handlers.BUILTIN_HANDLERS.append(
        ("before-parameter-build.ec2.DescribeLaunchTemplates", stash_requested_names)
    )
    botocore.handlers.BUILTIN_HANDLERS.append(("after-call.ec2.DescribeLaunchTemplates", raise_not_found_on_empty))


def run_ec2_worker_manager(
    scheduler_address: str,
    worker_scheduler_address: str,
    endpoint_url: str,
    wheel_url: str,
    python_version: str,
    max_task_concurrency: int,
    worker_manager_id: str,
) -> None:
    """Runs the real ``ORBAWSEC2WorkerManager``: via boto3 aimed at floci it launches AL2023 instances
    whose SHIPPED UserData installs the current-source wheel from ``wheel_url`` (served over the gateway)
    and starts a worker that dials back over ``worker_scheduler_address``."""
    import tempfile

    point_boto3_at_floci(endpoint_url)
    _install_floci_ec2_compat()  # before any boto3 client (including the ORB SDK's) is created
    # ORB persists templates/config under ORB_ROOT_DIR plus a cwd-relative metrics/ dir; keep both out of
    # the repo. This process is short-lived and its own, so the dir is disposable and the chdir is safe.
    orb_root = tempfile.mkdtemp(prefix="scaler-it-orb-")
    os.environ["ORB_ROOT_DIR"] = orb_root
    os.chdir(orb_root)

    from scaler.config.common.python_worker_environment import PythonWorkerEnvironmentConfig
    from scaler.config.common.worker import WorkerConfig
    from scaler.config.common.worker_manager import WorkerManagerConfig
    from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
    from scaler.config.types.address import AddressConfig
    from scaler.config.types.network_backend import NetworkBackendType
    from scaler.config.types.worker import WorkerCapabilities
    from scaler.utility.logging.utility import setup_logger
    from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager

    setup_logger()
    ORBAWSEC2WorkerManager(
        ORBAWSEC2WorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_scheduler_address=AddressConfig.from_string(worker_scheduler_address),
                worker_manager_id=worker_manager_id,
                max_task_concurrency=max_task_concurrency,
            ),
            aws_region=AWS_REGION,
            image_id=None,  # None triggers the UserData bootstrap; floci launches AL2023 regardless of id
            python_worker_environment=PythonWorkerEnvironmentConfig(
                python_version=python_version,
                # A plain (non-git) URL requirement takes the shipped UserData's wheel-install path, so
                # nothing is compiled in the instance: uv fetches this wheel and its deps from PyPI.
                requirements_txt=f"opengris-scaler @ {wheel_url}",
            ),
            instance_type=_INSTANCE_TYPE,
            worker_config=WorkerConfig(per_worker_capabilities=WorkerCapabilities({})),
            # Match the host scheduler default; the UserData forwards this to the worker.
            network_backend=NetworkBackendType.ymq,
        )
    ).run()


class FlociEc2Backend:
    """Launches real Amazon Linux 2023 instances that install a current-source manylinux wheel and boot --
    minute-long boots, so it uses a generous profile and cloud-realistic liveness timeouts."""

    name = "ec2"
    needs_floci = True
    needs_wheel = True

    def __init__(self, max_task_concurrency: int = 4) -> None:
        self._max_task_concurrency = max_task_concurrency

    def profile(self) -> Profile:
        return Profile(
            task_seconds=0.15,
            burst_tasks=24,
            scale_timeout=420.0,
            drain_timeout=180.0,
            churn_window=180.0,
            churn_tolerance=3,
            client_timeout=300,
            worker_timeout=120,
            poll=1.0,
        )

    def ensure_image(self) -> None:
        ensure_ec2_base_image()

    def provision(self, ctx: DeployContext, worker_manager_id: str, cap: Optional[int]) -> ManagerHandle:
        args = (
            ctx.harness.scheduler_address,
            ctx.harness.worker_scheduler_address,
            ctx.floci_endpoint,
            ctx.wheel_url,
            _PYTHON_VERSION,
            self._max_task_concurrency if cap is None else cap,
            worker_manager_id,
        )
        return _spawn_manager(worker_manager_id, EC2_CONTAINER_PREFIX, run_ec2_worker_manager, args)
