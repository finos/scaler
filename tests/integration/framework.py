"""What a scenario is written against: a ``Deployment`` (a scheduler bound to one or more worker managers),
the ``Profile`` that sizes it, and the ``deploy`` builder that composes the shared infrastructure.

Every backend -- container "machines", ECS task containers, EC2 instance containers -- runs its workers in
host Docker containers, so one prefix-scoped ``docker ps`` observes any pool. That is what lets a single
set of scenarios drive all of them.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess
from multiprocessing.process import BaseProcess
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.integration import container_cli
from tests.integration.docker import host_gateway
from tests.integration.floci import FlociEmulator
from tests.integration.harness import SchedulerHarness, terminate_process
from tests.utility.utility import logging_test_name


def report(message: str) -> None:
    """Print each line under an ``[e2e]`` prefix, so a run's story greps out of a noisy CI log."""
    for line in message.splitlines():
        print(f"[e2e] {line}", flush=True)


@dataclasses.dataclass(frozen=True)
class Profile:
    """Scenario timing and sizing, supplied by a backend and matched to its boot latency (a container boots
    in seconds, an EC2 instance in minutes), so the same scenario stays honest on each."""

    task_seconds: float
    burst_tasks: int
    scale_timeout: float  # how long the pool may take to react to load
    drain_timeout: float  # how long an idle pool may take to reach zero
    churn_window: float  # how long a steady load is held while watching the pool settle
    churn_tolerance: int
    client_timeout: int
    worker_timeout: int
    poll: float


@dataclasses.dataclass(frozen=True)
class Tasks:
    """Task callables. Nested functions, so cloudpickle serializes them BY VALUE: no task image carries the
    test module, so a module-level (by-reference) task would fail to import on the worker."""

    square: Callable[[int], int]
    square_tagged: Callable[[int], Tuple[int, str]]


def make_tasks(task_seconds: float) -> Tasks:
    def square(value: int) -> int:
        import time

        time.sleep(task_seconds)
        return value * value

    def square_tagged(value: int) -> Tuple[int, str]:
        import os
        import socket
        import time

        time.sleep(task_seconds)
        # The container backend tags each machine by env; the shipped AWS provisioners set no id, so their
        # tasks fall back to the per-container hostname. One expression covers both.
        return value * value, os.environ.get("SCALER_IT_MACHINE_ID") or socket.gethostname()

    return Tasks(square=square, square_tagged=square_tagged)


class ContainerPool:
    """One manager's containers, observed by name prefix."""

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix

    def _names(self, include_stopped: bool = False) -> List[str]:
        result = subprocess.run(
            [*container_cli(), "ps", *(["-a"] if include_stopped else []), "--filter", f"name={self.prefix}",
             "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
        )  # fmt: skip
        # docker's name= filter is an unanchored substring match, so anchor to the real `<prefix>-<n>`
        # scheme -- otherwise one tier's prefix matches another's containers (`scaler-it-a` would match
        # `scaler-it-ab`), inflating its count or cross-reaping it on teardown.
        return [name for name in result.stdout.split() if name.startswith(self.prefix + "-")]

    def running(self) -> List[str]:
        return self._names()

    def reap(self) -> None:
        """Last-resort teardown for units a killed manager could not drain."""
        for name in self._names(include_stopped=True):
            subprocess.run([*container_cli(), "rm", "-f", name], capture_output=True)


@dataclasses.dataclass
class ManagerHandle:
    worker_manager_id: str
    pool: ContainerPool
    process: BaseProcess


@dataclasses.dataclass
class DeployContext:
    """Shared infrastructure a backend provisions its manager against (built once by :func:`deploy`)."""

    harness: SchedulerHarness
    floci_endpoint: Optional[str]
    wheel_url: Optional[str]


class WorkerManagerBackend(Protocol):
    """How to provision ONE worker manager and observe its pool. Concrete backends live in backends.py; all
    shared logic (harness, floci, images) is composed in, never inherited."""

    name: str
    needs_floci: bool
    needs_wheel: bool

    def profile(self) -> Profile: ...

    def ensure_image(self) -> None: ...

    def provision(self, ctx: DeployContext, worker_manager_id: str, cap: Optional[int]) -> ManagerHandle: ...


@dataclasses.dataclass
class ManagerSpec:
    """A backend to run at one waterfall tier, optionally capped at a max task concurrency."""

    backend: WorkerManagerBackend
    cap: Optional[int] = None


class Deployment:
    """Scenarios read :attr:`profile`, submit via :attr:`tasks`, and observe the whole pool with
    :meth:`running`."""

    def __init__(self, harness: SchedulerHarness, handles: List[ManagerHandle], profile: Profile) -> None:
        self.harness = harness
        self.handles = handles
        self.profile = profile
        self.tasks = make_tasks(profile.task_seconds)

    def running(self) -> List[str]:
        """The names of every unit currently up, across every manager."""
        return [name for handle in self.handles for name in handle.pool.running()]

    def assert_healthy(self, test_case) -> None:
        """Fail the test naming the fault when the scheduler or a manager crashed or wedged. Call from
        tearDown (before addCleanup tears the processes down), so a bare client TimeoutError -- usually the
        downstream symptom -- becomes an actionable failure."""
        dead = [f"{handle.worker_manager_id} (exit {handle.process.exitcode})"
                for handle in self.handles if not handle.process.is_alive()]  # fmt: skip
        harness_dead = self.harness.died()
        if harness_dead:
            dead.append(harness_dead)
        # The scheduler can log its traceback a beat after the client gives up, so wait for it to land only
        # when a fault is already evident; a green run scans the log once and returns at once.
        error = self.harness.unhandled_error(settle_seconds=4.0 if dead else 0.0)
        if dead:
            test_case.fail(
                f"{', '.join(dead)} exited during the run -- an unhandled crash under churn, not a slow "
                f"client. See the traceback in the log above." + (f" Scheduler logged: {error}." if error else "")
            )
        if error:
            test_case.fail(
                f"scheduler logged an unhandled exception under churn and wedged the pool ({error}); any "
                f"client TimeoutError above is the downstream symptom. Full traceback in the log above."
            )


def _merge_profiles(profiles: List[Profile]) -> Profile:
    """Combine a mixed deployment's profiles: the most generous of each knob, so the slowest backend's boot
    latency governs the timeouts and the deepest load governs the counts."""
    if len(profiles) == 1:
        return profiles[0]
    return Profile(**{
        field.name: max(getattr(profile, field.name) for profile in profiles)
        for field in dataclasses.fields(Profile)
    })  # fmt: skip


def _env(name: str) -> Optional[str]:
    """An on-demand-run knob. A workflow_dispatch input expands to an empty string on the labeled-PR
    trigger, so empty must read the same as absent."""
    return os.environ.get(name, "").strip() or None


def _env_number(name: str, parse: Callable):
    raw = _env(name)
    if raw is None:
        return None
    try:
        return parse(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r} is not a valid {parse.__name__}")


def _apply_overrides(profile: Profile) -> Profile:
    """Resize the load from the environment, at one chokepoint so every backend and the merged mixed
    profile are covered. The timeouts stay fixed, so a much larger count or task_seconds can exceed them."""
    changes: Dict[str, Any] = {}  # heterogeneous: task_seconds is a float, burst_tasks an int
    task_seconds = _env_number("SCALER_IT_TASK_SECONDS", float)
    if task_seconds is not None:
        changes["task_seconds"] = task_seconds
    burst_tasks = _env_number("SCALER_IT_NUM_TASKS", int)
    if burst_tasks is not None:
        if burst_tasks < 1:
            raise ValueError(f"SCALER_IT_NUM_TASKS={burst_tasks} must be >= 1")
        changes["burst_tasks"] = burst_tasks
    return dataclasses.replace(profile, **changes) if changes else profile


def _default_waterfall_policy(specs: List[ManagerSpec], manager_ids: List[str]) -> str:
    """Priority = position (1 = highest); a spec's cap becomes its rule's max_task_concurrency."""
    return "\n".join(
        f"{position},{manager_id}" + (f",{spec.cap}" if spec.cap is not None else "")
        for position, (manager_id, spec) in enumerate(zip(manager_ids, specs), start=1)
    )


def _caps_from_policy(policy_content: str, manager_ids: List[str]) -> Dict[str, Optional[int]]:
    """Read each manager's cap back from the effective policy with the scheduler's own parser, so a raw
    SCALER_IT_WATERFALL_POLICY sizes the provisioned pools too, not just the scheduler's spill threshold.
    A misnamed id fails fast (it would otherwise leave that tier with no desired concurrency: a false red)."""
    from scaler.scheduler.controllers.policies.waterfall_v1.scaling.utility import parse_waterfall_rules

    caps = {
        rule.worker_manager_id.decode(): rule.max_task_concurrency for rule in parse_waterfall_rules(policy_content)
    }
    missing = [manager_id for manager_id in manager_ids if manager_id not in caps]
    if missing:
        raise ValueError(
            f"waterfall policy has no rule for {missing}; provisioned managers are {manager_ids} "
            f"(one rule per line: 'priority,worker_manager_id[,max_task_concurrency]')"
        )
    return caps


def _resolve_policy(topology: str, specs: List[ManagerSpec], manager_ids: List[str]):
    """Return (SchedulerHarness policy kwargs, {manager_id: cap}). One manager runs vanilla (harness
    default); several run waterfall_v1, overridable by SCALER_IT_WATERFALL_POLICY."""
    override = _env("SCALER_IT_WATERFALL_POLICY")
    if len(specs) == 1:
        if override:
            report(f"ignoring SCALER_IT_WATERFALL_POLICY: {topology} is single-manager (vanilla)")
        return {}, {manager_ids[0]: specs[0].cap}
    content = override or _default_waterfall_policy(specs, manager_ids)
    report(f"policy waterfall_v1:\n{content}")
    return {"policy_content": content, "policy_engine_type": "waterfall_v1"}, _caps_from_policy(content, manager_ids)


def _start_floci(test_case) -> str:
    floci = FlociEmulator()
    # Registered BEFORE start(): a start() that brings the container up then times out on readiness must
    # still reap the privileged emulator (it holds the host docker socket).
    test_case.addCleanup(floci.stop)
    floci.start()
    return floci.endpoint_url


def _serve_wheel(test_case) -> str:
    from tests.integration.backends import WHEEL_DIR, manylinux_wheel, serve_wheel_on_gateway

    port = get_available_tcp_port()
    test_case.addCleanup(serve_wheel_on_gateway(WHEEL_DIR, port).shutdown)
    return f"http://{host_gateway()}:{port}/{os.path.basename(manylinux_wheel())}"


def deploy(test_case, topology: str, specs: List[ManagerSpec]) -> Deployment:
    """Bring up the scheduler + every manager, registering teardown on ``test_case``. One manager runs
    vanilla; several run waterfall_v1 at priority = position, so ``deploy(self, "ecs_ec2", [ecs, ec2])`` is
    a cross-backend waterfall. Overridable via SCALER_IT_NUM_TASKS / _TASK_SECONDS / _WATERFALL_POLICY."""
    setup_logger()
    logging_test_name(test_case)
    backends = [spec.backend for spec in specs]
    profile = _apply_overrides(_merge_profiles([backend.profile() for backend in backends]))
    manager_ids = [f"wm-{spec.backend.name}-p{position}" for position, spec in enumerate(specs, start=1)]

    for backend in backends:
        backend.ensure_image()

    floci_endpoint = _start_floci(test_case) if any(backend.needs_floci for backend in backends) else None
    wheel_url = _serve_wheel(test_case) if any(backend.needs_wheel for backend in backends) else None

    policy_kwargs, caps = _resolve_policy(topology, specs, manager_ids)
    harness = SchedulerHarness(
        gateway=host_gateway(),
        client_timeout_seconds=profile.client_timeout,
        worker_timeout_seconds=profile.worker_timeout,
        **policy_kwargs,
    )
    test_case.addCleanup(harness.shutdown)
    report(
        f"topology {topology}: managers {manager_ids} caps {[caps[manager_id] for manager_id in manager_ids]}, "
        f"{profile.burst_tasks} tasks x {profile.task_seconds}s"
    )

    context = DeployContext(harness=harness, floci_endpoint=floci_endpoint, wheel_url=wheel_url)
    handles = []
    for manager_id, spec in zip(manager_ids, specs):
        handle = spec.backend.provision(context, manager_id, caps[manager_id])
        # LIFO teardown: terminate the manager (registered last) before reaping any container it orphaned.
        test_case.addCleanup(handle.pool.reap)
        test_case.addCleanup(terminate_process, handle.process)
        handles.append(handle)

    return Deployment(harness, handles, profile)
