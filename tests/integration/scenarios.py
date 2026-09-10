"""Backend-agnostic scaling scenarios: ``(test_case, deployment)`` functions, reused by every backend,
reading task callables / timing / pool observation off the ``Deployment``. Each reports before asserting."""

from __future__ import annotations

import contextlib
import threading
import time

from scaler import Client
from tests.integration.framework import Deployment, report

# A concurrency-1 baseline for rising_load: submitted one at a time, so a single unit can serve them all.
_TRICKLE_TASKS = 4


class _PoolSample:
    def __init__(self) -> None:
        self.peak = 0
        self.created: set = set()

    def observe(self, names) -> None:
        self.peak = max(self.peak, len(names))
        self.created.update(names)


@contextlib.contextmanager
def _sampling(deployment: Deployment):
    """Poll the pool in the background while a load runs. Each provisioned unit has a fresh name, so the
    accumulated name set counts units CREATED while ``peak`` counts the most running at once."""
    sample = _PoolSample()
    stop = threading.Event()

    def poll() -> None:
        while not stop.is_set():
            sample.observe(deployment.running())
            time.sleep(deployment.profile.poll)

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    try:
        yield sample
    finally:
        stop.set()
        thread.join(timeout=5.0)


def burst_and_drain(test_case, deployment: Deployment) -> None:
    """A held burst brings the pool up with correct results, then an idle queue drains it to zero."""
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        with _sampling(deployment) as sample:
            futures = [client.submit(deployment.tasks.square, value) for value in range(profile.burst_tasks)]
            deadline = time.monotonic() + profile.scale_timeout
            while time.monotonic() < deadline and not all(future.done() for future in futures):
                time.sleep(profile.poll)
            results = [future.result() for future in futures]

    deadline = time.monotonic() + profile.drain_timeout
    while time.monotonic() < deadline and deployment.running():
        time.sleep(1.0)
    still_running = deployment.running()

    report(
        f"burst_and_drain: {len(results)} tasks ran on {len(sample.created)} unit(s), pool peaked at "
        f"{sample.peak} at once, drained to {len(still_running)} when idle"
    )
    test_case.assertEqual(results, expected, "the pool returned wrong results")
    test_case.assertGreaterEqual(sample.peak, 1, "no worker unit came up under load")
    test_case.assertEqual(still_running, [], f"pool did not drain to zero when idle ({still_running} still up)")


def rising_load(test_case, deployment: Deployment) -> None:
    """A sustained burst runs more units AT ONCE than a concurrency-1 trickle. Peak concurrent is the
    honest measure: counting distinct names would let a pool that churns one unit at a time pass. The
    burst is held continuously so a second unit has time to boot before the wave drains."""
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        with _sampling(deployment) as trickle:
            for value in range(_TRICKLE_TASKS):
                test_case.assertEqual(client.submit(deployment.tasks.square, value).result(), value * value)

        with _sampling(deployment) as burst:
            deadline = time.monotonic() + profile.scale_timeout
            while burst.peak <= trickle.peak and time.monotonic() < deadline:
                test_case.assertEqual(client.map(deployment.tasks.square, range(profile.burst_tasks)), expected)

    report(f"rising_load: trickle peaked at {trickle.peak} unit(s), sustained burst peaked at {burst.peak}")
    test_case.assertGreater(
        burst.peak, trickle.peak, "a sustained burst did not run more units at once than a concurrency-1 trickle"
    )


def steady_load_stable(test_case, deployment: Deployment) -> None:
    """A steady load settles on a stable pool: units created over the run stays close to peak concurrent,
    not thrash through provision/teardown. Steady-load only -- scale up-and-down legitimately creates > peak."""
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    with _sampling(deployment) as sample:
        with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
            deadline = time.monotonic() + profile.churn_window
            while time.monotonic() < deadline:
                wave = client.map(deployment.tasks.square, range(profile.burst_tasks))  # back-to-back waves
                test_case.assertEqual(wave, expected, "the pool returned wrong results under sustained load")

    created = len(sample.created)
    report(f"steady_load_stable: created {created} unit(s) over {profile.churn_window:.0f}s, peak {sample.peak}")
    test_case.assertGreaterEqual(created, 1, "no unit was provisioned under the steady load")
    test_case.assertLessEqual(
        created,
        sample.peak + profile.churn_tolerance,
        f"pool thrashed: created {created} units but only {sample.peak} ever ran at once (a steady load "
        f"should settle on a stable pool, not repeatedly provision and tear down)",
    )


def waterfall_spills(test_case, deployment: Deployment) -> None:
    """A saturated top pool (capped, highest priority) spills onto the next tier. The backlog is held
    continuously -- blocking waves would let the queue drain and the lower tier's desired count fall back --
    so the spill is deterministic even when the lower tier boots slowly. ``top_ran`` is asserted so a
    mis-prioritized policy that runs a lower tier first is a red, not a pass."""
    profile = deployment.profile
    top, *overflow = deployment.handles
    top_ran = False
    spilled = False

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        inflight = {}  # future -> expected result; kept topped up so the overflow load stays saturated

        def replenish() -> None:
            for value in range(profile.burst_tasks):
                inflight[client.submit(deployment.tasks.square, value)] = value * value

        replenish()
        deadline = time.monotonic() + profile.scale_timeout
        while not spilled and time.monotonic() < deadline:
            for future in [future for future in inflight if future.done()]:
                test_case.assertEqual(future.result(), inflight.pop(future))  # real workers ran the work
            if len(inflight) < profile.burst_tasks:
                replenish()
            top_ran = top_ran or bool(top.pool.running())
            spilled = any(handle.pool.running() for handle in overflow)
            time.sleep(profile.poll)
        for future, expected in list(inflight.items()):
            test_case.assertEqual(future.result(), expected)

    report(f"waterfall_spills: top tier {top.worker_manager_id} ran = {top_ran}, spilled to a lower tier = {spilled}")
    test_case.assertTrue(top_ran, "the top-priority pool never ran work before overflow spilled to a lower tier")
    test_case.assertTrue(spilled, "sustained overflow beyond the top pool's cap never spilled to a lower tier")
