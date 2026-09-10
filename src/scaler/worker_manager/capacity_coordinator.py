from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from scaler.config.defaults import DEFAULT_WORKER_MANAGER_SCALE_DOWN_COOLDOWN_SECONDS
from scaler.utility.cooldown import Cooldown

logger = logging.getLogger(__name__)


class CapacityCoordinator:
    """Manages async scale-up/down reconciliation for a pool of homogeneous units.

    Callers set a desired unit count via `set_desired_unit_count`. The loop
    compares that against the live count returned by `active_unit_count` and
    calls `start_units` or `stop_units` with the delta. A single long-lived task
    blocks on an asyncio.Event between reconciles so it only wakes when a new
    desired count has been signalled; rapid successive calls are coalesced because
    the task always reads the latest desired count when it wakes.

    If `scale_down_cooldown_seconds` is positive, a scale-down is deferred until that many
    seconds have passed since the first scale-down request in the current streak;
    further decreases while deferred do not extend the window, but a scale-up (or the
    desired count returning to/above the live count) cancels it. This avoids flapping
    under intermittent load. A deferred scale-down is retried the next time
    `set_desired_unit_count` is called, even with an unchanged count (e.g. on the next
    heartbeat).

    Call `cancel()` to stop the reconcile task on shutdown.

    Args:
        start_units: Async callable that launches `n` new units.
        stop_units: Async callable that terminates `n` existing units.
        active_unit_count: Callable that returns the current live unit count.
        max_unit_count: Hard cap on the number of units. -1 means unlimited.
        scale_down_cooldown_seconds: Minimum seconds a scale-down must be requested for before
            it is honored. 0 disables the cooldown.
    """

    def __init__(
        self,
        start_units: Callable[[int], Awaitable[None]],
        stop_units: Callable[[int], Awaitable[None]],
        active_unit_count: Callable[[], int],
        max_unit_count: int,
        scale_down_cooldown_seconds: float = DEFAULT_WORKER_MANAGER_SCALE_DOWN_COOLDOWN_SECONDS,
    ) -> None:
        self._start_units = start_units
        self._stop_units = stop_units
        self._active_unit_count = active_unit_count
        self._max_unit_count = max_unit_count
        self._scale_down_cooldown = Cooldown(scale_down_cooldown_seconds)
        self._desired_unit_count: int = 0
        self._active_reconcile_task: Optional[asyncio.Task] = None
        self._reconcile_needed: asyncio.Event = asyncio.Event()
        self._stop: asyncio.Event = asyncio.Event()

    async def set_desired_unit_count(self, count: int) -> None:
        """Set the desired number of units and signal the reconcile task.

        Also re-signals when *count* is unchanged but a scale-down is deferred in cooldown,
        so callers that re-assert the same desired count on a fixed interval (e.g. every
        heartbeat) will trigger a retry once the cooldown window has elapsed.
        """
        # An unchanged count is only a true no-op if nothing is pending: while a scale-down is
        # deferred, we must keep signalling or it would never be retried once the cooldown ends.
        if count == self._desired_unit_count and not self._scale_down_cooldown.is_running:
            return
        if count != self._desired_unit_count:
            logger.info(f"Desired unit count changed: {self._desired_unit_count} -> {count}")
            self._desired_unit_count = count
        self._reconcile_needed.set()
        if self._active_reconcile_task is None:
            self._active_reconcile_task = asyncio.create_task(self._reconcile())

    def cancel(self) -> None:
        """Stop the reconcile task. Safe to call multiple times."""
        self._stop.set()
        self._reconcile_needed.set()  # unblock any waiting
        if self._active_reconcile_task is not None:
            self._active_reconcile_task.cancel()

    def __del__(self) -> None:
        self.cancel()

    async def _reconcile(self) -> None:
        try:
            while not self._stop.is_set():
                await self._reconcile_needed.wait()
                self._reconcile_needed.clear()
                if self._stop.is_set():
                    break

                desired = self._desired_unit_count
                current = self._active_unit_count()
                delta = desired - current
                if self._max_unit_count != -1:
                    delta = min(delta, self._max_unit_count - current)
                capped = self._max_unit_count != -1 and delta != desired - current
                msg = f"Reconcile: desired={desired}, current={current}, delta={delta:+d}" + (
                    f" (capped by max_unit_count={self._max_unit_count})" if capped else ""
                )
                if delta != 0:
                    logger.info(msg)
                else:
                    logger.debug(msg)

                if delta >= 0:
                    self._scale_down_cooldown.reset()
                else:
                    # Anchors to the first scale-down request in the streak; further requests while
                    # deferred don't extend the window. When it expires, we act on whatever
                    # desired/delta is current then - not the value that first triggered the cooldown.
                    self._scale_down_cooldown.start_if_not_running()
                    remaining = self._scale_down_cooldown.remaining_seconds()
                    if remaining is not None:
                        logger.info(
                            f"Deferring scale-down of {abs(delta)} unit(s): scale-down cooldown active "
                            f"({remaining:.1f}s remaining)"
                        )
                        continue

                try:
                    if delta > 0:
                        await self._start_units(delta)
                    elif delta < 0:
                        await self._stop_units(abs(delta))
                        self._scale_down_cooldown.reset()
                except Exception:
                    logger.exception("Reconcile failed")
        finally:
            self._active_reconcile_task = None
