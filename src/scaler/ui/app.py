import asyncio
import dataclasses
import datetime
import hashlib
import json
import logging
import queue
import struct
import threading
from collections import deque
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from scaler.config.section.webgui import WebGUIConfig
from scaler.io.mixins import SyncSubscriber
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    StateBalanceAdvice,
    StateScheduler,
    StateTask,
    StateWorker,
    TaskState,
    WorkerState,
)
from scaler.protocol.helpers import capabilities_to_dict
from scaler.utility.formatter import format_bytes, format_microseconds, format_percentage, format_seconds
from scaler.utility.identifiers import WorkerID
from scaler.utility.metadata.profile_result import ProfileResult

_logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

COMPLETED_TASK_STATUSES = (
    TaskState.success,
    TaskState.canceled,
    TaskState.canceledNotFound,
    TaskState.failed,
    TaskState.failedWorkerDied,
    TaskState.failedSchedulerFault,
)

SLIDING_WINDOW_OPTIONS = {
    5: datetime.timedelta(minutes=5),
    10: datetime.timedelta(minutes=10),
    30: datetime.timedelta(minutes=30),
}

DEFAULT_STREAM_WINDOW_MINUTES = 5

# Rows per page. Server-side only: the browser is told which page it got and how many exist, never
# the size, so it never has to agree with these.
WORKERS_PAGE_SIZE = 50
PROCESSORS_PAGE_SIZE = 20
STREAM_PAGE_SIZE = 50

# Columns the workers table can be sorted by, mirroring the table's own columns.
WORKER_SORT_NUMERIC_FIELDS = frozenset(
    {"agt_cpu", "agt_rss", "proc_cpu", "proc_rss", "mem_used_pct", "free", "sent", "queued", "suspended"}
)
# Columns whose display value is preformatted; sort them by the raw number behind it instead.
WORKER_SORT_RAW_FIELDS = {"lag": "lag_us", "last_seen": "last_s"}
WORKER_SORT_FIELDS = frozenset(
    WORKER_SORT_NUMERIC_FIELDS | set(WORKER_SORT_RAW_FIELDS) | {"name", "manager_id", "itl", "capabilities"}
)


@dataclasses.dataclass
class ClientView:
    """What one browser is looking at. Held per socket, so viewers never move each other's view."""

    workers_page: int = 0
    workers_sort: Optional[str] = None
    workers_sort_ascending: bool = True
    processors_page: int = 0
    stream_page: int = 0
    stream_window_minutes: int = DEFAULT_STREAM_WINDOW_MINUTES
    memory_scale: str = "linear"

    def apply_view(self, view: Dict[str, Any]) -> None:
        """Apply a browser's `view` message, ignoring anything unrecognised."""
        for name in ("workers_page", "processors_page", "stream_page"):
            if name in view:
                setattr(self, name, max(0, int(view[name])))

        if "workers_sort" in view:
            field = view["workers_sort"]
            self.workers_sort = str(field) if field in WORKER_SORT_FIELDS else None
        if "workers_sort_ascending" in view:
            self.workers_sort_ascending = bool(view["workers_sort_ascending"])

    def apply_settings(self, settings: Dict[str, Any]) -> None:
        """Apply a browser's `settings` message, ignoring anything unrecognised."""
        if "stream_window" in settings:
            window = int(settings["stream_window"])
            if window in SLIDING_WINDOW_OPTIONS:
                self.stream_window_minutes = window
        if "memory_scale" in settings:
            scale = str(settings["memory_scale"])
            if scale in ("log", "linear"):
                self.memory_scale = scale

    def settings(self) -> Dict[str, Any]:
        return {"stream_window": self.stream_window_minutes, "memory_scale": self.memory_scale}


class _RenderCache:
    """Memoizes the whole-fleet work (sort, grouping, stream render) for one tick, so N browsers
    sharing a sort column cost one sort rather than N."""

    def __init__(self) -> None:
        self._sorted_workers: Dict[Tuple[Optional[str], bool], List[Dict[str, Any]]] = {}
        self._processors: Optional[List[Dict[str, Any]]] = None
        self._stream: Dict[int, Dict[str, Any]] = {}
        self._memory: Dict[Tuple[float, str], Dict[str, Any]] = {}

    def sorted_workers(self, app: "WebUIApp", view: ClientView) -> List[Dict[str, Any]]:
        key = (view.workers_sort, view.workers_sort_ascending)
        if key not in self._sorted_workers:
            self._sorted_workers[key] = app._sorted_workers(*key)
        return self._sorted_workers[key]

    def processors(self, app: "WebUIApp") -> List[Dict[str, Any]]:
        if self._processors is None:
            self._processors = app._build_processors_data()
        return self._processors

    def stream(self, app: "WebUIApp", window_minutes: int) -> Dict[str, Any]:
        if window_minutes not in self._stream:
            stream_data = app._task_stream.get_render_data(window_minutes)
            app._enrich_stream_with_managers(stream_data)
            self._stream[window_minutes] = stream_data
        return self._stream[window_minutes]

    def memory(self, app: "WebUIApp", window_seconds: float, scale: str) -> Dict[str, Any]:
        key = (window_seconds, scale)
        if key not in self._memory:
            self._memory[key] = app._memory_chart.get_render_data(window_seconds, scale)
        return self._memory[key]


def paginate(items: List[Any], page: int, size: int) -> Tuple[List[Any], int, int]:
    """Slice `items` into the requested page, clamped to what exists: (rows, page, total pages)."""
    total_pages = max(1, (len(items) + size - 1) // size)
    page = min(max(page, 0), total_pages - 1)
    return items[page * size : page * size + size], page, total_pages


def _format_worker_name(worker_name: str, cutoff: int = 15) -> str:
    if len(worker_name) <= cutoff:
        return worker_name
    return worker_name[:cutoff] + "+"


# Minimum angular distance (degrees) between any two assigned hues.
# 30 deg allows ~12 maximally-distinct slots; beyond that the algorithm
# degrades gracefully by placing new hues in the largest available gap.
_MIN_HUE_DISTANCE = 30


def _hue_distance(a: float, b: float) -> float:
    """Angular distance between two hues on the 360 deg wheel."""
    d = abs(a - b) % 360
    return min(d, 360 - d)


def _extract_hue(hsl_str: str) -> Optional[float]:
    """Extract hue from an ``hsl(H,S%,L%)`` string. Returns *None* for non-HSL values."""
    if not hsl_str.startswith("hsl("):
        return None
    try:
        return float(hsl_str[4 : hsl_str.index(",")])
    except (ValueError, IndexError):
        return None


def _find_best_hue(preferred_hue: float, existing_hues: List[float]) -> float:
    """Return *preferred_hue* if it is far enough from every existing hue,
    otherwise place the new hue at the midpoint of the largest angular gap."""
    if not existing_hues:
        return preferred_hue

    # Check whether the preferred hue has enough distance from all existing ones.
    if all(_hue_distance(preferred_hue, h) >= _MIN_HUE_DISTANCE for h in existing_hues):
        return preferred_hue

    # Find the largest gap on the hue wheel and place the new hue at its midpoint.
    sorted_hues = sorted(existing_hues)
    best_gap = 0.0
    best_mid = preferred_hue  # fallback

    for i in range(len(sorted_hues)):
        next_hue = sorted_hues[(i + 1) % len(sorted_hues)]
        prev_hue = sorted_hues[i]
        gap = (next_hue - prev_hue) % 360
        if gap > best_gap:
            best_gap = gap
            best_mid = (prev_hue + gap / 2) % 360

    return best_mid


def _capabilities_color(capabilities_str: str, color_map: Dict[str, str]) -> str:
    if capabilities_str not in color_map:
        h = hashlib.md5(capabilities_str.encode()).hexdigest()
        preferred_hue = int(h[:4], 16) % 360
        sat = 55 + (int(h[4:6], 16) % 20)  # 55-75%
        lit = 45 + (int(h[6:8], 16) % 15)  # 45-60%

        existing_hues = [eh for v in color_map.values() if (eh := _extract_hue(v)) is not None]
        hue = _find_best_hue(preferred_hue, existing_hues)

        color_map[capabilities_str] = f"hsl({hue:.0f},{sat}%,{lit}%)"
    return color_map[capabilities_str]


def _display_capabilities(capabilities: Set[str]) -> str:
    if not capabilities:
        return "<no capabilities>"
    return " ".join(sorted(capabilities))


class TaskStreamState:
    """Server-side state for the task stream chart."""

    def __init__(self) -> None:
        self._memory_store_time = datetime.timedelta(minutes=30)

        # worker tracking
        self._seen_workers: Set[str] = set()
        self._worker_capabilities: Dict[str, Set[str]] = {}
        self._capabilities_color_map: Dict[str, str] = {"<no capabilities>": "#ffffff"}

        # task tracking  (worker -> {task_id -> start_time})
        self._current_tasks: Dict[str, Dict[bytes, datetime.datetime]] = {}
        self._task_id_to_worker: Dict[bytes, str] = {}
        self._task_id_to_capabilities: Dict[bytes, str] = {}
        self._task_id_to_function: Dict[bytes, str] = {}
        self._worker_to_task_ids: Dict[str, Set[bytes]] = {}

        # completed bar history: worker -> list of bar dicts
        # each bar has absolute "start" and "end" timestamps
        self._bar_history: Dict[str, List[Dict[str, Any]]] = {}

        self._dead_workers: Deque[Tuple[datetime.datetime, str]] = deque()

        self._lock = threading.Lock()

    def _caps_to_colors(self, caps_str: str) -> List[str]:
        """Return a list of colors for the capabilities string.

        Single-capability and no-capability tasks return one color.
        Multi-capability tasks return one color per individual capability (sorted).
        """
        if caps_str == "<no capabilities>":
            return ["#ffffff"]
        parts = caps_str.split()
        if len(parts) <= 1:
            return [_capabilities_color(caps_str, self._capabilities_color_map)]
        return [_capabilities_color(p, self._capabilities_color_map) for p in parts]

    def _ensure_worker(self, worker: str, now: datetime.datetime) -> None:
        if worker not in self._seen_workers:
            self._seen_workers.add(worker)
            self._bar_history.setdefault(worker, [])

    def handle_worker_state(self, state_worker: StateWorker) -> None:
        worker_id = state_worker.workerId.decode()
        worker_state = state_worker.state
        now = datetime.datetime.now()

        with self._lock:
            if worker_state == WorkerState.connected:
                self._ensure_worker(worker_id, now)
                self._worker_capabilities[worker_id] = set(capabilities_to_dict(state_worker.capabilities).keys())
            elif worker_state == WorkerState.disconnected:
                self._current_tasks.pop(worker_id, None)
                self._dead_workers.append((now, worker_id))

    def handle_task_state(self, state_task: StateTask) -> None:
        task_state = state_task.state
        now = datetime.datetime.now()

        with self._lock:
            if any(task_state == s for s in COMPLETED_TASK_STATUSES):
                self._handle_task_result(state_task, now)
                return

            worker = state_task.worker
            if not worker:
                return

            worker_str = worker.decode()
            self._ensure_worker(worker_str, now)
            if worker_str not in self._worker_capabilities:
                self._worker_capabilities[worker_str] = set()

            if task_state == TaskState.running:
                self._handle_running_task(state_task, worker_str, now)

    def _handle_running_task(self, state_task: StateTask, worker: str, now: datetime.datetime) -> None:
        task_id = state_task.taskId
        caps = _display_capabilities(set(capabilities_to_dict(state_task.capabilities).keys()))
        self._task_id_to_capabilities[task_id] = caps
        func_name = state_task.functionName.decode()
        if func_name:
            self._task_id_to_function[task_id] = func_name

        # if reassigned from another worker, clean up old worker tracking
        prev_worker = self._task_id_to_worker.get(task_id)
        if prev_worker and prev_worker != worker:
            task_map = self._current_tasks.get(prev_worker, {})
            task_map.pop(task_id, None)
            self._worker_to_task_ids.get(prev_worker, set()).discard(task_id)

        self._task_id_to_worker[task_id] = worker
        self._worker_to_task_ids.setdefault(worker, set()).add(task_id)

        # only set start time if this is a new task (don't overwrite on repeated Running messages)
        task_map = self._current_tasks.setdefault(worker, {})
        if task_id not in task_map:
            task_map[task_id] = now

    def _handle_task_result(self, state: StateTask, now: datetime.datetime) -> None:
        task_id = state.taskId
        worker = self._task_id_to_worker.get(task_id, "")

        # fallback: use worker from the completion message itself (late-connect case)
        if not worker and state.worker:
            worker = state.worker.decode()
            self._ensure_worker(worker, now)

        if not worker:
            return

        # store capabilities/function from completion message if not already known
        if task_id not in self._task_id_to_capabilities and state.capabilities:
            self._task_id_to_capabilities[task_id] = _display_capabilities(
                set(capabilities_to_dict(state.capabilities).keys())
            )
        func_name = state.functionName.decode() if state.functionName else ""
        if func_name and task_id not in self._task_id_to_function:
            self._task_id_to_function[task_id] = func_name

        task_map = self._current_tasks.get(worker, {})

        # use ProfileResult duration for accurate start time when available
        # (skip for cancelled tasks - profile data may be from a prior attempt)
        start = now
        end = now
        if state.state not in (TaskState.canceled, TaskState.canceledNotFound):
            try:
                if state.metadata and state.metadata != b"":
                    profile = ProfileResult.deserialize(state.metadata)
                    if profile.duration_s > 0:
                        start = now - datetime.timedelta(seconds=profile.duration_s)
            except struct.error:
                pass

        # fallback to Running message timestamp if no profile data
        if start == end and task_id in task_map:
            start = task_map[task_id]

        self._add_bar(worker, task_id, start, now, state.state)

        task_map.pop(task_id, None)
        if not task_map:
            self._current_tasks.pop(worker, None)
        self._worker_to_task_ids.get(worker, set()).discard(task_id)

    def _add_bar(
        self,
        worker: str,
        task_id: bytes,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        task_state: TaskState,
    ) -> None:
        caps = self._task_id_to_capabilities.get(task_id, "<no capabilities>")
        colors = self._caps_to_colors(caps)
        func = self._task_id_to_function.get(task_id, "")

        # For cancelled tasks, clip start to the end of the last completed bar on this worker
        # so the cancelled bar only extends back to where the previous task ended.
        if task_state in (TaskState.canceled, TaskState.canceledNotFound):
            worker_bars = self._bar_history.get(worker, [])
            for prev_bar in reversed(worker_bars):
                if prev_bar["pattern"] != "/":
                    last_end = datetime.datetime.fromtimestamp(prev_bar["end"])
                    if last_end > start_time:
                        start_time = last_end
                    break

        duration = (end_time - start_time).total_seconds()

        pattern = ""
        outline_color = "black"
        outline_width = 1
        if task_state in (TaskState.failed, TaskState.failedWorkerDied):
            pattern = "x"
            outline_color = "red"
        elif task_state == TaskState.failedSchedulerFault:
            # a fault of the scheduler itself, drawn apart from a task that raised
            pattern = "+"
            outline_color = "red"
        elif task_state in (TaskState.canceled, TaskState.canceledNotFound):
            pattern = "/"

        bar = {
            "start": start_time.timestamp(),
            "end": end_time.timestamp(),
            "color": colors,
            "caps": caps,
            "pattern": pattern,
            "outline_color": outline_color,
            "outline_width": outline_width,
            "hover": f"{func} ({duration:.2f}s) - {task_state.name}",
        }

        self._bar_history.setdefault(worker, []).append(bar)

    def _prune_old_data(self, now: datetime.datetime) -> None:
        cutoff = now - self._memory_store_time
        cutoff_ts = cutoff.timestamp()

        # remove old bars
        for worker in list(self._bar_history.keys()):
            bars = self._bar_history[worker]
            while bars and bars[0]["end"] < cutoff_ts:
                bars.pop(0)

        # remove dead workers past retention
        while self._dead_workers and self._dead_workers[0][0] < cutoff:
            _, worker = self._dead_workers.popleft()
            self._bar_history.pop(worker, None)
            self._worker_to_task_ids.pop(worker, None)
            self._worker_capabilities.pop(worker, None)
            self._seen_workers.discard(worker)

    def get_render_data(self, window_minutes: int) -> Dict[str, Any]:
        """Render the stream. The window is per-browser, so it is passed in rather than held here."""
        now = datetime.datetime.now()
        now_ts = now.timestamp()

        with self._lock:
            self._prune_old_data(now)
            window = SLIDING_WINDOW_OPTIONS.get(window_minutes, SLIDING_WINDOW_OPTIONS[DEFAULT_STREAM_WINDOW_MINUTES])
            window_seconds = window.total_seconds()
            window_start_ts = now_ts - window_seconds

            # one row per worker, sorted by name - only include workers with visible activity
            row_labels: List[str] = []
            full_row_labels: List[str] = []
            worker_order: List[str] = []
            for worker in sorted(self._seen_workers):
                # check if worker has any running tasks
                has_running = bool(self._current_tasks.get(worker))
                # check if worker has any completed bars in the visible window
                has_visible_bars = False
                if not has_running:
                    for bar in self._bar_history.get(worker, []):
                        if bar["end"] >= window_start_ts:
                            has_visible_bars = True
                            break
                if has_running or has_visible_bars:
                    row_labels.append(_format_worker_name(worker))
                    full_row_labels.append(worker)
                    worker_order.append(worker)

            # Build bars list ordered so that:
            # - Running tasks are drawn first (behind everything)
            # - Completed bars are drawn newest-first, oldest-last (oldest on top)
            # JS hover iterates backwards, so last items = checked first = hoverable on top
            bars: List[Dict[str, Any]] = []

            # 1) Running tasks (drawn first / behind completed bars)
            #    Compute sublanes per row: if N tasks running on same worker, each gets sl=0..N-1, sn=N
            running_per_row: Dict[int, List[Dict[str, Any]]] = {}
            for row_idx, worker in enumerate(worker_order):
                task_map = self._current_tasks.get(worker)
                if not task_map:
                    continue
                for task_id, start_time in task_map.items():
                    actual_duration = (now - start_time).total_seconds()
                    x_start = (start_time - now).total_seconds()
                    x_end = 0.0  # now
                    x_start = max(x_start, -window_seconds)
                    w = x_end - x_start
                    if w <= 0:
                        continue
                    caps = self._task_id_to_capabilities.get(task_id, "<no capabilities>")
                    colors = self._caps_to_colors(caps)
                    func = self._task_id_to_function.get(task_id, "")
                    bar_dict = {
                        "r": row_idx,
                        "x": x_start,
                        "w": w,
                        "cs": colors,
                        "p": "",
                        "oc": "#eab308",  # yellow for running
                        "ow": 2,
                        "h": f"{func} ({actual_duration:.1f}s) - Running",
                        "rn": 1,
                    }
                    running_per_row.setdefault(row_idx, []).append(bar_dict)

            for row_idx, row_bars in running_per_row.items():
                count = len(row_bars)
                for i, b in enumerate(row_bars):
                    b["sl"] = i
                    b["sn"] = count
                bars.extend(row_bars)

            # 2) Completed bars in reverse order (newest first, oldest last = oldest drawn on top)
            #    Collect per-row first so we can compute sublane assignments.
            completed_per_row: Dict[int, List[Dict[str, Any]]] = {}
            for row_idx, worker in enumerate(worker_order):
                worker_bars = self._bar_history.get(worker, [])
                for bar in reversed(worker_bars):
                    if bar["end"] < window_start_ts:
                        continue  # outside visible window
                    # convert absolute timestamps to relative seconds from now
                    x_start = bar["start"] - now_ts  # negative
                    x_end = bar["end"] - now_ts  # negative or near-zero
                    # clip to window
                    x_start = max(x_start, -window_seconds)
                    w = x_end - x_start
                    if w <= 0:
                        continue
                    bar_dict = {
                        "r": row_idx,
                        "x": x_start,
                        "w": w,
                        "cs": bar["color"],
                        "p": bar["pattern"],
                        "oc": bar["outline_color"],
                        "ow": bar["outline_width"],
                        "h": bar["hover"],
                    }
                    completed_per_row.setdefault(row_idx, []).append(bar_dict)

            # Compute sublane assignments per row.
            # Only non-cancelled completed bars participate; cancelled bars keep sl=0/sn=1.
            # Overlaps of <= 2 seconds are ignored (likely timing rounding).
            # Bars are grouped into connected overlap components so non-overlapping
            # bars remain full height.
            OVERLAP_THRESHOLD = 2.0  # seconds
            for row_idx, row_bars in completed_per_row.items():
                # Separate cancelled bars (they don't participate in sublane logic)
                normal_bars = [b for b in row_bars if b["p"] != "/"]
                for b in row_bars:
                    if b["p"] == "/":
                        b["sl"] = 0
                        b["sn"] = 1

                if not normal_bars:
                    continue

                sorted_bars = sorted(normal_bars, key=lambda b: b["x"])

                # Build connected overlap groups (merge-intervals with threshold)
                groups: List[List[int]] = []  # each group is list of indices into sorted_bars
                group_end = -float("inf")
                for idx, b in enumerate(sorted_bars):
                    b_end = b["x"] + b["w"]
                    if b["x"] < group_end - OVERLAP_THRESHOLD:
                        # overlaps current group by more than threshold
                        groups[-1].append(idx)
                        if b_end > group_end:
                            group_end = b_end
                    else:
                        # start new group
                        groups.append([idx])
                        group_end = b_end

                # Assign lanes within each group
                for group in groups:
                    if len(group) == 1:
                        sorted_bars[group[0]]["sl"] = 0
                        sorted_bars[group[0]]["sn"] = 1
                        continue
                    # greedy interval coloring within the group
                    lane_ends: List[float] = []
                    bar_lanes: List[int] = []
                    for idx in group:
                        b = sorted_bars[idx]
                        placed = False
                        for lane_idx, end in enumerate(lane_ends):
                            if end <= b["x"] + OVERLAP_THRESHOLD:
                                lane_ends[lane_idx] = b["x"] + b["w"]
                                bar_lanes.append(lane_idx)
                                placed = True
                                break
                        if not placed:
                            bar_lanes.append(len(lane_ends))
                            lane_ends.append(b["x"] + b["w"])
                    total_lanes = len(lane_ends)
                    for i, idx in enumerate(group):
                        sorted_bars[idx]["sl"] = bar_lanes[i]
                        sorted_bars[idx]["sn"] = total_lanes

            # Add completed bars to the bars list (preserving original reverse order)
            for row_idx in sorted(completed_per_row.keys()):
                bars.extend(completed_per_row[row_idx])

            # capability legend: derived from tasks visible in the stream
            active_caps: Set[str] = set()
            # from running tasks
            for worker in worker_order:
                for task_id in self._current_tasks.get(worker, {}):
                    caps_str = self._task_id_to_capabilities.get(task_id, "<no capabilities>")
                    if caps_str != "<no capabilities>":
                        active_caps.update(caps_str.split())
            # from completed bars in the visible window
            for worker in worker_order:
                for bar in self._bar_history.get(worker, []):
                    if bar["end"] >= window_start_ts:
                        task_caps = bar.get("caps", "")
                        if task_caps and task_caps != "<no capabilities>":
                            active_caps.update(task_caps.split())

            legend: List[Dict[str, str]] = [{"name": "<no capabilities>", "color": "#ffffff"}]
            legend.extend(
                {"name": cap, "color": _capabilities_color(cap, self._capabilities_color_map)}
                for cap in sorted(active_caps)
            )

            # time axis ticks
            ticks: List[Dict[str, Any]] = []
            num_ticks = 7
            for i in range(num_ticks):
                val = -window_seconds + i * (window_seconds / (num_ticks - 1))
                ticks.append({"val": round(val, 1), "label": f"{int(val)}s"})

        return {
            "rows": row_labels,
            "full_rows": full_row_labels,
            "bars": bars,
            "legend": legend,
            "ticks": ticks,
            "window": window_seconds,
        }


class MemoryChartState:
    """Server-side state for the memory usage chart."""

    def __init__(self) -> None:
        self._start_time = datetime.datetime.now()
        self._points: List[Tuple[float, int]] = []  # (timestamp, memory_bytes)
        self._memory_store_time = datetime.timedelta(minutes=30)
        self._lock = threading.Lock()

    def handle_task_state(self, state_task: StateTask) -> None:
        if state_task.metadata == b"":
            return

        try:
            profile = ProfileResult.deserialize(state_task.metadata)
        except struct.error:
            return

        if profile.memory_peak == 0:
            return

        now = datetime.datetime.now()
        with self._lock:
            start_ts = now.timestamp() - profile.duration_s
            self._points.append((start_ts, profile.memory_peak))
            self._points.append((now.timestamp(), -profile.memory_peak))

    def get_render_data(self, window_seconds: float, scale: str) -> Dict[str, Any]:
        """Render the chart. Window and scale are per-browser, so they are passed in."""
        now = datetime.datetime.now()
        now_ts = now.timestamp()
        cutoff_ts = now_ts - self._memory_store_time.total_seconds()

        with self._lock:
            # prune old points
            self._points = [(t, m) for t, m in self._points if t >= cutoff_ts]

            # build memory timeline within visible window
            events = sorted(self._points, key=lambda p: p[0])

        # accumulate memory usage
        running_mem = 0
        chart_points: List[Dict[str, Any]] = []
        for ts, delta in events:
            running_mem += delta
            if running_mem < 0:
                running_mem = 0
            x = ts - now_ts  # relative seconds
            if x < -window_seconds:
                continue
            chart_points.append({"x": round(x, 2), "y": max(running_mem, 0)})

        # always include current point
        if not chart_points or chart_points[-1]["x"] < -0.1:
            chart_points.append({"x": 0, "y": max(running_mem, 0)})

        # compute y-axis ticks
        max_mem = max((p["y"] for p in chart_points), default=0)
        max_mem = max(max_mem, 1024 * 1024 * 1024)  # minimum 1GB
        y_ticks = []
        for i in range(5):
            val = int(max_mem * i / 4)
            y_ticks.append({"val": val, "label": format_bytes(val)})

        return {"points": chart_points, "y_ticks": y_ticks, "scale": scale, "window": window_seconds}


class WebUIApp:
    """Main application holding all server-side state and managing connections."""

    def __init__(self, config: WebGUIConfig) -> None:
        self._config = config
        self._broadcast_interval_seconds: float = config.broadcast_interval_seconds
        self._task_log_max_size: int = config.task_log_max_size
        # Mark the scheduler stale once its periodic StateScheduler heartbeat has not arrived for ~5x its
        # report interval; that heartbeat runs on the scheduler's main loop, so a stalled loop stops it.
        self._scheduler_stale_seconds: float = 5 * config.status_report_interval_seconds
        # Total completed tasks seen since this GUI process started, uncapped by the display ring buffer.
        self._task_log_total: int = 0
        # Full fleet worker count from per-manager totals; each browser is sent one page of worker rows.
        self._total_workers: int = 0
        self._message_queue: queue.Queue[BaseMessage] = queue.Queue()
        self._clients: Dict[WebSocket, ClientView] = {}
        self._clients_lock = asyncio.Lock()

        # server-side state
        self._scheduler_data: Dict[str, Any] = {}
        self._workers_data: Dict[str, Dict[str, Any]] = {}
        self._worker_capabilities: Dict[str, Dict[str, int]] = {}
        self._task_log: Deque[Dict[str, Any]] = deque(maxlen=self._task_log_max_size)
        self._active_tasks: Dict[str, Dict[str, Any]] = {}  # task_id_hex -> entry (running tasks)
        self._task_id_to_function: Dict[str, str] = {}
        self._task_stream = TaskStreamState()
        self._memory_chart = MemoryChartState()
        self._worker_processors: Dict[str, Dict[str, Any]] = {}
        self._worker_manager_map: Dict[str, str] = {}  # worker_name -> manager_id (persistent)
        self._worker_managers_data: Dict[str, Dict[str, Any]] = {}  # manager_id -> manager info
        self._dead_managers: Dict[str, float] = {}  # manager_id -> disconnect timestamp
        self._manager_color_map: Dict[str, str] = {}  # manager_id -> color hex
        self._monitor_address: str = str(config.monitor_address)
        # Timestamp of the last StateScheduler heartbeat; the scheduler's last-seen derives from it and goes
        # stale when the main loop stalls.
        self._last_scheduler_heartbeat_time: Optional[datetime.datetime] = None

        self._identity = generate_identity_from_name("webui")

        self._backend = get_network_backend_from_env()
        self._subscriber: Optional[SyncSubscriber] = None
        self._batch_task: Optional[asyncio.Task] = None

    def _on_monitor_message(self, message: BaseMessage) -> None:
        """Called from the subscriber thread. Just enqueue, don't process."""
        try:
            self._message_queue.put_nowait(message)
        except queue.Full:
            pass

    def start_subscriber(self) -> None:
        self._subscriber = self._backend.create_sync_subscriber(
            identity=self._identity,
            address=self._config.monitor_address,
            callback=self._on_monitor_message,
            timeout=None,
            security_config=self._config.security,
        )
        self._subscriber.daemon = True
        self._subscriber.start()

    async def start_batcher(self) -> None:
        self._batch_task = asyncio.create_task(self._batch_loop())

    async def stop_batcher(self) -> None:
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

    async def _batch_loop(self) -> None:
        """Drain the message queue every broadcast interval and push to browsers."""
        while True:
            await asyncio.sleep(self._broadcast_interval_seconds)
            messages: List[BaseMessage] = []
            while True:
                try:
                    messages.append(self._message_queue.get_nowait())
                except queue.Empty:
                    break

            # Process messages
            has_scheduler_update = False
            new_task_logs: List[Dict[str, Any]] = []
            worker_events: List[Dict[str, Any]] = []

            for msg in messages:
                try:
                    if isinstance(msg, StateScheduler):
                        self._process_scheduler(msg)
                        has_scheduler_update = True
                    elif isinstance(msg, StateWorker):
                        event = self._process_worker_state(msg)
                        if event:
                            worker_events.append(event)
                    elif isinstance(msg, StateTask):
                        log_entry = self._process_task_state(msg)
                        if log_entry:
                            new_task_logs.append(log_entry)
                    elif isinstance(msg, StateBalanceAdvice):
                        pass  # unused
                except Exception:
                    _logger.exception("error processing scheduler message")

            if has_scheduler_update:
                self._last_scheduler_heartbeat_time = datetime.datetime.now()

            # The parts every browser gets identically.
            shared: Dict[str, Any] = {}

            # Always include scheduler data with a last_seen derived from the periodic heartbeat.
            if self._scheduler_data:
                sched = dict(self._scheduler_data)
                sched.update(self.__scheduler_liveness())
                shared["scheduler"] = sched

            if worker_events:
                shared["worker_events"] = worker_events

            if new_task_logs:
                shared["task_updates"] = new_task_logs
                shared["task_log_total"] = self._task_log_total

            if has_scheduler_update:
                shared["worker_managers"] = list(self._worker_managers_data.values())

            # The paged parts differ per browser; the fleet-wide work behind them is shared via the cache.
            cache = _RenderCache()
            await self._send_to_clients(
                lambda view: {
                    **shared,
                    **(self._workers_section(view, cache) if has_scheduler_update else {}),
                    **(self._processors_section(view, cache) if has_scheduler_update else {}),
                    "task_stream": self._stream_section(view, cache),
                    "memory_chart": cache.memory(
                        self, cache.stream(self, view.stream_window_minutes)["window"], view.memory_scale
                    ),
                }
            )

    def _process_scheduler(self, data: StateScheduler) -> None:
        self._scheduler_data = {
            "cpu": format_percentage(data.scheduler.cpu),
            "rss": format_bytes(data.scheduler.rss),
            "rss_free": format_bytes(data.rssFree),
            "monitor_address": self._monitor_address,
        }

        # Update the worker-to-manager mapping and count workers per manager and across the fleet, in a
        # single pass while each capnp workerIDs list is freshly accessed. Storing the lazy lists to len()
        # them in the detail loop below is unreliable -- the references do not survive -- which otherwise
        # reports 0 workers for every manager past the first. The fleet total also feeds the "N of M"
        # workers indicator, since each browser receives only a bounded subset of workers.
        # Key by the decoded manager name (a materialized str), not by the capnp id field: reading that
        # field more than once per detail returns divergent values under capnp aliasing, so joining the two
        # loops on it silently misses.
        manager_worker_counts: Dict[str, int] = {}
        total_workers = 0
        for pair in data.scalingManager.managedWorkers:
            manager_id_raw = bytes(pair.workerManagerID)
            manager_name = manager_id_raw.decode() if manager_id_raw else "unknown"
            manager_worker_count = 0
            for wid in pair.workerIDs:
                self._worker_manager_map[bytes(wid).decode()] = manager_name
                manager_worker_count += 1
            manager_worker_counts[manager_name] = manager_worker_count
            total_workers += manager_worker_count
        self._total_workers = total_workers

        # Update worker manager details from scaling_manager
        current_managers: Set[str] = set()
        for detail in data.scalingManager.workerManagerDetails:
            manager_id_raw = bytes(detail.workerManagerID)
            manager_id = manager_id_raw.decode() if manager_id_raw else "unknown"
            current_managers.add(manager_id)
            self._worker_managers_data[manager_id] = {
                "manager_id": manager_id,
                "identity": detail.identity,
                "last_seen": format_seconds(detail.lastSeenS),
                "max_task_concurrency": detail.maxTaskConcurrency,
                "worker_count": manager_worker_counts.get(manager_id, 0),
                "pending_workers": detail.pendingWorkers,
                "capabilities": detail.capabilities,
            }
        # Mark newly-disappeared managers with a disconnect timestamp instead of
        # removing immediately, so the UI keeps showing them for a grace period.
        now_ts = datetime.datetime.now().timestamp()
        newly_dead = set(self._worker_managers_data.keys()) - current_managers
        for mid in newly_dead:
            if mid not in self._dead_managers:
                self._dead_managers[mid] = now_ts
        # Re-alive managers that came back
        for mid in current_managers:
            self._dead_managers.pop(mid, None)
        # Evict managers that have been gone for more than 2 minutes
        manager_retention_seconds = 120
        evict = [mid for mid, ts in self._dead_managers.items() if now_ts - ts > manager_retention_seconds]
        for mid in evict:
            self._dead_managers.pop(mid)
            self._worker_managers_data.pop(mid, None)

        current_workers = set()
        now = datetime.datetime.now()
        for worker_data in data.workerManager.workers:
            worker_name = worker_data.workerId.decode()
            current_workers.add(worker_name)
            # ensure task stream knows about this worker (handles late UI connect)
            self._task_stream._ensure_worker(worker_name, now)
            total_proc_cpu = sum(p.resource.cpu for p in worker_data.processorStatuses)
            total_proc_rss = sum(p.resource.rss for p in worker_data.processorStatuses)
            total_rss = int(total_proc_rss / 1e6)
            rss_free = int(worker_data.rssFree / 1e6)
            agt_rss = int(worker_data.agent.rss / 1e6)

            # OOM-proximity gauge: memLimit is the ceiling the worker runs under (cgroup limit in a pod,
            # else host total) and rssFree is its headroom, so (limit - free) is what is actually in use
            # against that ceiling -- the number that predicts an OOM kill.
            mem_limit = int(worker_data.memLimit / 1e6)
            mem_used = max(0, mem_limit - rss_free)
            mem_used_pct = round(100.0 * mem_used / mem_limit, 1) if mem_limit > 0 else 0.0

            self._workers_data[worker_name] = {
                "id": worker_name,
                "name": _format_worker_name(worker_name),
                "full_name": worker_name,
                "manager_id": self._worker_manager_map.get(worker_name, "\u2014"),
                "agt_cpu": round(worker_data.agent.cpu / 10, 1),
                "agt_rss": agt_rss,
                "proc_cpu": round(total_proc_cpu / 10, 1),
                "proc_rss": total_rss,
                "rss_free": rss_free,
                "total_rss": total_rss + rss_free,
                "worker_rss": agt_rss + total_rss,
                "mem_limit": mem_limit,
                "mem_used": mem_used,
                "mem_used_pct": mem_used_pct,
                "free": worker_data.free,
                "sent": worker_data.sent,
                "queued": worker_data.queued,
                "suspended": worker_data.suspended,
                "lag": format_microseconds(worker_data.lagUS),
                # raw values behind the preformatted columns, so sorting them orders by magnitude
                "lag_us": worker_data.lagUS,
                "last_s": worker_data.lastS,
                "itl": worker_data.itl,
                "last_seen": format_seconds(worker_data.lastS),
                "capabilities": _display_capabilities(set(self._worker_capabilities.get(worker_name, {}).keys())),
            }

            # update processor details
            self._worker_processors[worker_name] = {
                "name": _format_worker_name(worker_name),
                "full_name": worker_name,
                "manager_id": self._worker_manager_map.get(worker_name, "\u2014"),
                "rss_free": rss_free,
                "processors": [],
            }
            max_rss = 0
            for ps in sorted(worker_data.processorStatuses, key=lambda x: x.pid):
                rss_val = int(ps.resource.rss / 1e6)
                if ps.resource.rss > max_rss:
                    max_rss = ps.resource.rss
                self._worker_processors[worker_name]["processors"].append(
                    {
                        "pid": ps.pid,
                        "cpu": round(ps.resource.cpu / 10, 1),
                        "rss": rss_val,
                        "max_rss": int(max_rss / 1e6),
                        "rss_max_gauge": rss_val + rss_free,
                        "initialized": bool(ps.initialized),
                        "has_task": bool(ps.hasTask),
                        "suspended": bool(ps.suspended),
                    }
                )

        # remove dead workers
        dead = set(self._workers_data.keys()) - current_workers
        for w in dead:
            self._workers_data.pop(w, None)
            self._worker_processors.pop(w, None)
            self._worker_manager_map.pop(w, None)
            self._task_stream.handle_worker_state(
                StateWorker(workerId=WorkerID(w.encode()), state=WorkerState.disconnected, capabilities=[])
            )

        # Aggregate per-manager summary stats over every worker the backend received (the whole fleet by
        # default) -- so these sums are complete even though each browser is sent only a bounded subset for
        # display. worker_count keeps the full per-manager total computed above.
        for manager_id, mgr_data in self._worker_managers_data.items():
            mgr_proc_cpu = 0.0
            mgr_proc_rss = 0
            mgr_free = 0
            mgr_sent = 0
            mgr_queued = 0
            mgr_suspended = 0
            for w_data in self._workers_data.values():
                if w_data.get("manager_id") == manager_id:
                    mgr_proc_cpu += w_data.get("proc_cpu", 0)
                    mgr_proc_rss += w_data.get("proc_rss", 0)
                    mgr_free += w_data.get("free", 0)
                    mgr_sent += w_data.get("sent", 0)
                    mgr_queued += w_data.get("queued", 0)
                    mgr_suspended += w_data.get("suspended", 0)
            mgr_data["total_proc_cpu"] = round(mgr_proc_cpu, 1)
            mgr_data["total_proc_rss"] = mgr_proc_rss
            mgr_data["total_free"] = mgr_free
            mgr_data["total_sent"] = mgr_sent
            mgr_data["total_queued"] = mgr_queued
            mgr_data["total_suspended"] = mgr_suspended

    def _process_worker_state(self, state_worker: StateWorker) -> Optional[Dict[str, Any]]:
        worker_id = state_worker.workerId.decode()
        state = state_worker.state

        if state == WorkerState.connected:
            # Store capabilities as a {name: value} dict so downstream consumers
            # (e.g. _process_scheduler -> _display_capabilities) can call .keys() on them.
            self._worker_capabilities[worker_id] = capabilities_to_dict(state_worker.capabilities)
        elif state == WorkerState.disconnected:
            self._workers_data.pop(worker_id, None)
            self._worker_capabilities.pop(worker_id, None)
            self._worker_processors.pop(worker_id, None)

        self._task_stream.handle_worker_state(state_worker)

        return {
            "worker_id": worker_id,
            "state": state.name,
            "capabilities": list(capabilities_to_dict(state_worker.capabilities).keys()),
        }

    def _process_task_state(self, state_task: StateTask) -> Optional[Dict[str, Any]]:
        task_id_hex = state_task.taskId.hex()
        func_name = state_task.functionName.decode()

        if func_name and task_id_hex not in self._task_id_to_function:
            self._task_id_to_function[task_id_hex] = func_name

        # forward to chart states
        self._task_stream.handle_task_state(state_task)
        self._memory_chart.handle_task_state(state_task)

        if not func_name:
            func_name = self._task_id_to_function.get(task_id_hex, "")

        worker_str = ""
        full_worker = ""
        if state_task.worker:
            full_worker = state_task.worker.decode()
            worker_str = _format_worker_name(full_worker)

        caps_str = _display_capabilities(set(capabilities_to_dict(state_task.capabilities).keys()))
        now = datetime.datetime.now()

        if any(state_task.state == s for s in COMPLETED_TASK_STATUSES):
            # preserve worker/time from active entry if completion message lacks them
            prev_entry = self._active_tasks.pop(task_id_hex, None)
            if not worker_str and prev_entry:
                worker_str = prev_entry.get("worker", "")
                full_worker = prev_entry.get("full_worker", "")
            submitted_time = prev_entry["time"] if prev_entry and "time" in prev_entry else now.timestamp()
            self._task_id_to_function.pop(task_id_hex, None)

            duration_str = "N/A"
            peak_mem_str = "N/A"
            if state_task.metadata != b"":
                try:
                    profile = ProfileResult.deserialize(state_task.metadata)
                    duration_str = f"{profile.duration_s:.2f}s"
                    peak_mem_str = format_bytes(profile.memory_peak) if profile.memory_peak != 0 else "0"
                    # back-compute submitted time when no prior entry exists (late-connect)
                    if not prev_entry:
                        submitted_time = now.timestamp() - profile.duration_s
                except struct.error:
                    pass

            entry = {
                "task_id": task_id_hex,
                "function": func_name,
                "worker": worker_str,
                "full_worker": full_worker,
                "time": submitted_time,
                "duration": duration_str,
                "peak_mem": peak_mem_str,
                "status": state_task.state.name,
                "capabilities": caps_str,
            }
            self._task_log.appendleft(entry)
            self._task_log_total += 1
            return entry
        else:
            # running/inactive/canceling - track as active task
            prev_entry = self._active_tasks.get(task_id_hex)
            submitted_time = prev_entry["time"] if prev_entry and "time" in prev_entry else now.timestamp()
            if not worker_str and prev_entry:
                worker_str = prev_entry.get("worker", "")
                full_worker = prev_entry.get("full_worker", "")
            # remove stale completed entry if task was re-submitted
            self._task_log = deque(
                (e for e in self._task_log if e["task_id"] != task_id_hex), maxlen=self._task_log_max_size
            )
            entry = {
                "task_id": task_id_hex,
                "function": func_name,
                "worker": worker_str,
                "full_worker": full_worker,
                "time": submitted_time,
                "duration": "",
                "peak_mem": "",
                "status": state_task.state.name,
                "capabilities": caps_str,
            }
            self._active_tasks[task_id_hex] = entry
            return entry

    def _enrich_stream_with_managers(self, stream_data: Dict[str, Any]) -> None:
        """Add per-row manager IDs and a manager color legend to task stream data."""
        full_rows = stream_data.get("full_rows", [])
        row_managers = [self._worker_manager_map.get(w, "") for w in full_rows]
        stream_data["row_managers"] = row_managers

        seen: Set[str] = set()
        for mid in row_managers:
            if mid:
                seen.add(mid)
        manager_legend: List[Dict[str, str]] = [
            {"name": mid, "color": _capabilities_color(mid, self._manager_color_map)} for mid in sorted(seen)
        ]
        stream_data["manager_legend"] = manager_legend

    def _sorted_workers(self, sort_field: Optional[str], ascending: bool) -> List[Dict[str, Any]]:
        """The whole fleet in one browser's sort order; the browser holds a page and cannot sort."""
        workers = list(self._workers_data.values())
        if sort_field is None:
            return workers

        raw_field = WORKER_SORT_RAW_FIELDS.get(sort_field, sort_field)
        if sort_field in WORKER_SORT_NUMERIC_FIELDS or sort_field in WORKER_SORT_RAW_FIELDS:

            def key(worker: Dict[str, Any]) -> Any:
                value = worker.get(raw_field, 0)
                return value if isinstance(value, (int, float)) else 0

        else:

            def key(worker: Dict[str, Any]) -> Any:
                return str(worker.get(raw_field, "")).lower()

        return sorted(workers, key=key, reverse=not ascending)

    def _workers_section(self, view: ClientView, cache: "_RenderCache") -> Dict[str, Any]:
        workers = cache.sorted_workers(self, view)
        rows, page, total_pages = paginate(workers, view.workers_page, WORKERS_PAGE_SIZE)
        view.workers_page = page
        return {
            "workers": rows,
            "workers_total": self._fleet_worker_count(),
            "workers_page": page,
            "workers_pages": total_pages,
        }

    def _processors_section(self, view: ClientView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of processor detail; the per-manager summaries still cover every worker."""
        groups = cache.processors(self)
        flat: List[Tuple[str, Dict[str, Any]]] = [
            (group["manager_id"], worker) for group in groups for worker in group["workers"]
        ]
        page_rows, page, total_pages = paginate(flat, view.processors_page, PROCESSORS_PAGE_SIZE)
        view.processors_page = page

        shown: Dict[str, List[Dict[str, Any]]] = {}
        for manager_id, worker in page_rows:
            shown.setdefault(manager_id, []).append(worker)

        paged_groups = [dict(group, workers=shown.get(group["manager_id"], [])) for group in groups]
        return {
            "processors": paged_groups,
            "processors_page": page,
            "processors_pages": total_pages,
            "processors_total": len(flat),
        }

    def _stream_section(self, view: ClientView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of stream rows, with each bar's row index rebased to the page."""
        stream_data = dict(cache.stream(self, view.stream_window_minutes))
        rows = stream_data.get("rows", [])
        _, page, total_pages = paginate(rows, view.stream_page, STREAM_PAGE_SIZE)
        view.stream_page = page

        start = page * STREAM_PAGE_SIZE
        end = start + STREAM_PAGE_SIZE
        stream_data["rows"] = rows[start:end]
        stream_data["full_rows"] = stream_data.get("full_rows", [])[start:end]
        stream_data["row_managers"] = stream_data.get("row_managers", [])[start:end]
        stream_data["bars"] = [
            dict(bar, r=bar["r"] - start) for bar in stream_data.get("bars", []) if start <= bar.get("r", 0) < end
        ]
        stream_data["page"] = page
        stream_data["pages"] = total_pages
        stream_data["total_rows"] = len(rows)
        return stream_data

    def _fleet_worker_count(self) -> int:
        """Full fleet size, for the "N of M" indicator next to a bounded worker list.

        Per-manager totals are authoritative when managers report in, but a deployment can run workers
        without a registered worker manager (e.g. the native manager in fixed mode), leaving those totals
        at zero -- in which case the workers this backend holds are the fleet it knows about.
        """
        return max(self._total_workers, len(self._workers_data))

    def _build_processors_data(self) -> List[Dict[str, Any]]:
        # Group every worker by manager for complete per-manager summaries.
        managers: Dict[str, List[Dict[str, Any]]] = {}
        for wp in self._worker_processors.values():
            mid = wp.get("manager_id", "—")
            managers.setdefault(mid, []).append(wp)

        # Ensure all known worker managers appear even if they have no workers
        for mid in self._worker_managers_data:
            managers.setdefault(mid, [])

        # Every worker's detail; _processors_section slices one page out of it per browser.
        result = []
        for manager_id, workers in sorted(managers.items()):
            total_rss = 0
            total_cpu = 0.0
            total_processors = 0
            active_processors = 0
            for wp in workers:
                for proc in wp["processors"]:
                    total_rss += proc["rss"]
                    total_cpu += proc["cpu"]
                    total_processors += 1
                    if proc["has_task"]:
                        active_processors += 1
            result.append(
                {
                    "manager_id": manager_id,
                    "worker_count": len(workers),
                    "total_rss": total_rss,
                    "total_cpu": round(total_cpu, 1),
                    "total_processors": total_processors,
                    "active_processors": active_processors,
                    "workers": workers,
                }
            )
        return result

    def _drain_pending_messages(self) -> None:
        """Process any pending messages from the queue immediately.

        Called before building a full-state snapshot so a freshly connected
        browser always sees the latest data."""
        while True:
            try:
                msg = self._message_queue.get_nowait()
            except queue.Empty:
                break
            try:
                if isinstance(msg, StateScheduler):
                    self._process_scheduler(msg)
                elif isinstance(msg, StateWorker):
                    self._process_worker_state(msg)
                elif isinstance(msg, StateTask):
                    self._process_task_state(msg)
            except Exception:
                _logger.exception("error processing scheduler message during drain")

    def __scheduler_liveness(self) -> Dict[str, Any]:
        """last_seen + stale flag derived from the last StateScheduler heartbeat."""
        if self._last_scheduler_heartbeat_time is None:
            return {"last_seen": "\u2014", "stale": False}
        elapsed = int((datetime.datetime.now() - self._last_scheduler_heartbeat_time).total_seconds())
        return {"last_seen": format_seconds(elapsed), "stale": elapsed > self._scheduler_stale_seconds}

    def get_full_state(self, view: ClientView) -> Dict[str, Any]:
        """Get complete current state for one client, in that client's view."""
        # Flush any messages that arrived since the last batch-loop iteration so
        # the snapshot is as fresh as possible.
        self._drain_pending_messages()

        cache = _RenderCache()
        stream_data = self._stream_section(view, cache)
        memory_data = cache.memory(self, stream_data["window"], view.memory_scale)
        # combine active + completed for initial task log, sorted by time (newest first). _active_tasks is
        # unbounded (one entry per running task), so cap the snapshot to the display size -- at thousands of
        # concurrent tasks the browser only shows task_log_max_size rows anyway, and the rest stream in.
        initial_task_log = list(self._active_tasks.values()) + list(self._task_log)
        initial_task_log.sort(key=lambda e: e.get("time", 0), reverse=True)
        initial_task_log = initial_task_log[: self._task_log_max_size]
        # Build scheduler data with a last_seen derived from the periodic heartbeat.
        sched = dict(self._scheduler_data) if self._scheduler_data else {}
        sched.update(self.__scheduler_liveness())

        return {
            "scheduler": sched,
            **self._workers_section(view, cache),
            "task_log": initial_task_log,
            "task_log_max_size": self._task_log_max_size,
            "task_log_total": self._task_log_total,
            "task_stream": stream_data,
            "memory_chart": memory_data,
            **self._processors_section(view, cache),
            "worker_managers": list(self._worker_managers_data.values()),
            "settings": view.settings(),
        }

    def view_update(self, view: ClientView) -> Dict[str, Any]:
        """The paged sections for one client, answered on its change instead of at the next tick."""
        cache = _RenderCache()
        stream_data = self._stream_section(view, cache)
        return {
            **self._workers_section(view, cache),
            **self._processors_section(view, cache),
            "task_stream": stream_data,
            "memory_chart": cache.memory(self, stream_data["window"], view.memory_scale),
            "settings": view.settings(),
        }

    async def add_client(self, ws: WebSocket) -> ClientView:
        view = ClientView()
        async with self._clients_lock:
            self._clients[ws] = view
        return view

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.pop(ws, None)

    async def _send_to_clients(self, build_payload: Callable[[ClientView], Dict[str, Any]]) -> None:
        """Serialize and send one payload per client: each browser is on its own page and sort order."""
        async with self._clients_lock:
            dead: List[WebSocket] = []
            for ws, view in self._clients.items():
                try:
                    await ws.send_text(json.dumps(build_payload(view)))
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self._clients.pop(ws, None)


def create_app(config: WebGUIConfig) -> FastAPI:
    app_state = WebUIApp(config)

    # Start ZMQ subscriber immediately so messages are collected even while uvicorn
    # is still initialising.  The subscriber thread puts into a thread-safe queue;
    # the asyncio batch_loop (started in the startup event) drains it later.
    app_state.start_subscriber()

    app = FastAPI(title="Scaler Web GUI")

    @app.middleware("http")
    async def no_cache_headers(request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return response

    @app.on_event("startup")
    async def startup() -> None:
        await app_state.start_batcher()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await app_state.stop_batcher()
        if app_state._subscriber:
            app_state._subscriber.destroy()

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        view = await app_state.add_client(ws)
        try:
            # send full state on connect
            full_state = app_state.get_full_state(view)
            full_state["type"] = "full_state"
            await ws.send_text(json.dumps(full_state))

            # this browser's own view changes: paging, sorting, chart settings
            while True:
                data = await ws.receive_text()
                try:
                    msg = json.loads(data)
                    message_type = msg.get("type")
                    if message_type == "settings":
                        view.apply_settings(msg.get("settings", {}))
                    elif message_type == "view":
                        view.apply_view(msg.get("view", {}))
                    else:
                        continue
                    update = app_state.view_update(view)
                    update["type"] = "view_update"
                    await ws.send_text(json.dumps(update))
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    pass
        except WebSocketDisconnect:
            pass
        finally:
            await app_state.remove_client(ws)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app
