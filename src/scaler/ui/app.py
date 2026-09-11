import dataclasses
import datetime
import hashlib
import itertools
import json
import logging
import math
import queue
import re
import struct
import threading
from collections import deque
from pathlib import Path
from typing import Any, Callable, Collection, Deque, Dict, FrozenSet, Iterable, List, Mapping, Optional, Set, Tuple

from scaler.config.section.webgui import WebGUIConfig
from scaler.io.mixins import SyncSubscriber
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    ObjectManagerStatus,
    ProcessorStatus,
    StateBalanceAdvice,
    StateObject,
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
)

SLIDING_WINDOW_OPTIONS = {
    5: datetime.timedelta(minutes=5),
    10: datetime.timedelta(minutes=10),
    30: datetime.timedelta(minutes=30),
}

DEFAULT_STREAM_WINDOW_MINUTES = 5

# The name Scaler generates for an unnamed object: its kind, then a repr of its id.
GENERATED_OBJECT_NAME = re.compile(r"^<(\w+) ObjectID\(.*\)>$")

# An object ID is an owner hash then a unique tag, so the tag is the half that tells two of them apart.
OBJECT_TAG_OFFSET = 16

# Enough of an ID to pick one out of a page, short enough that a row of them still reads.
TASK_ID_DISPLAY_LENGTH = 12

# The memory chart's axis floor, so an idle cluster is a flat line rather than noise filling the plot.
MEMORY_CHART_MINIMUM_BYTES = 1024**3

# Ticks on each of the chart's y axes, memory on the left and CPU on the right, the last at the axis maximum.
MEMORY_CHART_TICKS = 5

# The CPU axis floor, so the few percent an idle fleet's agents use do not fill the plot.
CPU_CHART_MINIMUM_PERCENT = 10.0

# Multiples of a power of ten a CPU tick step may be, so every tick reads as a round percentage.
CPU_CHART_STEP_MULTIPLES = (1, 2, 2.5, 5, 10)

# Payloads a browser may fall behind by before its stream is dropped and it reconnects for a full state.
BROWSER_QUEUE_MAX_PAYLOADS = 100

# Rows per page. Server-side only: the browser is told which page it got and how many exist, never
# the size, so it never has to agree with these.
WORKERS_PAGE_SIZE = 50
WORKER_DETAILS_PAGE_SIZE = 20
STREAM_PAGE_SIZE = 50
TASK_LOG_PAGE_SIZE = 50
TASK_EVENTS_PAGE_SIZE = 50
OBJECTS_PAGE_SIZE = 50

# About two lines of a worker's queue. A queue runs to thousands, so the rest is counted, not named.
WORKER_QUEUE_SAMPLE = 12


@dataclasses.dataclass(frozen=True)
class SortSpec:
    """How one table's columns order, and so which columns it sorts by at all.

    `text` orders by the cell's own value and `numeric` compares it as a number.
    `raw` names the field behind a preformatted cell, whose own text does not order.
    """

    text: FrozenSet[str] = frozenset()
    numeric: FrozenSet[str] = frozenset()
    raw: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def accepts(self, field: Any) -> bool:
        return field in self.text or field in self.numeric or field in self.raw

    def sort(self, rows: List[Dict[str, Any]], field: Optional[str], ascending: bool) -> List[Dict[str, Any]]:
        if field is None:
            return rows

        source = self.raw.get(field, field)
        if field in self.text:

            def key(row: Dict[str, Any]) -> Any:
                return str(row.get(source, "")).lower()

        else:

            def key(row: Dict[str, Any]) -> Any:
                value = row.get(source, 0)
                return value if isinstance(value, (int, float)) else 0

        return sorted(rows, key=key, reverse=not ascending)


# One spec per sortable table, each covering that table's own columns.
WORKER_SORT = SortSpec(
    text=frozenset({"name", "manager_id", "host", "task", "itl", "capabilities"}),
    numeric=frozenset(
        {"agt_cpu", "agt_rss", "proc_cpu", "proc_rss", "mem_used_pct", "free", "sent", "queued", "suspended"}
    ),
    raw={"lag": "lag_us", "last_seen": "last_s", "task_age": "task_age_s"},
)
TASK_LOG_SORT = SortSpec(
    text=frozenset({"task_id", "function", "client", "worker", "status", "capabilities"}),
    numeric=frozenset({"time"}),
    raw={"duration": "duration_s", "peak_mem": "peak_bytes", "objects": "object_bytes"},
)
# A trail row's time is a clock reading, so it orders by the sequence number it was appended with.
TASK_EVENTS_SORT = SortSpec(
    text=frozenset({"task_id", "event", "client", "worker", "function", "detail"}), raw={"time": "seq"}
)
OBJECTS_SORT = SortSpec(
    text=frozenset({"object", "name", "type", "client"}), numeric=frozenset({"tasks"}), raw={"size": "size_bytes"}
)
SORTABLE_TABLES = {
    "workers": WORKER_SORT,
    "task_log": TASK_LOG_SORT,
    "task_events": TASK_EVENTS_SORT,
    "objects": OBJECTS_SORT,
}


@dataclasses.dataclass
class BrowserView:
    """What one browser is looking at. Held per socket, so viewers never move each other's view."""

    workers_page: int = 0
    workers_sort: Optional[str] = None
    workers_sort_ascending: bool = True
    worker_details_page: int = 0
    stream_page: int = 0
    task_log_page: int = 0
    task_log_sort: Optional[str] = None
    task_log_sort_ascending: bool = True
    task_events_page: int = 0
    task_events_sort: Optional[str] = None
    task_events_sort_ascending: bool = True
    task_events_task: str = ""  # show only this task's events; empty shows every task
    objects_page: int = 0
    objects_sort: Optional[str] = None
    objects_sort_ascending: bool = True
    stream_window_minutes: int = DEFAULT_STREAM_WINDOW_MINUTES
    memory_scale: str = "linear"

    def apply_view(self, view: Dict[str, Any]) -> None:
        """Apply a browser's `view` message, ignoring anything unrecognised."""
        for name in (
            "workers_page",
            "worker_details_page",
            "stream_page",
            "task_log_page",
            "task_events_page",
            "objects_page",
        ):
            if name in view:
                setattr(self, name, max(0, int(view[name])))

        if "task_events_task" in view:
            self.task_events_task = str(view["task_events_task"])

        for name, spec in SORTABLE_TABLES.items():
            if f"{name}_sort" in view:
                field = view[f"{name}_sort"]
                setattr(self, f"{name}_sort", str(field) if spec.accepts(field) else None)
            if f"{name}_sort_ascending" in view:
                setattr(self, f"{name}_sort_ascending", bool(view[f"{name}_sort_ascending"]))

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


class BrowserStream:
    """One browser's event stream: what it is looking at, and the payloads waiting to be written to it.

    The batcher only appends, so a slow browser falls behind on its own queue rather than the others.
    Past the bound it is closed and reconnects for a full state.

    `view` is written by the connection thread and read by the batcher.
    Each field is set in one assignment, so a race costs one payload built from a half-applied view.
    The next tick corrects it.
    """

    def __init__(self, browser_id: int, view: BrowserView) -> None:
        self.browser_id = browser_id
        self.view = view
        self._payloads: queue.Queue[str] = queue.Queue(maxsize=BROWSER_QUEUE_MAX_PAYLOADS)
        self._closed = threading.Event()

    def offer(self, payload: str) -> None:
        try:
            self._payloads.put_nowait(payload)
        except queue.Full:
            self.close()

    def take(self, timeout: float) -> Optional[str]:
        """The next payload, or None when none arrived within `timeout`."""
        try:
            return self._payloads.get(timeout=timeout)
        except queue.Empty:
            return None

    def is_closed(self) -> bool:
        return self._closed.is_set()

    def close(self) -> None:
        self._closed.set()


class _RenderCache:
    """Memoizes the whole-fleet work (sort, grouping, stream render) for one tick.

    N browsers sharing a sort column cost one sort rather than N.
    """

    def __init__(self) -> None:
        self._sorted: Dict[Tuple[str, Optional[str], bool], List[Dict[str, Any]]] = {}
        self._worker_details: Optional[List[Dict[str, Any]]] = None
        self._stream: Dict[int, Dict[str, Any]] = {}
        self._memory: Dict[Tuple[float, str], Dict[str, Any]] = {}

    def sorted_rows(
        self,
        table: str,
        rows: Callable[[], List[Dict[str, Any]]],
        spec: SortSpec,
        field: Optional[str],
        ascending: bool,
    ) -> List[Dict[str, Any]]:
        """One table in one browser's order.

        Building the row list is deferred: a table nobody sorted pages straight out of what holds it.
        """
        key = (table, field, ascending)
        if key not in self._sorted:
            self._sorted[key] = spec.sort(rows(), field, ascending)
        return self._sorted[key]

    def worker_details(self, app: "WebUIApp") -> List[Dict[str, Any]]:
        if self._worker_details is None:
            self._worker_details = app._build_worker_details()
        return self._worker_details

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


def _oldest_task_age(processor_statuses: Iterable[ProcessorStatus]) -> int:
    """Age of the longest-running task on a worker, 0 when it is idle.

    The oldest is the interesting one: a worker wedged on a single task shows an age that keeps climbing.
    """
    ages = [status.taskAgeSeconds for status in processor_statuses if status.hasTask]
    return max(ages) if ages else 0


def _current_task_label(processor_statuses: Iterable[ProcessorStatus]) -> str:
    """Short id of the task a worker is running, or a count when it is running several."""
    busy = [status for status in processor_statuses if status.hasTask]
    if not busy:
        return "\u2014"
    if len(busy) == 1:
        return bytes(busy[0].currentTaskId).hex()[:TASK_ID_DISPLAY_LENGTH]
    return f"{len(busy)} tasks"


def paginate(items: Collection[Any], page: int, size: int) -> Tuple[List[Any], int, int]:
    """Take the requested page of `items`, clamped to what exists: (rows, page, total pages).

    Walks to the page rather than slicing, so a deque of retained rows costs the page, not a copy.
    """
    total_pages = max(1, (len(items) + size - 1) // size)
    page = min(max(page, 0), total_pages - 1)
    return list(itertools.islice(items, page * size, page * size + size)), page, total_pages


def _format_worker_name(worker_name: str, cutoff: int = 15) -> str:
    if len(worker_name) <= cutoff:
        return worker_name
    return worker_name[:cutoff] + "+"


def _format_client_name(client_name: str, cutoff: int = 24) -> str:
    """A client id is "Client|name|uuid". The uuid identifies it, so keep the head and the uuid's start."""
    if len(client_name) <= cutoff:
        return client_name
    return client_name[:cutoff] + "+"


def _format_object_name(object_name: str, cutoff: int = 40) -> str:
    """An object a client did not name carries a repr of its id, which the Object column already shows.

    Those reprs are the same length and differ only past the cutoff, so a page of them reads as one string.
    Keep the one thing a generated name adds: what kind of object it is.
    """
    generated = GENERATED_OBJECT_NAME.match(object_name)
    if generated:
        return f"<{generated.group(1)}>"
    if len(object_name) <= cutoff:
        return object_name
    return object_name[:cutoff] + "+"


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
                self._note_dispatched_task(state_task, worker_str)

    def _note_dispatched_task(self, state_task: StateTask, worker: str) -> None:
        """Remember what a task is and where it went.

        A dispatched task is not a running one: the scheduler reports it running as soon as it sends it.
        The worker may hold it queued for minutes, so nothing is drawn until a processor picks it up.
        """
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

    def handle_worker_processors(self, worker: str, running: List[Tuple[bytes, int]]) -> None:
        """What this worker's processors are on right now, and how long each has been on it.

        The bar begins at the age the worker reports, so a task the monitor never saw start draws from its real start.
        """
        now = datetime.datetime.now()

        with self._lock:
            self._ensure_worker(worker, now)
            started = self._current_tasks.setdefault(worker, {})
            held = {task_id for task_id, _ in running}
            for task_id, age_seconds in running:
                self._task_id_to_worker[task_id] = worker
                started.setdefault(task_id, now - datetime.timedelta(seconds=age_seconds))

            # a task no processor holds has finished or moved on; its result is what draws the bar
            for finished in [task_id for task_id in started if task_id not in held]:
                started.pop(finished)

            if not started:
                self._current_tasks.pop(worker, None)

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


def _cpu_axis_ticks(peak_percent: float) -> List[float]:
    """Evenly spaced round percentages from 0 up to the smallest axis maximum that holds `peak_percent`.

    CPU is summed across processes, so a fleet computing on 64 cores peaks near 6400%.
    """
    intervals = MEMORY_CHART_TICKS - 1
    peak = max(peak_percent, CPU_CHART_MINIMUM_PERCENT)
    magnitude = 10 ** math.floor(math.log10(peak / intervals))
    steps = (multiple * magnitude for multiple in CPU_CHART_STEP_MULTIPLES)
    step = next(candidate for candidate in steps if candidate * intervals >= peak)
    return [step * index for index in range(MEMORY_CHART_TICKS)]


class MemoryChartState:
    """Server-side state for the chart of what the fleet holds and how hard it computes."""

    def __init__(self) -> None:
        self._start_time = datetime.datetime.now()
        self._points: List[Tuple[float, int]] = []  # (timestamp, memory_bytes)
        # What the fleet holds, sampled once per scheduler update.
        self._live: Deque[Tuple[float, int, float]] = deque()  # (timestamp, rss_bytes, cpu_percent)
        self._memory_store_time = datetime.timedelta(minutes=30)
        self._lock = threading.Lock()

    def record_fleet_sample(self, rss_bytes: int, cpu_percent: float) -> None:
        now = datetime.datetime.now().timestamp()
        cutoff = now - self._memory_store_time.total_seconds()
        with self._lock:
            self._live.append((now, rss_bytes, cpu_percent))
            while self._live[0][0] < cutoff:
                self._live.popleft()

    def _live_series(self, now_ts: float, window_seconds: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        with self._lock:
            samples = list(self._live)
        memory_points, cpu_points = [], []
        for timestamp, rss_bytes, cpu_percent in samples:
            offset = timestamp - now_ts
            if offset < -window_seconds:
                continue
            memory_points.append({"x": round(offset, 2), "y": rss_bytes})
            cpu_points.append({"x": round(offset, 2), "y": round(cpu_percent, 1)})
        return memory_points, cpu_points

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

        live_memory, live_cpu = self._live_series(now_ts, window_seconds)
        if live_memory:
            # the series derived from task profiles is the fallback until the first live sample
            chart_points = live_memory

        max_mem = max(max((point["y"] for point in chart_points), default=0), MEMORY_CHART_MINIMUM_BYTES)
        ticks = [int(max_mem * step / (MEMORY_CHART_TICKS - 1)) for step in range(MEMORY_CHART_TICKS)]
        y_ticks = [{"val": tick, "label": format_bytes(tick)} for tick in ticks]
        cpu_ticks = _cpu_axis_ticks(max((point["y"] for point in live_cpu), default=0.0))

        return {
            "points": chart_points,
            "y_ticks": y_ticks,
            "scale": scale,
            "window": window_seconds,
            "cpu_points": live_cpu,
            # the CPU axis stays linear whatever the memory scale, from 0 up to its last tick
            "cpu_ticks": [{"val": tick, "label": f"{tick:g}%"} for tick in cpu_ticks],
        }


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
        self._browsers: Dict[int, BrowserStream] = {}
        self._browsers_lock = threading.Lock()
        self._browser_ids = itertools.count(1)
        # Held across a whole payload, so a browser's thread never reads state the batcher is rewriting.
        self._state_lock = threading.Lock()

        # server-side state
        self._scheduler_data: Dict[str, Any] = {}
        self._workers_data: Dict[str, Dict[str, Any]] = {}
        self._worker_capabilities: Dict[str, Dict[str, int]] = {}
        # One row per task, newest first, rewritten in place; `_task_log_by_id` holds the same row objects.
        self._task_log: Deque[Dict[str, Any]] = deque(maxlen=self._task_log_max_size)
        self._task_log_by_id: Dict[str, Dict[str, Any]] = {}
        # One row per state change, so a task that is rebalanced, cancelled and retried leaves a trail.
        self._task_events: Deque[Dict[str, Any]] = deque(maxlen=self._task_log_max_size)
        self._task_event_seq: int = 0
        self._task_id_to_function: Dict[str, str] = {}
        # Tasks each worker holds; a dict, not a set, because a worker works through them in arrival order.
        self._worker_tasks: Dict[str, Dict[str, None]] = {}
        self._task_worker: Dict[str, str] = {}
        self._task_stream = TaskStreamState()
        self._memory_chart = MemoryChartState()
        self._worker_processors: Dict[str, Dict[str, Any]] = {}
        # Scaler clients the scheduler sees, rebuilt every status frame; the totals below only ever grow.
        self._clients_data: Dict[str, Dict[str, Any]] = {}
        self._storage_data: Dict[str, Any] = {}
        self._objects_data: List[Dict[str, Any]] = []
        self._objects_total: int = 0
        self._client_task_totals: Dict[str, Dict[str, int]] = {}
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
        self._batcher: Optional[threading.Thread] = None
        self._stopped = threading.Event()

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

    def start_batcher(self) -> None:
        self._batcher = threading.Thread(target=self._batch_loop, name="webui-batcher", daemon=True)
        self._batcher.start()

    def stop(self) -> None:
        self._stopped.set()
        if self._batcher is not None:
            self._batcher.join(timeout=self._broadcast_interval_seconds + 1)
        if self._subscriber is not None:
            self._subscriber.destroy()

    def _batch_loop(self) -> None:
        """Drain the message queue every broadcast interval and push to browsers."""
        while not self._stopped.wait(self._broadcast_interval_seconds):
            with self._state_lock:
                self._batch_once()

    def _batch_once(self) -> None:
        """One tick: apply everything the subscriber queued, then queue each browser its own payload."""
        messages: List[BaseMessage] = []
        while True:
            try:
                messages.append(self._message_queue.get_nowait())
            except queue.Empty:
                break

        has_scheduler_update = False
        has_object_update = False
        has_task_update = False
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
                    self._process_task_state(msg)
                    self._record_task_event(msg)
                    has_task_update = True
                elif isinstance(msg, StateObject):
                    self._process_objects(msg)
                    has_object_update = True
                elif isinstance(msg, StateBalanceAdvice):
                    self._record_balance_advice(msg)
                    has_task_update = True
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

        if has_scheduler_update:
            shared["worker_managers"] = list(self._worker_managers_data.values())
            shared.update(self._storage_section())
            shared.update(self._clients_section())
            shared.update(self._machines_section())

        # The paged parts differ per browser; the fleet-wide work behind them is shared via the cache.
        cache = _RenderCache()
        self._send_to_browsers(
            lambda view: {
                **shared,
                **(self._workers_section(view, cache) if has_scheduler_update else {}),
                **(self._worker_details_section(view, cache) if has_scheduler_update else {}),
                **(self._objects_section(view, cache) if has_object_update else {}),
                **(self._task_log_section(view, cache) if has_task_update else {}),
                **(self._task_events_section(view, cache) if has_task_update else {}),
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
        self._storage_data = self.__storage_section(data.objectManager)

        self._process_clients(data)

        # Key by the decoded name and read each capnp list once: capnp aliasing loses managers past the first.
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
        for worker_data in data.workerManager.workers:
            worker_name = worker_data.workerId.decode()
            current_workers.add(worker_name)
            oldest_task_age = _oldest_task_age(worker_data.processorStatuses)
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
                "host": worker_data.hostname or "\u2014",
                "net_sent": worker_data.netSentBytes,
                "net_recv": worker_data.netRecvBytes,
                "task": _current_task_label(worker_data.processorStatuses),
                "task_age": format_seconds(oldest_task_age),
                "task_age_s": oldest_task_age,
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

            # the highest this monitor has seen each processor at, carried by pid so a restarted one starts over
            peaks = {
                processor["pid"]: processor["peak_rss"]
                for processor in self._worker_processors.get(worker_name, {}).get("processors", [])
            }
            self._worker_processors[worker_name] = {
                "name": _format_worker_name(worker_name),
                "full_name": worker_name,
                "manager_id": self._worker_manager_map.get(worker_name, "\u2014"),
                "rss_free": rss_free,
                "processors": [],
            }
            running_tasks: List[Tuple[bytes, int]] = []
            for ps in sorted(worker_data.processorStatuses, key=lambda x: x.pid):
                rss_val = int(ps.resource.rss / 1e6)
                if ps.hasTask:
                    running_tasks.append((bytes(ps.currentTaskId), ps.taskAgeSeconds))
                task_id = bytes(ps.currentTaskId).hex() if ps.hasTask else ""
                self._worker_processors[worker_name]["processors"].append(
                    {
                        "pid": ps.pid,
                        "cpu": round(ps.resource.cpu / 10, 1),
                        "rss": rss_val,
                        "peak_rss": max(rss_val, peaks.get(ps.pid, 0)),
                        "initialized": bool(ps.initialized),
                        "has_task": bool(ps.hasTask),
                        "suspended": bool(ps.suspended),
                        "task_id": task_id,
                        "task": task_id[:TASK_ID_DISPLAY_LENGTH] if task_id else "\u2014",
                        "task_age": format_seconds(ps.taskAgeSeconds) if ps.hasTask else "\u2014",
                    }
                )

            # the stream draws what the processors hold, so a queued task is not a bar
            self._task_stream.handle_worker_processors(worker_name, running_tasks)

        # remove dead workers
        dead = set(self._workers_data.keys()) - current_workers
        for w in dead:
            self._workers_data.pop(w, None)
            self._worker_processors.pop(w, None)
            self._worker_manager_map.pop(w, None)
            self.__release_worker_tasks(w)
            self._task_stream.handle_worker_state(
                StateWorker(workerId=WorkerID(w.encode()), state=WorkerState.disconnected, capabilities=[])
            )

        # One live sample of what the whole fleet is holding, for the memory and CPU charts.
        fleet_rss = sum(
            (worker.get("proc_rss", 0) + worker.get("agt_rss", 0)) for worker in self._workers_data.values()
        )
        fleet_cpu = sum(
            (worker.get("proc_cpu", 0.0) + worker.get("agt_cpu", 0.0)) for worker in self._workers_data.values()
        )
        self._memory_chart.record_fleet_sample(int(fleet_rss * 1e6), fleet_cpu)

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

    @staticmethod
    def __storage_section(status: ObjectManagerStatus) -> Dict[str, Any]:
        """What the object storage server holds, and what is stuck waiting on it.

        `pending` counts requests for an object nobody has created yet.
        A client blocks in `get_object` until that happens, so a number here that does not fall is a stalled fetch.
        """
        held = status.storageTotalBytes
        unique = status.storageUniqueCount
        return {
            "tracked_objects": status.numberOfObjects,
            "objects": status.storageObjectCount,
            "unique_objects": unique,
            "size": format_bytes(held),
            "shared": status.storageObjectCount - unique,
            "pending": status.storagePendingRequests,
            "pending_objects": status.storagePendingObjects,
            "oldest_pending": format_seconds(status.storageOldestPendingS) if status.storagePendingRequests else "0s",
        }

    def _process_objects(self, state: StateObject) -> None:
        """The biggest objects the scheduler tracks, and the tasks holding each one."""
        rows = []
        for detail in state.objects:
            task_ids = [bytes(task_id).hex()[:TASK_ID_DISPLAY_LENGTH] for task_id in detail.taskIds]
            creator = bytes(detail.creator).decode(errors="replace")
            object_id = bytes(detail.objectId)
            name = detail.name.decode(errors="replace")
            rows.append(
                {
                    "object": object_id[OBJECT_TAG_OFFSET:].hex()[:TASK_ID_DISPLAY_LENGTH],
                    "object_id": object_id.hex(),
                    "name": _format_object_name(name),
                    "full_name": name,
                    "type": detail.objectType.name,
                    "size": format_bytes(detail.size),
                    "size_bytes": detail.size,
                    "client": _format_client_name(creator) if creator else "\u2014",
                    "full_client": creator,
                    "tasks": detail.taskCount,
                    "task_ids": task_ids,
                }
            )
        self._objects_data = rows
        self._objects_total = state.totalObjects

    def _objects_section(self, view: BrowserView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of the object rows the scheduler sent, which are the biggest few of `objects_total`."""
        objects = cache.sorted_rows(
            "objects", lambda: list(self._objects_data), OBJECTS_SORT, view.objects_sort, view.objects_sort_ascending
        )
        rows, page, total_pages = paginate(objects, view.objects_page, OBJECTS_PAGE_SIZE)
        view.objects_page = page
        return {
            "objects": rows,
            "objects_held": len(objects),
            "objects_total": self._objects_total,
            "objects_page": page,
            "objects_pages": total_pages,
        }

    def _process_clients(self, data: StateScheduler) -> None:
        """One row per client the scheduler has heard from, with the totals this GUI has counted."""
        seen: Set[str] = set()
        clients: Dict[str, Dict[str, Any]] = {}
        for client in data.clientManager.clients:
            name = bytes(client.clientId).decode(errors="replace")
            seen.add(name)
            totals = self._client_task_totals.setdefault(name, {"finished": 0, "failed": 0})
            clients[name] = {
                "client": _format_client_name(name),
                "full_client": name,
                "host": client.hostname or "\u2014",
                "tasks": client.numTask,
                "finished": totals["finished"],
                "failed": totals["failed"],
                "cpu": format_percentage(client.resource.cpu),
                "rss": format_bytes(client.resource.rss),
                "latency": format_microseconds(client.latencyUS),
                "connected": format_seconds(client.connectedS),
                "last_seen": format_seconds(client.lastSeenS),
            }

        self._clients_data = clients
        for name in set(self._client_task_totals) - seen:
            self._client_task_totals.pop(name)

    def _count_task_outcome(self, client_name: str, state: TaskState) -> None:
        """Tally a finished task against the client that submitted it, for the Clients page."""
        if not client_name:
            return

        totals = self._client_task_totals.setdefault(client_name, {"finished": 0, "failed": 0})
        totals["finished"] += 1
        if state in (TaskState.failed, TaskState.failedWorkerDied):
            totals["failed"] += 1

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
            self.__release_worker_tasks(worker_id)

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

        full_client = state_task.client.decode(errors="replace") if state_task.client else ""
        client_str = _format_client_name(full_client) if full_client else ""

        entry = self._task_log_by_id.get(task_id_hex)
        first_sighting = entry is None
        if entry is None:
            entry = {
                "task_id": task_id_hex,
                "time": now.timestamp(),
                "worker": "",
                "full_worker": "",
                "client": "",
                "full_client": "",
            }
            self.__remember_task(entry)

        # A later message that omits the worker or the client keeps what the task already carried.
        if worker_str:
            entry["worker"], entry["full_worker"] = worker_str, full_worker
        if full_client:
            entry["client"], entry["full_client"] = client_str, full_client

        entry["function"] = func_name
        entry["status"] = state_task.state.name
        entry["capabilities"] = caps_str
        entry["objects"] = format_bytes(state_task.objectBytes) if state_task.objectBytes else "\u2014"
        entry["object_bytes"] = state_task.objectBytes
        entry["duration"] = ""
        entry["duration_s"] = 0.0
        entry["peak_mem"] = ""
        entry["peak_bytes"] = 0

        if not any(state_task.state == s for s in COMPLETED_TASK_STATUSES):
            self.__hold_task(task_id_hex, entry["full_worker"])
            return entry

        self.__hold_task(task_id_hex, "")
        self._count_task_outcome(entry["full_client"], state_task.state)
        self._task_id_to_function.pop(task_id_hex, None)
        self._task_log_total += 1

        entry["duration"] = "N/A"
        entry["peak_mem"] = "N/A"
        if state_task.metadata != b"":
            try:
                profile = ProfileResult.deserialize(state_task.metadata)
                entry["duration"] = f"{profile.duration_s:.2f}s"
                entry["duration_s"] = profile.duration_s
                entry["peak_mem"] = format_bytes(profile.memory_peak) if profile.memory_peak != 0 else "0"
                entry["peak_bytes"] = profile.memory_peak
                # back-compute submitted time for a task this GUI never saw start
                if first_sighting:
                    entry["time"] = now.timestamp() - profile.duration_s
            except struct.error:
                pass
        return entry

    def __remember_task(self, entry: Dict[str, Any]) -> None:
        """Put a newly seen task at the top of the log, dropping the oldest once the log is full."""
        if self._task_log and len(self._task_log) == self._task_log_max_size:
            evicted = self._task_log[-1]["task_id"]
            self._task_log_by_id.pop(evicted, None)
            self.__hold_task(evicted, "")
        self._task_log.appendleft(entry)
        self._task_log_by_id[entry["task_id"]] = entry

    def __hold_task(self, task_id: str, worker: str) -> None:
        """Move a task to `worker`, or off every worker when it is empty."""
        previous = self._task_worker.get(task_id, "")
        if previous == worker:
            return

        if previous:
            held = self._worker_tasks.get(previous)
            if held is not None:
                held.pop(task_id, None)
                if not held:
                    self._worker_tasks.pop(previous, None)

        if worker:
            self._worker_tasks.setdefault(worker, {})[task_id] = None
            self._task_worker[task_id] = worker
        else:
            self._task_worker.pop(task_id, None)

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

    def _record_task_event(self, state_task: StateTask) -> None:
        """One immutable row per state change, so the sequence a task went through stays readable."""
        task_id = state_task.taskId.hex()
        worker = state_task.worker.decode() if state_task.worker else ""
        client = state_task.client.decode(errors="replace") if state_task.client else ""
        self.__append_task_event(
            task_id=task_id,
            event=state_task.state.name,
            worker=worker,
            client=client,
            function=state_task.functionName.decode(errors="replace") if state_task.functionName else "",
            detail=format_bytes(state_task.objectBytes) if state_task.objectBytes else "",
        )

    def _record_balance_advice(self, advice: StateBalanceAdvice) -> None:
        """A rebalance moves tasks off a worker; without this the trail just shows them reappearing."""
        worker = advice.workerId.decode() if advice.workerId else ""
        for task_id in advice.taskIds:
            self.__append_task_event(
                task_id=bytes(task_id).hex(),
                event="rebalance",
                worker=worker,
                client="",
                function="",
                detail="moved off this worker",
            )

    def __append_task_event(
        self, task_id: str, event: str, worker: str, client: str, function: str, detail: str
    ) -> None:
        """A row of the trail.

        A result message names neither the worker that ran the task nor the client that submitted it.
        Both fall back to the task's own row: a row reading "success" against no worker says nothing.
        """
        known = self._task_log_by_id.get(task_id, {})
        self._task_event_seq += 1
        self._task_events.appendleft(
            {
                "seq": self._task_event_seq,
                "time": datetime.datetime.now().strftime("%H:%M:%S"),
                "task_id": task_id,
                "function": function or known.get("function", ""),
                "event": event,
                "worker": _format_worker_name(worker) if worker else known.get("worker", "") or "\u2014",
                "client": _format_client_name(client) if client else known.get("client", "") or "\u2014",
                "detail": detail,
            }
        )

    def _storage_section(self) -> Dict[str, Any]:
        """The object storage card, absent until the scheduler has reported once.

        Sending it early would put measured-looking zeros where the page still shows placeholders.
        """
        return {"storage": self._storage_data} if self._storage_data else {}

    def _clients_section(self) -> Dict[str, Any]:
        """Every connected client. There are far fewer clients than workers, so this page is not paged."""
        rows = sorted(self._clients_data.values(), key=lambda row: row["full_client"])
        return {"clients": rows, "clients_total": len(rows)}

    def _task_log_section(self, view: BrowserView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of the task list: one row per retained task, newest first unless sorted."""
        tasks: Collection[Dict[str, Any]] = self._task_log
        if view.task_log_sort is not None:
            tasks = cache.sorted_rows(
                "task_log",
                lambda: list(self._task_log),
                TASK_LOG_SORT,
                view.task_log_sort,
                view.task_log_sort_ascending,
            )
        rows, page, total_pages = paginate(tasks, view.task_log_page, TASK_LOG_PAGE_SIZE)
        view.task_log_page = page
        return {
            "task_log": rows,
            "task_log_page": page,
            "task_log_pages": total_pages,
            "task_log_held": len(self._task_log),
            "task_log_total": self._task_log_total,
        }

    def _task_events_section(self, view: BrowserView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of the task log: one row per state change, newest first, one task's or every task's."""
        filtered: Collection[Dict[str, Any]] = self._task_events
        if view.task_events_task:
            filtered = [event for event in self._task_events if event["task_id"] == view.task_events_task]

        events = filtered
        if view.task_events_sort is not None:
            events = cache.sorted_rows(
                f"task_events:{view.task_events_task}",
                lambda: list(filtered),
                TASK_EVENTS_SORT,
                view.task_events_sort,
                view.task_events_sort_ascending,
            )

        rows, page, total_pages = paginate(events, view.task_events_page, TASK_EVENTS_PAGE_SIZE)
        view.task_events_page = page
        return {
            "task_events": rows,
            "task_events_page": page,
            "task_events_pages": total_pages,
            "task_events_held": len(events),
            "task_events_task": view.task_events_task,
        }

    def _machines_section(self) -> Dict[str, Any]:
        """One row per physical machine, however many workers it hosts.

        `netSentBytes` and `netRecvBytes` are host-wide, so they are read once per hostname, never summed.
        Everything else is per-worker and is added up.
        """
        machines: Dict[str, Dict[str, Any]] = {}
        for worker in self._workers_data.values():
            host = worker.get("host") or "\u2014"
            entry = machines.setdefault(
                host,
                {
                    "host": host,
                    "workers": 0,
                    "busy": 0,
                    "proc_cpu": 0.0,
                    "agt_cpu": 0.0,
                    "proc_rss": 0,
                    "agt_rss": 0,
                    "rss_free": 0,
                    "mem_limit": 0,
                    "queued": 0,
                    "sent": 0,
                    "net_sent": 0,
                    "net_recv": 0,
                    "managers": set(),
                },
            )
            entry["workers"] += 1
            entry["busy"] += 1 if worker.get("task", "\u2014") != "\u2014" else 0
            entry["proc_cpu"] += worker.get("proc_cpu", 0.0)
            entry["agt_cpu"] += worker.get("agt_cpu", 0.0)
            entry["proc_rss"] += worker.get("proc_rss", 0)
            entry["agt_rss"] += worker.get("agt_rss", 0)
            entry["queued"] += worker.get("queued", 0)
            entry["sent"] += worker.get("sent", 0)
            entry["managers"].add(worker.get("manager_id", "\u2014"))
            # host-wide, so take one reading rather than accumulating
            entry["rss_free"] = max(entry["rss_free"], worker.get("rss_free", 0))
            entry["mem_limit"] = max(entry["mem_limit"], worker.get("mem_limit", 0))
            entry["net_sent"] = max(entry["net_sent"], worker.get("net_sent", 0))
            entry["net_recv"] = max(entry["net_recv"], worker.get("net_recv", 0))

        rows = []
        for entry in machines.values():
            used = entry["proc_rss"] + entry["agt_rss"]
            rows.append(
                {
                    "host": entry["host"],
                    "workers": entry["workers"],
                    "busy": entry["busy"],
                    "idle": entry["workers"] - entry["busy"],
                    "managers": ", ".join(sorted(name for name in entry["managers"] if name)),
                    "cpu": round(entry["proc_cpu"] + entry["agt_cpu"], 1),
                    "rss": used,
                    "rss_free": entry["rss_free"],
                    "mem_limit": entry["mem_limit"],
                    "mem_used_pct": round(100 * used / (used + entry["rss_free"]), 1) if entry["rss_free"] else 0,
                    "queued": entry["queued"],
                    "sent": entry["sent"],
                    "net_sent": format_bytes(entry["net_sent"]),
                    "net_recv": format_bytes(entry["net_recv"]),
                }
            )
        rows.sort(key=lambda row: row["host"])
        return {"machines": rows, "machines_total": len(rows)}

    def _workers_section(self, view: BrowserView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of the fleet in this browser's order; the browser holds a page and cannot sort."""
        workers = cache.sorted_rows(
            "workers",
            lambda: list(self._workers_data.values()),
            WORKER_SORT,
            view.workers_sort,
            view.workers_sort_ascending,
        )
        rows, page, total_pages = paginate(workers, view.workers_page, WORKERS_PAGE_SIZE)
        view.workers_page = page
        return {
            "workers": rows,
            "workers_total": self._fleet_worker_count(),
            "workers_page": page,
            "workers_pages": total_pages,
        }

    def _worker_details_section(self, view: BrowserView, cache: "_RenderCache") -> Dict[str, Any]:
        """One page of worker detail; the per-manager summaries still cover every worker."""
        groups = cache.worker_details(self)
        flat: List[Tuple[str, Dict[str, Any]]] = [
            (group["manager_id"], worker) for group in groups for worker in group["workers"]
        ]
        page_rows, page, total_pages = paginate(flat, view.worker_details_page, WORKER_DETAILS_PAGE_SIZE)
        view.worker_details_page = page

        shown: Dict[str, List[Dict[str, Any]]] = {}
        for manager_id, worker in page_rows:
            shown.setdefault(manager_id, []).append(worker)

        paged_groups = [dict(group, workers=shown.get(group["manager_id"], [])) for group in groups]
        return {
            "worker_details": paged_groups,
            "worker_details_page": page,
            "worker_details_pages": total_pages,
            "worker_details_total": len(flat),
        }

    def _stream_section(self, view: BrowserView, cache: "_RenderCache") -> Dict[str, Any]:
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

        Workers can run without a registered manager (a native manager in fixed mode), zeroing those totals.
        The workers this backend holds are then the fleet it knows about.
        """
        return max(self._total_workers, len(self._workers_data))

    def _build_worker_details(self) -> List[Dict[str, Any]]:
        """Every worker with what it is running and what is waiting behind it, grouped by manager.

        `_worker_details_section` slices one page of workers out of this per browser.
        """
        # Group every worker by manager for complete per-manager summaries.
        managers: Dict[str, List[Dict[str, Any]]] = {}
        for worker_name, wp in self._worker_processors.items():
            mid = wp.get("manager_id", "—")
            managers.setdefault(mid, []).append(dict(wp, **self.__worker_task_lists(worker_name, wp["processors"])))

        # Ensure all known worker managers appear even if they have no workers
        for mid in self._worker_managers_data:
            managers.setdefault(mid, [])

        result = []
        for manager_id, workers in sorted(managers.items()):
            total_rss = 0
            total_cpu = 0.0
            total_processors = 0
            active_processors = 0
            total_queued = 0
            for wp in workers:
                total_queued += wp["queue_depth"]
                total_rss += wp["rss"]
                total_cpu += wp["cpu"]
                for proc in wp["processors"]:
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
                    "total_queued": total_queued,
                    "workers": workers,
                }
            )
        return result

    def __worker_task_lists(self, worker_name: str, processors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """What one worker is running, what is waiting behind it, and the numbers its row carries.

        A processor names the task it is on, so everything else the worker holds is queued there.
        The processor at work leads, then the suspended ones holding a task, then any that are idle.
        """
        running = {proc["task_id"] for proc in processors if proc["task_id"]}
        queued = [task_id for task_id in self._worker_tasks.get(worker_name, {}) if task_id not in running]
        in_order = sorted(processors, key=lambda proc: (proc.get("suspended", False), not proc["task_id"]))

        worker = self._workers_data.get(worker_name, {})
        return {
            "processors": [dict(proc, function=self.__function_of(proc["task_id"])) for proc in in_order],
            "queue": [self.__queued_task(task_id) for task_id in queued[:WORKER_QUEUE_SAMPLE]],
            "queue_named": len(queued),
            # what the worker itself reports queued, which counts tasks this monitor never saw arrive
            "queue_depth": worker.get("queued", 0),
            "running": len(running),
            "host": worker.get("host", "\u2014"),
            "cpu": round(worker.get("proc_cpu", 0.0) + worker.get("agt_cpu", 0.0), 1),
            "rss": worker.get("worker_rss", 0),
            "mem_used_pct": worker.get("mem_used_pct", 0.0),
            "mem_limit": worker.get("mem_limit", 0),
            "free": worker.get("free", 0),
            "sent": worker.get("sent", 0),
            "last_seen": worker.get("last_seen", "\u2014"),
            "capabilities": worker.get("capabilities", ""),
        }

    def __queued_task(self, task_id: str) -> Dict[str, str]:
        """A queued task as its worker's row shows it. The whole id travels, because the row links to its trail."""
        return {"task_id": task_id, "function": self.__function_of(task_id)}

    def __function_of(self, task_id: str) -> str:
        return self._task_log_by_id.get(task_id, {}).get("function", "") if task_id else ""

    def __release_worker_tasks(self, worker_name: str) -> None:
        """Forget what a departed worker held; the scheduler places those tasks again elsewhere."""
        for task_id in self._worker_tasks.pop(worker_name, {}):
            self._task_worker.pop(task_id, None)

    def __scheduler_liveness(self) -> Dict[str, Any]:
        """last_seen + stale flag derived from the last StateScheduler heartbeat."""
        if self._last_scheduler_heartbeat_time is None:
            return {"last_seen": "\u2014", "stale": False}
        elapsed = int((datetime.datetime.now() - self._last_scheduler_heartbeat_time).total_seconds())
        return {"last_seen": format_seconds(elapsed), "stale": elapsed > self._scheduler_stale_seconds}

    def get_full_state(self, view: BrowserView) -> Dict[str, Any]:
        """The whole current state for one browser, in that browser's view.

        Built from what the batcher has processed, so anything still queued arrives one interval later.
        This never drains the queue itself: only the batcher thread writes this state.
        """
        with self._state_lock:
            return self.__full_state(view)

    def __full_state(self, view: BrowserView) -> Dict[str, Any]:
        cache = _RenderCache()
        stream_data = self._stream_section(view, cache)
        memory_data = cache.memory(self, stream_data["window"], view.memory_scale)
        # Build scheduler data with a last_seen derived from the periodic heartbeat.
        sched = dict(self._scheduler_data) if self._scheduler_data else {}
        sched.update(self.__scheduler_liveness())

        return {
            "scheduler": sched,
            **self._workers_section(view, cache),
            **self._machines_section(),
            **self._clients_section(),
            **self._storage_section(),
            **self._objects_section(view, cache),
            **self._task_log_section(view, cache),
            **self._task_events_section(view, cache),
            "task_stream": stream_data,
            "memory_chart": memory_data,
            **self._worker_details_section(view, cache),
            "worker_managers": list(self._worker_managers_data.values()),
            "settings": view.settings(),
        }

    def view_update(self, view: BrowserView) -> Dict[str, Any]:
        """The paged sections for one browser, answered on its change instead of at the next tick."""
        with self._state_lock:
            return self.__view_update(view)

    def __view_update(self, view: BrowserView) -> Dict[str, Any]:
        cache = _RenderCache()
        stream_data = self._stream_section(view, cache)
        return {
            **self._workers_section(view, cache),
            **self._machines_section(),
            **self._worker_details_section(view, cache),
            **self._objects_section(view, cache),
            **self._task_log_section(view, cache),
            **self._task_events_section(view, cache),
            "task_stream": stream_data,
            "memory_chart": cache.memory(self, stream_data["window"], view.memory_scale),
            "settings": view.settings(),
        }

    def add_browser(self, view: BrowserView) -> "BrowserStream":
        """Register a browser's event stream, opening on `view`. Its id is what its later view requests name."""
        stream = BrowserStream(browser_id=next(self._browser_ids), view=view)
        with self._browsers_lock:
            self._browsers[stream.browser_id] = stream
        return stream

    def remove_browser(self, browser_id: int) -> None:
        with self._browsers_lock:
            self._browsers.pop(browser_id, None)

    def get_browser(self, browser_id: int) -> Optional["BrowserStream"]:
        with self._browsers_lock:
            return self._browsers.get(browser_id)

    def _send_to_browsers(self, build_payload: Callable[[BrowserView], Dict[str, Any]]) -> None:
        """Queue one payload per browser: each is on its own page and sort order.

        The queue is what keeps a slow browser from holding up the batch: this never writes a socket.
        """
        with self._browsers_lock:
            streams = list(self._browsers.values())
        for stream in streams:
            stream.offer(json.dumps(build_payload(stream.view)))


def create_app(config: WebGUIConfig) -> WebUIApp:
    """The GUI's state, subscribed to the scheduler and pushing to browsers."""
    app = WebUIApp(config)

    # Subscribe before the HTTP server binds, so the monitor stream is collected from the first frame.
    app.start_subscriber()
    app.start_batcher()
    return app
