"""Every sortable table's columns, which three files have to agree on.

A table's field list in app.js is its column order: one cell per entry, filled from the row field of that name.
A column added to one file and not the others writes into the wrong cell, so this pins all three together.
"""

import json
import re
import unittest
from typing import Any, Dict, List

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ObjectMetadata,
    ProcessorStatus,
    Resource,
    ScalingManagerStatus,
    StateObject,
    StateScheduler,
    StateTask,
    TaskManagerStatus,
    TaskState,
    WorkerManagerStatus,
    WorkerStatus,
)
from scaler.ui.app import (
    OBJECTS_SORT,
    STATIC_DIR,
    TASK_EVENTS_SORT,
    TASK_LOG_SORT,
    WORKER_SORT,
    SortSpec,
    WebGUIConfig,
    WebUIApp,
)


def make_app() -> WebUIApp:
    return WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))


def columns(listing: str) -> List[str]:
    """The column order app.js holds for one table."""
    source = (STATIC_DIR / "app.js").read_text()
    found = re.search(rf"var {listing} = (\[.*?\]);", source, re.DOTALL)
    assert found is not None, f"app.js declares no {listing}"
    return json.loads(re.sub(r"\s+", " ", found.group(1)))


def headers(table_id: str) -> List[str]:
    """The header cells index.html holds for one table."""
    page = (STATIC_DIR / "index.html").read_text()
    table = page.split(f'id="{table_id}"', 1)[1].split("</thead>", 1)[0]
    return re.findall(r"<th[^>]*>(.*?)</th>", table)


def worker_row() -> Dict:
    app = make_app()
    status = StateScheduler(
        binder=BinderStatus(received=[], sent=[]),
        scheduler=Resource(cpu=0, rss=0),
        rssFree=0,
        clientManager=ClientManagerStatus(clients=[]),
        objectManager=ObjectManagerStatus(numberOfObjects=0),
        taskManager=TaskManagerStatus(stateToCount=[]),
        workerManager=WorkerManagerStatus(
            workers=[
                WorkerStatus(
                    workerId=b"Worker|one",
                    agent=Resource(cpu=10, rss=1_000_000),
                    rssFree=8_000_000,
                    memLimit=16_000_000,
                    free=2,
                    sent=3,
                    queued=4,
                    suspended=0,
                    lagUS=500,
                    lastS=1,
                    itl=" ",
                    processorStatuses=[
                        ProcessorStatus(
                            pid=42,
                            initialized=True,
                            hasTask=True,
                            suspended=False,
                            resource=Resource(cpu=250, rss=2_000_000),
                            currentTaskId=b"\xab\xcd" * 16,
                            taskAgeSeconds=7,
                        )
                    ],
                    hostname="box-1",
                    netSentBytes=100,
                    netRecvBytes=200,
                )
            ]
        ),
        scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
    )
    app._process_scheduler(StateScheduler.from_bytes(status.to_bytes()))
    return app._workers_data["Worker|one"]


def task_rows() -> Dict[str, Dict]:
    """A finished task, as the task list and the task log each hold it."""
    app = make_app()
    state = StateTask.from_bytes(
        StateTask(
            taskId=b"t" * 32,
            functionName=b"work",
            state=TaskState.success,
            worker=b"Worker|one",
            client=b"Client|one",
            capabilities=[],
            metadata=b"",
            objectBytes=1024,
        ).to_bytes()
    )
    app._process_task_state(state)
    app._record_task_event(state)
    return {"task_log": app._task_log[0], "task_events": app._task_events[0]}


def object_row() -> Dict:
    app = make_app()
    app._process_objects(
        StateObject.from_bytes(
            StateObject(
                objects=[
                    StateObject.ObjectDetail(
                        objectId=b"a" * 32,
                        name=b"frame",
                        objectType=ObjectMetadata.ObjectContentType.object,
                        size=2_000_000,
                        creator=b"Client|one",
                        taskIds=[b"b" * 32],
                        taskCount=1,
                    )
                ],
                totalObjects=1,
            ).to_bytes()
        )
    )
    return app._objects_data[0]


TABLES = {
    "workers": ("workers-table", "WORKER_FIELDS", WORKER_SORT),
    "task_log": ("tasklog-table", "TASK_LOG_FIELDS", TASK_LOG_SORT),
    "task_events": ("taskevents-table", "TASK_EVENT_FIELDS", TASK_EVENTS_SORT),
    "objects": ("objects-table", "OBJECT_FIELDS", OBJECTS_SORT),
}


class TestTableColumns(unittest.TestCase):
    def test_every_column_has_a_header(self) -> None:
        for name, (table_id, listing, _) in TABLES.items():
            with self.subTest(table=name):
                self.assertEqual(len(headers(table_id)), len(columns(listing)))

    def test_every_column_can_be_sorted_by(self) -> None:
        """A header click sends its field name, which the server drops unless it can sort by it."""
        for name, (_, listing, spec) in TABLES.items():
            with self.subTest(table=name):
                missing = [field for field in columns(listing) if not spec.accepts(field)]
                self.assertEqual(missing, [], f"columns the server will not sort by: {missing}")

    def test_every_column_is_a_field_of_the_row_the_backend_builds(self) -> None:
        rows = dict(task_rows(), workers=worker_row(), objects=object_row())
        for name, (_, listing, _spec) in TABLES.items():
            with self.subTest(table=name):
                missing = [field for field in columns(listing) if field not in rows[name]]
                self.assertEqual(missing, [], f"app.js names columns the backend never sends: {missing}")

    def test_a_preformatted_column_sorts_by_the_value_behind_it(self) -> None:
        """A cell like "1.9M" or "2.00s" orders by magnitude, which its own text does not."""
        rows = dict(task_rows(), workers=worker_row(), objects=object_row())
        for name, (_, _, spec) in TABLES.items():
            with self.subTest(table=name):
                missing = [source for source in spec.raw.values() if source not in rows[name]]
                self.assertEqual(missing, [], f"sort fields the backend never sends: {missing}")


class TestSortSpec(unittest.TestCase):
    def test_an_unsortable_column_is_refused(self) -> None:
        self.assertFalse(WORKER_SORT.accepts("password"))

    def test_rows_order_by_the_raw_field_of_a_preformatted_column(self) -> None:
        spec = SortSpec(raw={"size": "size_bytes"})
        rows = [{"size": "9K", "size_bytes": 9_000}, {"size": "1.9M", "size_bytes": 2_000_000}]
        self.assertEqual([row["size"] for row in spec.sort(rows, "size", True)], ["9K", "1.9M"])
        self.assertEqual([row["size"] for row in spec.sort(rows, "size", False)], ["1.9M", "9K"])

    def test_text_orders_without_regard_to_case(self) -> None:
        spec = SortSpec(text=frozenset({"function"}))
        rows = [{"function": "beta"}, {"function": "Alpha"}]
        self.assertEqual([row["function"] for row in spec.sort(rows, "function", True)], ["Alpha", "beta"])

    def test_a_missing_value_sorts_as_zero_rather_than_raising(self) -> None:
        spec = SortSpec(numeric=frozenset({"free"}))
        rows: List[Dict[str, Any]] = [{"free": 5}, {}, {"free": "N/A"}]
        self.assertEqual([row.get("free") for row in spec.sort(rows, "free", True)], [None, "N/A", 5])


if __name__ == "__main__":
    unittest.main()
