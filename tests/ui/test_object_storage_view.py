"""What the monitor shows about the object storage server, and about the objects it holds.

The store reports what it holds through its `infoGetTotal` request.
The scheduler reports which tasks name each object, because the store never sees a task.
"""

import struct
import unittest
from typing import List, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import ObjectStorageTotals
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ObjectMetadata,
    Resource,
    ScalingManagerStatus,
    StateObject,
    StateScheduler,
    TaskManagerStatus,
    WorkerManagerStatus,
)
from scaler.ui.app import OBJECTS_PAGE_SIZE, BrowserView, WebGUIConfig, WebUIApp, _RenderCache


def make_app() -> WebUIApp:
    return WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))


def make_status(object_manager: ObjectManagerStatus) -> StateScheduler:
    """A status frame as the GUI receives it: field reads need a deserialized struct."""
    return StateScheduler.from_bytes(
        StateScheduler(
            binder=BinderStatus(received=[], sent=[]),
            scheduler=Resource(cpu=0, rss=0),
            rssFree=0,
            clientManager=ClientManagerStatus(clients=[]),
            objectManager=object_manager,
            taskManager=TaskManagerStatus(stateToCount=[]),
            workerManager=WorkerManagerStatus(workers=[]),
            scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
        ).to_bytes()
    )


class TestObjectStorageTotals(unittest.TestCase):
    def test_a_full_payload_reads_every_field(self) -> None:
        totals = ObjectStorageTotals.from_payload(struct.pack("<6Q", 10, 7, 4096, 2, 1, 42))
        self.assertEqual(totals.object_count, 10)
        self.assertEqual(totals.unique_object_count, 7)
        self.assertEqual(totals.total_bytes, 4096)
        self.assertEqual(totals.pending_request_count, 2)
        self.assertEqual(totals.pending_object_count, 1)
        self.assertEqual(totals.oldest_pending_seconds, 42)

    def test_a_server_answering_with_fewer_fields_stays_readable(self) -> None:
        totals = ObjectStorageTotals.from_payload(struct.pack("<3Q", 10, 7, 4096))
        self.assertEqual(totals.total_bytes, 4096)
        self.assertEqual(totals.pending_request_count, 0)
        self.assertEqual(totals.oldest_pending_seconds, 0)

    def test_a_server_answering_with_more_fields_stays_readable(self) -> None:
        totals = ObjectStorageTotals.from_payload(struct.pack("<8Q", 10, 7, 4096, 2, 1, 42, 99, 99))
        self.assertEqual(totals.oldest_pending_seconds, 42)


class TestStorageCard(unittest.TestCase):
    def test_the_card_reports_what_the_store_holds(self) -> None:
        app = make_app()
        app._process_scheduler(
            make_status(
                ObjectManagerStatus(
                    numberOfObjects=4,
                    storageObjectCount=10,
                    storageUniqueCount=7,
                    storageTotalBytes=4096,
                    storagePendingRequests=2,
                    storagePendingObjects=1,
                    storageOldestPendingS=42,
                )
            )
        )

        storage = app._storage_data
        self.assertEqual(storage["objects"], 10)
        self.assertEqual(storage["unique_objects"], 7)
        self.assertEqual(storage["shared"], 3)
        self.assertEqual(storage["pending"], 2)
        self.assertEqual(storage["oldest_pending"], "42s")

    def test_nothing_waiting_reports_no_age(self) -> None:
        app = make_app()
        app._process_scheduler(make_status(ObjectManagerStatus(numberOfObjects=0)))

        self.assertEqual(app._storage_data["pending"], 0)
        self.assertEqual(app._storage_data["oldest_pending"], "0s")

    def test_the_card_is_not_sent_before_the_scheduler_has_reported(self) -> None:
        """A browser that opens first must keep the page's placeholders, not read zeros as measurements."""
        app = make_app()
        self.assertEqual(app._storage_section(), {})

        app._process_scheduler(make_status(ObjectManagerStatus(numberOfObjects=0)))
        self.assertIn("storage", app._storage_section())


def make_objects(*object_ids: bytes, name: bytes = b"payload", sizes: Optional[List[int]] = None) -> StateObject:
    """A StateObject as the GUI receives it: field reads need a deserialized struct."""
    payload_sizes = sizes if sizes is not None else [1] * len(object_ids)
    return StateObject.from_bytes(
        StateObject(
            objects=[
                StateObject.ObjectDetail(
                    objectId=object_id,
                    name=name,
                    objectType=ObjectMetadata.ObjectContentType.object,
                    size=payload_sizes[index],
                    creator=b"Client|one",
                    taskIds=[],
                    taskCount=0,
                )
                for index, object_id in enumerate(object_ids)
            ],
            totalObjects=len(object_ids),
        ).to_bytes()
    )


class TestObjectsView(unittest.TestCase):
    def test_an_object_row_names_its_client_and_its_tasks(self) -> None:
        app = make_app()
        state = StateObject.from_bytes(
            StateObject(
                objects=[
                    StateObject.ObjectDetail(
                        objectId=b"a" * 32,
                        name=b"heavy_frame",
                        objectType=ObjectMetadata.ObjectContentType.object,
                        size=2_000_000,
                        creator=b"Client|one",
                        taskIds=[b"b" * 32, b"c" * 32],
                        taskCount=57,
                    )
                ],
                totalObjects=9,
            ).to_bytes()
        )
        app._process_objects(state)

        section = app._objects_section(BrowserView(), _RenderCache())
        self.assertEqual(section["objects_total"], 9)
        row = section["objects"][0]
        self.assertEqual(row["name"], "heavy_frame")
        self.assertEqual(row["size"], "1.9M")
        self.assertEqual(row["full_client"], "Client|one")
        self.assertEqual(row["tasks"], 57, "the count is the whole set, not the sample that travels with it")
        self.assertEqual(row["task_ids"], ["626262626262", "636363636363"])

    def test_two_objects_of_one_client_are_told_apart(self) -> None:
        """An object ID starts with its owner's hash, so the head is the same for all of a client's."""
        app = make_app()
        owner = b"o" * 16
        app._process_objects(make_objects(owner + b"1" * 16, owner + b"2" * 16))

        rows = app._objects_section(BrowserView(), _RenderCache())["objects"]
        self.assertNotEqual(rows[0]["object"], rows[1]["object"])
        self.assertEqual(rows[0]["object_id"], (owner + b"1" * 16).hex(), "the tooltip keeps the whole id")

    def test_a_browser_is_sent_one_page_of_objects(self) -> None:
        app = make_app()
        app._process_objects(make_objects(*[bytes([index]) * 32 for index in range(120)]))

        section = app._objects_section(BrowserView(), _RenderCache())
        self.assertEqual(len(section["objects"]), OBJECTS_PAGE_SIZE)
        self.assertEqual(section["objects_held"], 120)
        self.assertEqual(section["objects_pages"], 3)

        second = app._objects_section(BrowserView(objects_page=1), _RenderCache())["objects"]
        self.assertNotEqual(second[0]["object_id"], section["objects"][0]["object_id"])

    def test_objects_sort_by_the_size_behind_the_cell(self) -> None:
        app = make_app()
        app._process_objects(make_objects(b"a" * 32, b"b" * 32, sizes=[9_000, 2_000_000]))

        view = BrowserView(objects_sort="size", objects_sort_ascending=False)
        rows = app._objects_section(view, _RenderCache())["objects"]
        self.assertEqual([row["size"] for row in rows], ["1.9M", "8K"])

    def test_a_generated_name_shows_the_kind_of_object_it_names(self) -> None:
        """A client that names nothing gets a repr of the object id, which the Object column already is."""
        app = make_app()
        generated = f"<obj ObjectID(owner_hash={'a' * 32}, object_tag={'b' * 32})>".encode()
        app._process_objects(make_objects(b"a" * 32, name=generated))

        row = app._objects_section(BrowserView(), _RenderCache())["objects"][0]
        self.assertEqual(row["name"], "<obj>")
        self.assertEqual(row["full_name"], generated.decode(), "the tooltip keeps the whole name")

    def test_a_name_the_client_chose_is_kept(self) -> None:
        app = make_app()
        app._process_objects(make_objects(b"a" * 32, name=b"frame_007"))

        self.assertEqual(app._objects_section(BrowserView(), _RenderCache())["objects"][0]["name"], "frame_007")


if __name__ == "__main__":
    unittest.main()
