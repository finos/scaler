import asyncio
import dataclasses
import heapq
import logging
from asyncio import Queue
from typing import Dict, List, Optional, Set

from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher, ObjectStorageTotals
from scaler.protocol.capnp import ObjectInstruction, ObjectManagerStatus, ObjectMetadata
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import ClientController, ObjectController, ObjectDetail, WorkerController
from scaler.scheduler.object_usage.object_tracker import ObjectTracker, ObjectUsage
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID
from scaler.utility.mixins import Looper, Reporter

logger = logging.getLogger(__name__)

# How long to wait for the storage server to answer what it holds; unbounded would wedge this routine.
STORAGE_TOTALS_TIMEOUT_SECONDS = 5.0


@dataclasses.dataclass
class _ObjectCreation(ObjectUsage):
    object_id: ObjectID
    object_creator: ClientID
    object_type: ObjectMetadata.ObjectContentType
    object_name: bytes

    def get_object_key(self) -> ObjectID:
        return self.object_id


class VanillaObjectController(ObjectController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller

        self._object_tracker: ObjectTracker[ClientID, ObjectID, _ObjectCreation] = ObjectTracker(
            "object_usage", self.__finished_object_storage
        )

        self._queue_deleted_object_ids: Queue[ObjectID] = Queue()

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        # keyed by raw id bytes: ids arrive off the wire, and an ObjectID would validate a length nobody needs
        self._object_sizes: Dict[bytes, int] = {}

        self._client_manager: Optional[ClientController] = None
        self._worker_manager: Optional[WorkerController] = None

        # The storage server's own view, refreshed by its routine: a status frame reports the last answer.
        self._storage_totals = ObjectStorageTotals()

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncPublisher,
        connector_storage: AsyncObjectStorageConnector,
        client_manager: ClientController,
        worker_manager: WorkerController,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._connector_storage = connector_storage
        self._client_manager = client_manager
        self._worker_manager = worker_manager

    async def on_object_instruction(self, source: bytes, instruction: ObjectInstruction):
        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.create:
            self.__on_object_create(source, instruction)
            return

        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete:
            self.on_del_objects(instruction.objectUser, set(instruction.objectMetadata.objectIds))
            return

        logger.error(f"received unknown object instruction_type={instruction.instructionType} from {source=}")

    def on_add_object(
        self,
        client_id: ClientID,
        object_id: ObjectID,
        object_type: ObjectMetadata.ObjectContentType,
        object_name: bytes,
    ):
        creation = _ObjectCreation(object_id, client_id, object_type, object_name)
        logger.debug(
            f"add object cache "
            f"object_name={creation.object_name!r}, "
            f"object_type={creation.object_type}, "
            f"object_id={creation.object_id!r}"
        )

        self._object_tracker.add_object(creation)
        self._object_tracker.add_blocks_for_one_object(creation.get_object_key(), {creation.object_creator})

    def on_del_objects(self, client_id: ClientID, object_ids: Set[ObjectID]):
        for object_id in object_ids:
            self._object_tracker.remove_one_block_for_objects({object_id}, client_id)

    def clean_client(self, client_id: ClientID):
        self._object_tracker.remove_blocks({client_id})

    async def routine(self):
        await self.__routine_send_objects_deletions()

    async def routine_storage_totals(self) -> None:
        """Ask the storage server what it holds. Its own loop is the only place those numbers exist."""
        try:
            self._storage_totals = await asyncio.wait_for(
                self._connector_storage.info_get_total(), timeout=STORAGE_TOTALS_TIMEOUT_SECONDS
            )
        except (ObjectStorageException, asyncio.TimeoutError):
            # A storage server that is gone or slow must not stop the scheduler reporting everything else.
            self._storage_totals = ObjectStorageTotals()

    def get_object_size(self, object_id: bytes) -> int:
        """Payload bytes for an object, 0 if it was created by a client that does not report sizes."""
        return self._object_sizes.get(bytes(object_id), 0)

    def object_count(self) -> int:
        return self._object_tracker.object_count()

    def get_largest_objects(self, limit: int) -> List[ObjectDetail]:
        """The `limit` biggest tracked objects, biggest first, which is what a full store is made of.

        A heap, because this runs on the scheduler's event loop.
        Sorting a few hundred thousand objects every status frame costs seconds.
        """
        largest = heapq.nlargest(limit, self._object_tracker.items(), key=lambda item: self.get_object_size(item[0]))
        return [
            ObjectDetail(
                object_id=object_id,
                name=creation.object_name,
                content_type=creation.object_type,
                size=self.get_object_size(object_id),
                creator=creation.object_creator,
            )
            for object_id, creation in largest
        ]

    def has_object(self, object_id: ObjectID) -> bool:
        return self._object_tracker.has_object(object_id)

    def get_object_name(self, object_id: ObjectID) -> bytes:
        if not self.has_object(object_id):
            return b"<Unknown>"

        return self._object_tracker.get_object(object_id).object_name

    def get_status(self) -> ObjectManagerStatus:
        return ObjectManagerStatus(
            numberOfObjects=self._object_tracker.object_count(),
            storageObjectCount=self._storage_totals.object_count,
            storageUniqueCount=self._storage_totals.unique_object_count,
            storageTotalBytes=self._storage_totals.total_bytes,
            storagePendingRequests=self._storage_totals.pending_request_count,
            storagePendingObjects=self._storage_totals.pending_object_count,
            storageOldestPendingS=self._storage_totals.oldest_pending_seconds,
        )

    async def __routine_send_objects_deletions(self):
        deleted_object_ids = [await self._queue_deleted_object_ids.get()]
        self._queue_deleted_object_ids.task_done()

        while not self._queue_deleted_object_ids.empty():
            deleted_object_ids.append(self._queue_deleted_object_ids.get_nowait())
            self._queue_deleted_object_ids.task_done()

        for worker in self._worker_manager.get_worker_ids():
            await self._binder.send(
                worker,
                ObjectInstruction(
                    instructionType=ObjectInstruction.ObjectInstructionType.delete,
                    # TODO: ideally object_user should be set to the owning client ID, but then we cannot batch these
                    # Delete instructions.
                    objectUser=b"",
                    objectMetadata=ObjectMetadata(objectIds=tuple(deleted_object_ids)),
                ),
                detached=True,
            )

        for object_id in deleted_object_ids:
            await self._connector_storage.delete_object(object_id)
            self.__forget_object_size(object_id)

    def __on_object_create(self, source: bytes, instruction: ObjectInstruction):
        if not self._client_manager.has_client_id(instruction.objectUser):
            logger.error(f"received object creation from {source!r} for unknown client {instruction.objectUser!r}")
            return

        # objectSizes is newer than the other three, so an older client omits it: pad rather than zip short
        sizes = list(instruction.objectMetadata.objectSizes)
        object_ids = list(instruction.objectMetadata.objectIds)
        sizes += [0] * (len(object_ids) - len(sizes))

        for object_id, object_type, object_name, object_size in zip(
            object_ids, instruction.objectMetadata.objectTypes, instruction.objectMetadata.objectNames, sizes
        ):
            self._object_sizes[bytes(object_id)] = object_size
            self.on_add_object(instruction.objectUser, object_id, object_type, object_name)

    def __forget_object_size(self, object_id: ObjectID) -> None:
        self._object_sizes.pop(bytes(object_id), None)

    def __finished_object_storage(self, creation: _ObjectCreation):
        logger.debug(f"del object cache object_name={creation.object_name!r}, object_id={creation.object_id!r}")
        self._queue_deleted_object_ids.put_nowait(creation.object_id)
