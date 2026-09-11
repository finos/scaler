import dataclasses
import logging
from asyncio import Queue
from typing import Optional

from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher
from scaler.protocol.capnp import ObjectInstruction, ObjectManagerStatus, ObjectMetadata
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import ClientController, ObjectController, WorkerController
from scaler.scheduler.object_usage.object_tracker import ObjectTracker, ObjectUsage
from scaler.utility.identifiers import ClientID, ObjectID
from scaler.utility.mixins import Looper, Reporter

logger = logging.getLogger(__name__)


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

        self._client_manager: Optional[ClientController] = None
        self._worker_manager: Optional[WorkerController] = None

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

    def on_del_objects(self, client_id: ClientID, object_ids: set[ObjectID]):
        for object_id in object_ids:
            self._object_tracker.remove_one_block_for_objects({object_id}, client_id)

    def clean_client(self, client_id: ClientID):
        self._object_tracker.remove_blocks({client_id})

    async def routine(self):
        await self.__routine_send_objects_deletions()

    def has_object(self, object_id: ObjectID) -> bool:
        return self._object_tracker.has_object(object_id)

    def get_object_name(self, object_id: ObjectID) -> bytes:
        if not self.has_object(object_id):
            return b"<Unknown>"

        return self._object_tracker.get_object(object_id).object_name

    def get_status(self) -> ObjectManagerStatus:
        return ObjectManagerStatus(numberOfObjects=self._object_tracker.object_count())

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

    def __on_object_create(self, source: bytes, instruction: ObjectInstruction):
        if not self._client_manager.has_client_id(instruction.objectUser):
            logger.error(f"received object creation from {source!r} for unknown client {instruction.objectUser!r}")
            return

        for object_id, object_type, object_name in zip(
            instruction.objectMetadata.objectIds,
            instruction.objectMetadata.objectTypes,
            instruction.objectMetadata.objectNames,
        ):
            self.on_add_object(instruction.objectUser, object_id, object_type, object_name)

    def __finished_object_storage(self, creation: _ObjectCreation):
        logger.debug(f"del object cache object_name={creation.object_name!r}, object_id={creation.object_id!r}")
        self._queue_deleted_object_ids.put_nowait(creation.object_id)
