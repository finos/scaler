from typing import Optional

from scaler.client.agent.mixins import ObjectManager
from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import ObjectInstruction, ObjectMetadata, TaskResult
from scaler.utility.identifiers import ClientID, ObjectID


class ClientObjectManager(ObjectManager):
    def __init__(self, identity: ClientID):
        self._sent_object_ids: set[ObjectID] = set()
        self._sent_serializer_id: Optional[ObjectID] = None

        self._identity = identity

        self._connector_internal: Optional[AsyncConnector] = None
        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_internal: AsyncConnector, connector_external: AsyncConnector):
        self._connector_internal = connector_internal
        self._connector_external = connector_external

    async def on_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.create:
            await self.__send_object_creation(instruction)
        elif instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete:
            await self.__delete_objects(instruction)
        elif instruction.instructionType == ObjectInstruction.ObjectInstructionType.clear:
            await self.clear_all_objects(clear_serializer=False)

    def on_task_result(self, task_result: TaskResult):
        self._sent_object_ids.update((ObjectID(object_id_bytes) for object_id_bytes in task_result.results))

    async def clear_all_objects(self, clear_serializer):
        cleared_object_ids = self._sent_object_ids.copy()

        if clear_serializer:
            self._sent_serializer_id = None
        elif self._sent_serializer_id is not None:
            cleared_object_ids.remove(self._sent_serializer_id)

        self._sent_object_ids.difference_update(cleared_object_ids)

        await self._connector_external.send(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.delete,
                objectUser=self._identity,
                objectMetadata=ObjectMetadata(objectIds=tuple(cleared_object_ids)),
            ),
            detached=True,
        )

    async def __send_object_creation(self, instruction: ObjectInstruction):
        assert instruction.instructionType == ObjectInstruction.ObjectInstructionType.create

        new_object_ids = {
            ObjectID(object_id) for object_id in instruction.objectMetadata.objectIds
        } - self._sent_object_ids
        if not new_object_ids:
            return

        if ObjectMetadata.ObjectContentType.serializer in instruction.objectMetadata.objectTypes:
            if self._sent_serializer_id is not None:
                raise ValueError("trying to send multiple serializers.")

            serializer_index = instruction.objectMetadata.objectTypes.index(ObjectMetadata.ObjectContentType.serializer)
            self._sent_serializer_id = ObjectID(instruction.objectMetadata.objectIds[serializer_index])

        new_object_content = ObjectMetadata(
            objectIds=[
                object_id for object_id in instruction.objectMetadata.objectIds if ObjectID(object_id) in new_object_ids
            ],
            objectTypes=[
                object_type
                for object_id, object_type in zip(
                    instruction.objectMetadata.objectIds, instruction.objectMetadata.objectTypes
                )
                if ObjectID(object_id) in new_object_ids
            ],
            objectNames=[
                object_name
                for object_id, object_name in zip(
                    instruction.objectMetadata.objectIds, instruction.objectMetadata.objectNames
                )
                if ObjectID(object_id) in new_object_ids
            ],
        )

        self._sent_object_ids.update(ObjectID(object_id) for object_id in new_object_content.objectIds)

        await self._connector_external.send(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.create,
                objectUser=instruction.objectUser,
                objectMetadata=new_object_content,
            ),
            detached=True,
        )

    async def __delete_objects(self, instruction: ObjectInstruction):
        assert instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete

        if self._sent_serializer_id in {ObjectID(object_id) for object_id in instruction.objectMetadata.objectIds}:
            raise ValueError("trying to delete serializer.")

        self._sent_object_ids.difference_update(
            ObjectID(object_id) for object_id in instruction.objectMetadata.objectIds
        )

        await self._connector_external.send(instruction, detached=True)
