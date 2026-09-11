"""What the client agent forwards to the scheduler when the client creates objects.

The agent filters out objects it has already sent, and everything the instruction carries has to survive that filter.
The scheduler describes an object from these fields alone.
"""

import unittest
from unittest.mock import AsyncMock

from scaler.client.agent.object_manager import ClientObjectManager
from scaler.protocol.capnp import ObjectInstruction, ObjectMetadata
from scaler.utility.identifiers import ClientID, ObjectID


def make_create(client_id: ClientID, objects) -> ObjectInstruction:
    """A create instruction as the agent receives it, deserialized like a real one."""
    return ObjectInstruction.from_bytes(
        ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.create,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(
                objectIds=[object_id for object_id, _, _ in objects],
                objectTypes=[object_type for _, object_type, _ in objects],
                objectNames=[name for _, _, name in objects],
                objectSizes=[size for size in (len(name) * 100 for _, _, name in objects)],
            ),
        ).to_bytes()
    )


class TestClientObjectManager(unittest.IsolatedAsyncioTestCase):
    async def test_every_field_survives_the_already_sent_filter(self) -> None:
        client_id = ClientID.generate_client_id()
        manager = ClientObjectManager(client_id)
        external = AsyncMock()
        manager.register(connector_internal=AsyncMock(), connector_external=external)

        first = ObjectID.generate_object_id(client_id)
        second = ObjectID.generate_object_id(client_id)
        await manager.on_object_instruction(
            make_create(
                client_id,
                [
                    (first, ObjectMetadata.ObjectContentType.object, b"one"),
                    (second, ObjectMetadata.ObjectContentType.object, b"two-two"),
                ],
            )
        )

        forwarded = ObjectInstruction.from_bytes(external.send.await_args.args[0].to_bytes())
        metadata = forwarded.objectMetadata
        self.assertEqual([bytes(object_id) for object_id in metadata.objectIds], [bytes(first), bytes(second)])
        self.assertEqual(list(metadata.objectNames), [b"one", b"two-two"])
        self.assertEqual(list(metadata.objectSizes), [300, 700])

    async def test_an_object_already_sent_is_dropped_with_its_size(self) -> None:
        client_id = ClientID.generate_client_id()
        manager = ClientObjectManager(client_id)
        external = AsyncMock()
        manager.register(connector_internal=AsyncMock(), connector_external=external)

        first = ObjectID.generate_object_id(client_id)
        second = ObjectID.generate_object_id(client_id)
        await manager.on_object_instruction(
            make_create(client_id, [(first, ObjectMetadata.ObjectContentType.object, b"one")])
        )
        await manager.on_object_instruction(
            make_create(
                client_id,
                [
                    (first, ObjectMetadata.ObjectContentType.object, b"one"),
                    (second, ObjectMetadata.ObjectContentType.object, b"two-two"),
                ],
            )
        )

        forwarded = ObjectInstruction.from_bytes(external.send.await_args.args[0].to_bytes())
        metadata = forwarded.objectMetadata
        self.assertEqual([bytes(object_id) for object_id in metadata.objectIds], [bytes(second)])
        self.assertEqual(list(metadata.objectSizes), [700])


if __name__ == "__main__":
    unittest.main()
