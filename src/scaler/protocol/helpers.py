import struct

import bidict

from scaler.protocol import capnp
from scaler.protocol.capnp import ObjectID as CapnpObjectID
from scaler.utility.identifiers import ObjectID as ScalerObjectID

OBJECT_ID_FORMAT = "!QQQQ"


def to_capnp_object_id(object_id: ScalerObjectID):
    field0, field1, field2, field3 = struct.unpack(OBJECT_ID_FORMAT, object_id)
    return capnp.ObjectID(field0=field0, field1=field1, field2=field2, field3=field3)


def from_capnp_object_id(capnp_object_id: CapnpObjectID) -> ScalerObjectID:
    return ScalerObjectID(
        struct.pack(
            OBJECT_ID_FORMAT,
            capnp_object_id.field0,
            capnp_object_id.field1,
            capnp_object_id.field2,
            capnp_object_id.field3,
        )
    )


def capabilities_to_dict(capabilities) -> dict[str, int]:
    if isinstance(capabilities, dict):
        return dict(capabilities)

    return {capability.name: capability.value for capability in capabilities}


def dict_to_capabilities(capabilities: dict[str, int] | list[capnp.TaskCapability]) -> list[capnp.TaskCapability]:
    """Convert capabilities into a list of freshly-built capnp ``TaskCapability`` structs.

    The capnp Python extension does not natively populate a ``list(TaskCapability)`` field from a
    Python ``dict`` (only the dict's keys are iterated, leaving every struct's ``value`` at its
    default), and assigning an existing capnp list reader to a builder field has the same effect:
    the destination list is sized to the source but each entry retains its default values. This
    helper rebuilds every entry as a new struct so both ``name`` and ``value`` reach the wire.
    """
    if isinstance(capabilities, dict):
        return [capnp.TaskCapability(name=name, value=value) for name, value in capabilities.items()]
    return [capnp.TaskCapability(name=c.name, value=c.value) for c in capabilities]


PROTOCOL: bidict.bidict[str, type] = bidict.bidict(
    {
        "task": capnp.Task,
        "taskCancel": capnp.TaskCancel,
        "taskCancelConfirm": capnp.TaskCancelConfirm,
        "taskResult": capnp.TaskResult,
        "taskLog": capnp.TaskLog,
        "graphTask": capnp.GraphTask,
        "objectInstruction": capnp.ObjectInstruction,
        "clientHeartbeat": capnp.ClientHeartbeat,
        "clientHeartbeatEcho": capnp.ClientHeartbeatEcho,
        "workerHeartbeat": capnp.WorkerHeartbeat,
        "workerHeartbeatEcho": capnp.WorkerHeartbeatEcho,
        "workerManagerHeartbeat": capnp.WorkerManagerHeartbeat,
        "workerManagerHeartbeatEcho": capnp.WorkerManagerHeartbeatEcho,
        "workerManagerCommand": capnp.WorkerManagerCommand,
        "stateClient": capnp.StateClient,
        "stateObject": capnp.StateObject,
        "stateBalanceAdvice": capnp.StateBalanceAdvice,
        "stateScheduler": capnp.StateScheduler,
        "stateWorker": capnp.StateWorker,
        "stateTask": capnp.StateTask,
        "stateGraphTask": capnp.StateGraphTask,
        "clientDisconnect": capnp.ClientDisconnect,
        "clientShutdownResponse": capnp.ClientShutdownResponse,
        "processorInitialized": capnp.ProcessorInitialized,
        "informationRequest": capnp.InformationRequest,
        "informationResponse": capnp.InformationResponse,
        "workerDisconnectNotification": capnp.WorkerDisconnectNotification,
    }
)
