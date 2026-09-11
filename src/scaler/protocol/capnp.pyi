from enum import IntEnum
from typing import Any, ClassVar

from scaler.utility.identifiers import ClientID
from scaler.utility.identifiers import ObjectID as ScalerObjectID
from scaler.utility.identifiers import TaskID, WorkerID

class CapnpStruct:
    def __init__(self, **kwargs: Any) -> None: ...
    def to_bytes(self) -> bytes: ...
    def get_message(self) -> "CapnpStruct": ...
    @classmethod
    def from_bytes(cls, data: bytes | bytearray, traversal_limit_in_words: int = ...) -> Any:
        """Deserialize zero-copy: fields are read lazily from *data*'s buffer.
        If *data* is a bytearray, mutations to it after this call are visible
        on first field access.  Pass bytes for an immutable view."""
        ...

class BaseMessage(CapnpStruct): ...

class CapnpUnionStruct(CapnpStruct):
    def which(self) -> str: ...

class TaskResultType(IntEnum):
    success = 0
    failed = 1
    failedWorkerDied = 2

class TaskCancelConfirmType(IntEnum):
    canceled = 0
    cancelFailed = 1
    cancelNotFound = 2

class TaskState(IntEnum):
    inactive = 0
    running = 1
    canceling = 2
    balanceCanceling = 3
    success = 4
    failed = 5
    failedWorkerDied = 6
    canceled = 7
    canceledNotFound = 8

class WorkerState(IntEnum):
    connected = 0
    disconnected = 1

class TaskCapability(CapnpStruct):
    name: str
    value: int
    @staticmethod
    def new_msg(name: str, value: int) -> "TaskCapability": ...

class ObjectMetadata(CapnpStruct):
    class ObjectContentType(IntEnum):
        serializer = 0
        object = 1

    objectIds: Any
    objectTypes: Any
    objectNames: Any
    objectSizes: Any

    @staticmethod
    def new_msg(object_ids: Any, object_types: Any = ..., object_names: Any = ...) -> "ObjectMetadata": ...

class ObjectStorageAddress(CapnpStruct):
    host: str
    port: int
    scheme: str
    @staticmethod
    def new_msg(host: str, port: int, scheme: str) -> "ObjectStorageAddress": ...

class Resource(CapnpStruct):
    cpu: int
    rss: int

class ObjectManagerStatus(CapnpStruct):
    numberOfObjects: int
    storageObjectCount: int
    storageUniqueCount: int
    storageTotalBytes: int
    storagePendingRequests: int
    storagePendingObjects: int
    storageOldestPendingS: int

class ClientManagerStatus(CapnpStruct):
    class ClientStatus(CapnpStruct):
        clientId: ClientID
        numTask: int
        resource: Resource
        latencyUS: int
        lastSeenS: int
        connectedS: int
        hostname: str

    clients: Any

class TaskManagerStatus(CapnpStruct):
    class Pair(CapnpStruct):
        state: int
        count: int

    stateToCount: Any

class ProcessorStatus(CapnpStruct):
    pid: int
    initialized: bool
    hasTask: bool
    suspended: bool
    resource: Resource
    currentTaskId: bytes
    taskAgeSeconds: int

class WorkerStatus(CapnpStruct):
    workerId: WorkerID
    agent: Resource
    rssFree: int
    memLimit: int
    free: int
    sent: int
    queued: int
    suspended: int
    lagUS: int
    lastS: int
    itl: str
    processorStatuses: Any
    hostname: str
    netSentBytes: int
    netRecvBytes: int

class WorkerManagerStatus(CapnpStruct):
    workers: Any

class ScalingManagerStatus(CapnpStruct):
    class Pair(CapnpStruct):
        workerManagerID: bytes
        workerIDs: Any

    class WorkerManagerDetail(CapnpStruct):
        workerManagerID: bytes
        identity: str
        lastSeenS: int
        maxTaskConcurrency: int
        capabilities: str
        pendingWorkers: int

    managedWorkers: Any
    workerManagerDetails: Any

class BinderStatus(CapnpStruct):
    class Pair(CapnpStruct):
        client: str
        number: int

    received: Any
    sent: Any

class Task(BaseMessage):
    taskId: TaskID
    source: ClientID
    metadata: bytes
    funcObjectId: ScalerObjectID
    functionArgs: Any
    capabilities: Any

    class Argument(CapnpStruct):
        type: Any
        data: bytes

        class ArgumentType(IntEnum):
            task = 0
            objectID = 1

    @staticmethod
    def new_msg(*args: Any, **kwargs: Any) -> "Task": ...

class TaskCancel(BaseMessage):
    taskId: TaskID
    flags: Any

    class TaskCancelFlags(CapnpStruct):
        force: bool

    @staticmethod
    def new_msg(*args: Any, **kwargs: Any) -> "TaskCancel": ...

class TaskLog(BaseMessage):
    taskId: TaskID
    logType: Any
    content: str

    class LogType(IntEnum):
        stdout = 0
        stderr = 1

    @staticmethod
    def new_msg(*args: Any, **kwargs: Any) -> "TaskLog": ...

class TaskResult(BaseMessage):
    taskId: TaskID
    resultType: TaskResultType
    metadata: bytes
    results: Any

class TaskCancelConfirm(BaseMessage):
    taskId: TaskID
    cancelConfirmType: TaskCancelConfirmType

class GraphTask(BaseMessage):
    taskId: TaskID
    source: ClientID
    targets: Any
    graph: Any

class ClientHeartbeat(BaseMessage):
    resource: Resource
    latencyUS: int
    hostname: str

class ClientHeartbeatEcho(BaseMessage):
    objectStorageAddress: ObjectStorageAddress

class WorkerHeartbeat(BaseMessage):
    agent: Resource
    rssFree: int
    queueSize: int
    queuedTasks: int
    latencyUS: int
    taskLock: bool
    processors: Any
    capabilities: Any
    workerManagerID: bytes
    memLimit: int
    hostname: str
    netSentBytes: int
    netRecvBytes: int

class WorkerHeartbeatEcho(BaseMessage):
    objectStorageAddress: ObjectStorageAddress

class WorkerManagerHeartbeat(BaseMessage):
    maxTaskConcurrency: int
    capabilities: Any
    workerManagerID: bytes

class WorkerManagerHeartbeatEcho(BaseMessage): ...

class WorkerManagerCommand(BaseMessage):
    setDesiredTaskConcurrencyRequests: Any

    class DesiredTaskConcurrencyRequest(CapnpStruct):
        taskConcurrency: int
        capabilities: Any

class ObjectInstruction(BaseMessage):
    instructionType: "ObjectInstruction.ObjectInstructionType"
    objectUser: ClientID
    objectMetadata: ObjectMetadata

    class ObjectInstructionType(IntEnum):
        create = 0
        delete = 1
        clear = 2

class WorkerDisconnectNotification(BaseMessage): ...

class ClientDisconnect(BaseMessage):
    disconnectType: "ClientDisconnect.DisconnectType"

    class DisconnectType(IntEnum):
        disconnect = 0
        shutdown = 1

class ClientShutdownResponse(BaseMessage):
    accepted: bool

class StateClient(BaseMessage): ...

class StateObject(BaseMessage):
    class ObjectDetail(CapnpStruct):
        objectId: ObjectID
        name: bytes
        objectType: ObjectMetadata.ObjectContentType
        size: int
        creator: ClientID
        taskIds: Any
        taskCount: int

    objects: Any
    totalObjects: int

class StateBalanceAdvice(BaseMessage):
    workerId: WorkerID
    taskIds: Any

class StateScheduler(BaseMessage):
    binder: BinderStatus
    scheduler: Resource
    rssFree: int
    clientManager: ClientManagerStatus
    objectManager: ObjectManagerStatus
    taskManager: TaskManagerStatus
    workerManager: WorkerManagerStatus
    scalingManager: ScalingManagerStatus

class StateWorker(BaseMessage):
    workerId: WorkerID
    state: WorkerState
    capabilities: Any

class StateTask(BaseMessage):
    taskId: TaskID
    functionName: bytes
    state: TaskState
    worker: WorkerID
    capabilities: Any
    metadata: bytes
    objectBytes: int
    client: ClientID

class StateGraphTask(BaseMessage):
    graphTaskId: TaskID
    taskId: TaskID
    nodeTaskType: "StateGraphTask.NodeTaskType"
    parentTaskIds: Any

    class NodeTaskType(IntEnum):
        normal = 0
        target = 1

class ProcessorInitialized(BaseMessage): ...

class InformationRequest(BaseMessage):
    request: bytes

class InformationResponse(BaseMessage):
    response: bytes

class Message(CapnpUnionStruct):
    task: Task
    taskCancel: TaskCancel
    taskCancelConfirm: TaskCancelConfirm
    taskResult: TaskResult
    taskLog: TaskLog
    graphTask: GraphTask
    objectInstruction: ObjectInstruction
    clientHeartbeat: ClientHeartbeat
    clientHeartbeatEcho: ClientHeartbeatEcho
    workerHeartbeat: WorkerHeartbeat
    workerHeartbeatEcho: WorkerHeartbeatEcho
    stateClient: StateClient
    stateObject: StateObject
    stateBalanceAdvice: StateBalanceAdvice
    stateScheduler: StateScheduler
    stateWorker: StateWorker
    stateTask: StateTask
    stateGraphTask: StateGraphTask
    clientDisconnect: ClientDisconnect
    clientShutdownResponse: ClientShutdownResponse
    processorInitialized: ProcessorInitialized
    informationRequest: InformationRequest
    informationResponse: InformationResponse
    workerManagerHeartbeat: WorkerManagerHeartbeat
    workerManagerHeartbeatEcho: WorkerManagerHeartbeatEcho
    workerManagerCommand: WorkerManagerCommand
    workerDisconnectNotification: WorkerDisconnectNotification

class ObjectRequestHeader(CapnpStruct):
    MESSAGE_LENGTH: ClassVar[int]
    objectID: Any
    payloadLength: int
    requestID: int
    requestType: "ObjectRequestHeader.ObjectRequestType"

    class ObjectRequestType(IntEnum):
        setObject = 0
        getObject = 1
        deleteObject = 2
        duplicateObjectID = 3
        infoGetTotal = 4

class ObjectID(CapnpStruct):
    field0: int
    field1: int
    field2: int
    field3: int

class ObjectResponseHeader(CapnpStruct):
    MESSAGE_LENGTH: ClassVar[int]
    objectID: Any
    payloadLength: int
    responseID: int
    responseType: "ObjectResponseHeader.ObjectResponseType"

    class ObjectResponseType(IntEnum):
        setOK = 0
        getOK = 1
        delOK = 2
        delNotExists = 3
        duplicateOK = 4
        infoGetTotalOK = 5

def get_module_descriptor(module_name: str) -> Any: ...
def message_to_bytes(variant_name: str, inner: Any) -> bytes: ...
def message_from_bytes(data: bytes, traversal_limit: int = ...) -> Any: ...
def struct_to_bytes(type_name: str, obj: Any) -> bytes: ...
def struct_from_bytes(type_name: str, data: bytes, traversal_limit: int = ...) -> Any: ...

PROTOCOL: Any
