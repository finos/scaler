@0xaf44f44ea94a4675;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("scaler::protocol");

using CommonType = import "common.capnp";
using Status = import "status.capnp";

struct Task {
    taskId @0 :Data;
    source @1 :Data;
    metadata @2 :Data;
    funcObjectId @3 :Data;
    functionArgs @4 :List(Argument);
    capabilities @5 :List(CommonType.TaskCapability);

    struct Argument {
        type @0 :ArgumentType;
        data @1 :Data;

        enum ArgumentType {
            task @0;
            objectID @1;
        }
    }
}

struct TaskCancel {
    struct TaskCancelFlags {
        force @0 :Bool;
    }

    taskId @0 :Data;
    flags @1 :TaskCancelFlags;
}

struct TaskLog {
    taskId @0 :Data;
    logType @1 :LogType;
    content @2 :Text;

    enum LogType {
        stdout @0;
        stderr @1;
    }
}

struct TaskResult {
    taskId @0 :Data;
    resultType @1 :CommonType.TaskResultType;
    metadata @2 :Data;
    results @3 :List(Data);
}

struct TaskCancelConfirm {
    taskId @0 :Data;
    cancelConfirmType @1 :CommonType.TaskCancelConfirmType;
}

struct GraphTask {
    taskId @0 :Data;
    source @1 :Data;
    targets @2 :List(Data);
    graph @3 :List(Task);
}

struct ClientHeartbeat {
    resource @0 :Status.Resource;
    latencyUS @1 :UInt32;
    hostname @2 :Text;   # machine the client runs on, so the UI can say where a task came from
}

struct ClientHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
}

struct WorkerHeartbeat {
    agent @0 :Status.Resource;
    rssFree @1 :UInt64;
    queueSize @2 :UInt32;
    queuedTasks @3 :UInt32;
    latencyUS @4 :UInt32;
    taskLock @5 :Bool;
    processors @6 :List(Status.ProcessorStatus);
    capabilities @7 :List(CommonType.TaskCapability);
    workerManagerID @8 :Data;
    memLimit @9 :UInt64;  # memory limit in bytes the worker runs under (cgroup if set, else system total); 0 if unknown
    hostname @10 :Text;   # machine this worker runs on, so the monitor can group workers by host
    netSentBytes @11 :UInt64;  # host-wide counters, identical for workers sharing a host; the monitor
    netRecvBytes @12 :UInt64;  # reads them once per hostname rather than summing them
}

struct WorkerHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
}

struct WorkerManagerHeartbeat {
    maxTaskConcurrency @0 :UInt32;
    capabilities @1 :List(CommonType.TaskCapability);
    workerManagerID @2 :Data;
}

struct WorkerManagerHeartbeatEcho {
}

struct WorkerManagerCommand {
    struct DesiredTaskConcurrencyRequest {
        taskConcurrency @0 :UInt32;
        capabilities @1 :List(CommonType.TaskCapability);
    }

    setDesiredTaskConcurrencyRequests @0 :List(DesiredTaskConcurrencyRequest);
}

struct ObjectInstruction {
    instructionType @0 :ObjectInstructionType;
    objectUser @1 :Data;
    objectMetadata @2 :CommonType.ObjectMetadata;

    enum ObjectInstructionType {
        create @0;
        delete @1;
        clear @2;
    }
}

# Carries no payload: the worker it refers to is the sender, which the binder already identifies.
struct WorkerDisconnectNotification {
}

struct ClientDisconnect {
    disconnectType @0 :DisconnectType;

    enum DisconnectType {
        disconnect @0;
        shutdown @1;
    }
}

struct ClientShutdownResponse {
    accepted @0 :Bool;
}

struct StateClient {
}

struct StateObject {
    objects @0 :List(ObjectDetail);
    totalObjects @1 :UInt32;   # objects the scheduler tracks, of which `objects` is the largest few

    struct ObjectDetail {
        objectId @0 :Data;
        name @1 :Data;
        objectType @2 :CommonType.ObjectMetadata.ObjectContentType;
        size @3 :UInt64;
        creator @4 :Data;        # client that created it
        taskIds @5 :List(Data);  # tasks that name it as their function or an argument, bounded
        taskCount @6 :UInt32;    # how many there are, which `taskIds` is only the first few of
    }
}

struct StateBalanceAdvice {
    workerId @0 :Data;
    taskIds @1 :List(Data);
}

struct StateScheduler {
    binder @0 :Status.BinderStatus;
    scheduler @1 :Status.Resource;
    rssFree @2 :UInt64;
    clientManager @3 :Status.ClientManagerStatus;
    objectManager @4 :Status.ObjectManagerStatus;
    taskManager @5 :Status.TaskManagerStatus;
    workerManager @6 :Status.WorkerManagerStatus;
    scalingManager @7 :Status.ScalingManagerStatus;
}

struct StateWorker {
    workerId @0 :Data;
    state@1 :CommonType.WorkerState;
    capabilities @2 :List(CommonType.TaskCapability);
}

struct StateTask {
    taskId @0 :Data;
    functionName @1 :Data;
    state @2 :CommonType.TaskState;
    worker @3 :Data;
    capabilities @4 :List(CommonType.TaskCapability);
    metadata @5 :Data;
    objectBytes @6 :UInt64;  # payload bytes this task's arguments move, so a heavy task is visible
    client @7 :Data;         # client that submitted the task
}

struct StateGraphTask {
    enum NodeTaskType {
        normal @0;
        target @1;
    }

    graphTaskId @0 :Data;
    taskId @1 :Data;
    nodeTaskType @2 :NodeTaskType;
    parentTaskIds @3 :List(Data);
}

struct ProcessorInitialized {
}

struct InformationRequest {
    request @0 :Data;
}

struct InformationResponse {
    response @0 :Data;
}

struct Message {
    union {
        task @0 :Task;
        taskCancel @1 :TaskCancel;
        taskCancelConfirm @2 :TaskCancelConfirm;
        taskResult @3 :TaskResult;
        taskLog @4 :TaskLog;

        graphTask @5 :GraphTask;

        objectInstruction @6 :ObjectInstruction;

        clientHeartbeat @7 :ClientHeartbeat;
        clientHeartbeatEcho @8 :ClientHeartbeatEcho;

        workerHeartbeat @9 :WorkerHeartbeat;
        workerHeartbeatEcho @10 :WorkerHeartbeatEcho;

        stateClient @11 :StateClient;
        stateObject @12 :StateObject;
        stateBalanceAdvice @13 :StateBalanceAdvice;
        stateScheduler @14 :StateScheduler;
        stateWorker @15 :StateWorker;
        stateTask @16 :StateTask;
        stateGraphTask @17 :StateGraphTask;

        clientDisconnect @18 :ClientDisconnect;
        clientShutdownResponse @19 :ClientShutdownResponse;

        processorInitialized @20 :ProcessorInitialized;

        informationRequest @21 :InformationRequest;
        informationResponse @22 :InformationResponse;

        workerManagerHeartbeat @23 :WorkerManagerHeartbeat;
        workerManagerHeartbeatEcho @24 :WorkerManagerHeartbeatEcho;
        workerManagerCommand @25 :WorkerManagerCommand;

        workerDisconnectNotification @26 :WorkerDisconnectNotification;
    }
}
