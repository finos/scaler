@0xa4dfa1212ad2d0f0;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("scaler::protocol");

struct Resource {
    cpu @0 :UInt16;   # 99.2% will be represented as 992 as integer
    rss @1 :UInt64;  # 32bit is capped to 4GB, so use 64bit to represent
}

struct ObjectManagerStatus {
    numberOfObjects @0 :UInt32;      # objects the scheduler is tracking

    # What the object storage server itself reports, from its infoGetTotal request. All zero until the
    # first answer arrives.
    storageObjectCount @1 :UInt64;   # object IDs the server holds
    storageUniqueCount @2 :UInt64;   # distinct payloads behind them
    storageTotalBytes @3 :UInt64;    # bytes those payloads occupy
    storagePendingRequests @4 :UInt64;  # requests waiting for an object that does not exist yet
    storagePendingObjects @5 :UInt64;   # distinct objects those requests wait for
    storageOldestPendingS @6 :UInt64;   # how long the oldest of them has waited
}

struct ClientManagerStatus {
    clients @0 :List(ClientStatus);

    struct ClientStatus {
        clientId @0 :Data;
        numTask @1 :UInt32;
        resource @2 :Resource;   # the client process itself, from its own heartbeat
        latencyUS @3 :UInt32;    # round trip the client last measured to the scheduler
        lastSeenS @4 :UInt16;    # seconds since that heartbeat arrived
        connectedS @5 :UInt32;   # seconds since the client's first heartbeat
        hostname @6 :Text;       # machine the client runs on
    }
}

struct TaskManagerStatus {
    stateToCount @0 :List(Pair);

    struct Pair {
        state @0 :UInt8;
        count @1 :UInt32;
    }
}

struct ProcessorStatus {
    pid @0 :UInt32;
    initialized @1 :Bool;
    hasTask @2 :Bool;
    suspended @3 :Bool;
    resource @4 :Resource;
    currentTaskId @5 :Data;      # task this processor is running, empty when idle
    taskAgeSeconds @6 :UInt32;   # how long it has been on that task, so a stuck one is visible
}

struct WorkerStatus {
    workerId @0 :Data;
    agent @1 :Resource;
    rssFree @2 :UInt64;
    memLimit @11 :UInt64;  # memory limit in bytes the worker runs under (cgroup if set, else system total); 0 if unknown
    free @3 :UInt32;
    sent @4 :UInt32;
    queued @5 :UInt32;
    suspended @6: UInt8;
    lagUS @7 :UInt64;
    lastS @8 :UInt16;
    itl @9 :Text;
    processorStatuses @10 :List(ProcessorStatus);
    hostname @12 :Text;          # machine this worker runs on, so the UI can group by host
    netSentBytes @13 :UInt64;    # host-wide network counters; identical for workers sharing a host,
    netRecvBytes @14 :UInt64;    # so the UI reads them once per hostname
}

struct WorkerManagerStatus {
    workers @0 :List(WorkerStatus);
}

struct ScalingManagerStatus {
    managedWorkers @0 :List(Pair);
    workerManagerDetails @1 :List(WorkerManagerDetail);

    struct Pair {
        workerManagerID @0 :Data;
        workerIDs @1 :List(Data);
    }

    struct WorkerManagerDetail {
        workerManagerID @0 :Data;
        identity @1 :Text;
        lastSeenS @2 :UInt16;
        maxTaskConcurrency @3 :UInt32;
        capabilities @4 :Text;
        # Workers the scheduler has requested but that have not yet connected.
        # Computed each tick as max(0, total_requested - connected_count).
        pendingWorkers @5 :UInt32;
    }
}

struct BinderStatus {
    received @0 :List(Pair);
    sent @1 :List(Pair);

    struct Pair {
        client @0 :Text;
        number @1 :UInt32;
    }
}
