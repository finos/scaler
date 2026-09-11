@0xf57f79ac88fab620;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("scaler::protocol");

enum TaskResultType {
    success @0;           # if submit and task is done and get result
    failed @1;            # if submit and task is failed on worker
    failedWorkerDied @2;  # if submit and worker died (only happened when scheduler keep_task=False)
}

enum TaskCancelConfirmType {
    canceled @0;               # if cancel success
    cancelFailed @1;           # if cancel failed, this might happened if the task is in process
    cancelNotFound @2;         # if cancel cannot find such task
}

enum TaskState {
    inactive @0;
    running @1;
    canceling @2;
    balanceCanceling @3;
    success @4;
    failed @5;
    failedWorkerDied @6;
    canceled @7;
    canceledNotFound @8;
}

enum WorkerState {
    connected @0;
    disconnected @1;
}

struct TaskCapability {
    name @0 :Text;      # the name of the capability provided by the worker/required by the task (e.g. "gpu" or "linux")
    value @1 :Int64;    # the quantity of the capability provided/required. Use -1 for quantity-less capabilities
}

struct ObjectMetadata {
    objectIds @0 :List(Data);
    objectTypes @1 :List(ObjectContentType);
    objectNames @2 :List(Data);
    objectSizes @3 :List(UInt64);  # payload bytes per object, so the monitor can show what a task moves

    enum ObjectContentType {
        serializer @0;
        object @1;
    }
}

struct ObjectStorageAddress {
    host   @0 :Text;
    port   @1 :UInt16;
    scheme @2 :Text;
}
