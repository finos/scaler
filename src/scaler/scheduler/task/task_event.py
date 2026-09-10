import dataclasses
from typing import Union

from scaler.protocol.capnp import TaskCancel, TaskCancelConfirm, TaskResult
from scaler.utility.identifiers import ClientID, TaskID, WorkerID


@dataclasses.dataclass(frozen=True)
class HasCapacity:
    """a worker has been acquired for a task that is waiting to be dispatched"""

    task_id: TaskID
    worker_id: WorkerID


@dataclasses.dataclass(frozen=True)
class TaskCancelRequested:
    """a client asked the scheduler to cancel one of its tasks"""

    task_id: TaskID
    client_id: ClientID
    task_cancel: TaskCancel


@dataclasses.dataclass(frozen=True)
class BalanceCancelRequested:
    """the balance controller asked the scheduler to take a task back from its worker"""

    task_id: TaskID


@dataclasses.dataclass(frozen=True)
class TaskResultReceived:
    """a worker returned a result for a task, successful or not"""

    task_id: TaskID
    worker_id: WorkerID
    task_result: TaskResult


@dataclasses.dataclass(frozen=True)
class CancelConfirmCanceled:
    """a worker confirmed that it canceled the task"""

    task_id: TaskID
    worker_id: WorkerID
    task_cancel_confirm: TaskCancelConfirm


@dataclasses.dataclass(frozen=True)
class CancelConfirmFailed:
    """a worker refused to cancel the task because it is already running"""

    task_id: TaskID
    worker_id: WorkerID
    task_cancel_confirm: TaskCancelConfirm


@dataclasses.dataclass(frozen=True)
class CancelConfirmNotFound:
    """a worker answered that it does not hold the task that the scheduler asked it to cancel"""

    task_id: TaskID
    worker_id: WorkerID
    task_cancel_confirm: TaskCancelConfirm


@dataclasses.dataclass(frozen=True)
class WorkerDisconnected:
    """the worker that holds the task disconnected, gracefully or through a heartbeat timeout"""

    task_id: TaskID
    worker_id: WorkerID


# The events a worker reports about a task it believes it holds. Their ``worker_id`` is the identity the binder
# read from the connection, not a field of the payload, so a worker cannot claim to be another one.
WorkerReportedTaskEvent = Union[TaskResultReceived, CancelConfirmCanceled, CancelConfirmFailed, CancelConfirmNotFound]

# the members of WorkerReportedTaskEvent, for isinstance
WORKER_REPORTED_TASK_EVENTS = (TaskResultReceived, CancelConfirmCanceled, CancelConfirmFailed, CancelConfirmNotFound)

TaskEvent = Union[HasCapacity, TaskCancelRequested, BalanceCancelRequested, WorkerReportedTaskEvent, WorkerDisconnected]
