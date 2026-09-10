import dataclasses
import struct

from scaler.protocol.capnp import Task


@dataclasses.dataclass
class TaskFlags:
    profiling: bool = dataclasses.field(default=True)
    priority: int = dataclasses.field(default=0)
    stream_output: bool = dataclasses.field(default=False)

    FORMAT = "!?i?"

    def serialize(self) -> bytes:
        return struct.pack(TaskFlags.FORMAT, self.profiling, self.priority, self.stream_output)

    @staticmethod
    def deserialize(data: bytes) -> "TaskFlags":
        return TaskFlags(*struct.unpack(TaskFlags.FORMAT, data))


def retrieve_task_flags_from_task(task: Task) -> TaskFlags:
    if task.metadata == b"":
        return TaskFlags()

    try:
        return TaskFlags.deserialize(task.metadata)
    except struct.error:
        raise ValueError(f"unexpected metadata value (expected {TaskFlags.__name__}).")


def is_nested_task(task: Task) -> bool:
    """Returns whether the task was submitted by a client running inside another task.

    A `Client` stamps a submission with its parent task's priority plus one when it runs inside a processor,
    and with zero otherwise. A non-zero priority therefore means some task that already holds a worker's
    queue slot is blocked until this one completes.
    """

    return retrieve_task_flags_from_task(task).priority > 0
