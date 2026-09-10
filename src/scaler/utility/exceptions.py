class TaskNotFoundError(Exception):
    pass


class WorkerDiedError(Exception):
    pass


class NoWorkerError(Exception):
    pass


class DisconnectedError(Exception):
    pass


class ProcessorDiedError(Exception):
    pass


class SchedulerError(Exception):
    """The scheduler failed while it was applying a transition for this task, so the task cannot continue."""

    pass


class SymphonyTaskError(Exception):
    """IBM Spectrum Symphony failed a task, or returned an output the worker manager cannot read.

    The message renders the Symphony failure as text: the client that receives it has no Symphony
    installation, so a ``soamapi`` exception would arrive there as an import error instead."""

    pass


class TaskExceptionNotSerializableError(Exception):
    """A task raised an exception that could not be pickled back to the client; the message preserves the
    original exception's type name and string so the failure is still meaningful."""

    pass


class DeserializeObjectError(Exception):
    pass


class MissingObjects(Exception):
    pass


class ClientCancelledException(Exception):
    pass


class ClientShutdownException(Exception):
    pass


class ClientQuitException(Exception):
    pass


class ObjectStorageException(Exception):
    pass


class CapnpDeserializationError(Exception):
    """Raised when scaler.protocol.capnp fails to decode a wire message,
    including unknown enum ordinals from a newer schema."""

    pass
