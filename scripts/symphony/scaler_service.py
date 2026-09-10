"""IBM Spectrum Symphony service that runs cloudpickle-serialized callables.

This is the service side of the Symphony worker manager. The worker manager sends
``cloudpickle.dumps((function, *arguments))`` as the task payload, so this container deserializes that
tuple, calls the function, and sends back the cloudpickled outcome tagged as a result or an exception.
``scaler.worker_manager.proxy.symphony.response_router`` is the client side of the same contract, and
both halves read the tags from ``task_output.py``, which is deployed alongside this file.

``setup_application.py`` packages and deploys this file. Symphony runs it under the interpreter named by
the ``startCmd`` of the generated application profile, so that interpreter needs ``cloudpickle``,
``tblib`` and a matching ``soamapi``.
"""

import array

try:
    # Symphony ships soamapi as one bytecode directory per interpreter, below the lib64 on PYTHONPATH.
    # Importing soamapiversion appends the directory matching the running interpreter. A PYTHONPATH that
    # names that directory outright does not need it, so its absence is not an error.
    import soamapiversion  # noqa: F401
except ImportError:
    pass

import cloudpickle
import soamapi
import tblib.pickling_support

# task_output.py is scaler/worker_manager/proxy/symphony/task_output.py, which setup_application.py
# packages next to this file. It is imported flat, without the scaler package around it, because
# Symphony puts the deployment directory first on the service's PYTHONPATH and there is no scaler
# installed on a compute host to import it from.
from task_output import TaskOutputTag


class PickledPayloadMessage(soamapi.Message):
    """Carries an opaque cloudpickle payload in both directions."""

    def __init__(self, payload: bytes = b"") -> None:
        self._payload = payload

    def set_payload(self, payload: bytes) -> None:
        self._payload = payload

    def get_payload(self) -> bytes:
        return self._payload

    def on_serialize(self, stream) -> None:
        payload_array = array.array("b", self.get_payload())
        stream.write_byte_array(payload_array, 0, len(payload_array))

    def on_deserialize(self, stream) -> None:
        self.set_payload(stream.read_byte_array("b").tobytes())


def pickle_task_output(function, arguments) -> bytes:
    """Return the output payload for one call, carrying either its result or the exception it raised.

    A raising function is a completed task with a failure to report, not a failed task: the exception is
    pickled here, in the process that raised it, so the client sees the original type, message and
    traceback. Letting it escape to Symphony instead would report the failure as a Symphony task error,
    which loses the exception and makes Symphony retry a call that will fail the same way again.
    """
    try:
        return cloudpickle.dumps((TaskOutputTag.RESULT.value, function(*arguments)))
    except Exception as exception:
        return pickle_exception(exception)


def pickle_exception(exception: BaseException) -> bytes:
    """Return the output payload carrying ``exception``, degrading when it cannot be pickled.

    An exception that holds a lock, a socket or a file handle, or whose class is defined locally, cannot
    be pickled. Sending its type name and message keeps the failure meaningful, where letting the pickling
    error escape would replace it with one that says nothing about what the task did.
    """
    try:
        return cloudpickle.dumps((TaskOutputTag.EXCEPTION.value, exception))
    except Exception:
        try:
            detail = f"{type(exception).__name__}: {exception}"
        except Exception:
            detail = type(exception).__name__
        return cloudpickle.dumps((TaskOutputTag.UNSERIALIZABLE_EXCEPTION.value, detail))


class ScalerServiceContainer(soamapi.ServiceContainer):
    """Calls the function in each task payload and returns its outcome."""

    def on_create_service(self, service_context) -> None:
        # Pickling a traceback needs tblib, and it has to be installed in the process that raises, which
        # is this one. Without it the client gets the exception with its traceback stripped.
        tblib.pickling_support.install()

    def on_session_enter(self, session_context) -> None:
        return

    def on_invoke(self, task_context) -> None:
        input_message = PickledPayloadMessage()
        task_context.populate_task_input(input_message)

        function, *arguments = cloudpickle.loads(input_message.get_payload())

        task_context.set_task_output(PickledPayloadMessage(pickle_task_output(function, arguments)))

    def on_session_leave(self) -> None:
        return

    def on_destroy_service(self) -> None:
        return


if __name__ == "__main__":
    ScalerServiceContainer().run()
