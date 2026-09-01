import contextlib
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from contextvars import ContextVar, Token
from multiprocessing.synchronize import Event as EventType
from typing import IO, Callable, Optional, TypeVar, cast

import tblib.pickling_support

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import ConnectorRemoteType, NetworkBackend, SyncConnector, SyncObjectStorageConnector
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    ObjectInstruction,
    ObjectMetadata,
    ProcessorInitialized,
    Task,
    TaskLog,
    TaskResult,
    TaskResultType,
)
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import LogType, detect_log_type
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.process_bootstrap import bootstrap_process
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.processor.object_cache import ObjectCache
from scaler.worker.agent.processor.streaming_buffer import StreamingBuffer
from scaler.worker.preload import execute_preload

logger = logging.getLogger(__name__)

SUSPEND_SIGNAL = "SIGUSR1"  # use str instead of a signal.Signal to not trigger an import error on unsupported systems.

# Attempts at handing a finished task's result off, and the wait before the second one, doubling from
# there. The connectors reconnect on their own, so the delays only have to outlast a reconnect.
RESULT_HAND_OFF_MAX_ATTEMPTS = 4
RESULT_HAND_OFF_RETRY_DELAY_SECONDS = 1.0

_current_processor: ContextVar[Optional["Processor"]] = ContextVar("_current_processor", default=None)

_T = TypeVar("_T")


class Processor(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        agent_address: AddressConfig,
        scheduler_address: AddressConfig,
        object_storage_address: AddressConfig,
        preload: Optional[str],
        resume_event: Optional[EventType],
        resumed_event: Optional[EventType],
        suspend_trigger: Optional[EventType],
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        logging_paths: tuple[str, ...],
        logging_level: str,
        security_config: Optional[SecurityConfig] = None,
    ):
        super().__init__(name="Processor")

        self._event_loop = event_loop
        self._agent_address = agent_address
        self._scheduler_address = scheduler_address
        self._object_storage_address = object_storage_address
        self._preload = preload
        self._security_config = security_config

        self._identity = generate_identity_from_name(f"processor|{self.pid}")
        self._backend: Optional[NetworkBackend] = None

        self._resume_event = resume_event
        self._resumed_event = resumed_event
        self._suspend_trigger = suspend_trigger

        # _listener_shutdown is created in __initialize (the child process) because threading.Event
        # cannot be pickled across the spawn boundary on Windows.
        self._suspend_listener: Optional[threading.Thread] = None
        self._listener_shutdown: Optional[threading.Event] = None

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._logging_paths = logging_paths
        self._logging_level = logging_level

        self._object_cache: Optional[ObjectCache] = None

        self._current_task: Optional[Task] = None

        # set by __interrupt so __log_exit can tell an agent-requested teardown apart from a real fault
        self._interrupted = False

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    @staticmethod
    def get_current_processor() -> Optional["Processor"]:
        """Returns the current Processor instance controlling the current process, if any."""
        return _current_processor.get()

    def scheduler_address(self) -> AddressConfig:
        """Returns the scheduler address this processor's worker is connected to."""
        return self._scheduler_address

    def current_task(self) -> Optional[Task]:
        return self._current_task

    def __initialize(self):
        self._listener_shutdown = threading.Event()

        # modify the logging path and add process id to the path, leaving screen paths (e.g. "/dev/stdout") untouched
        logging_paths = [
            path if detect_log_type(path) in {LogType.Stdout, LogType.Stderr} else f"{path}-{os.getpid()}"
            for path in self._logging_paths
        ]

        bootstrap_process(log_paths=tuple(logging_paths), logging_level=self._logging_level)
        tblib.pickling_support.install()

        self._backend = get_network_backend_from_env()
        assert self._backend is not None

        self._connector_agent: SyncConnector = self._backend.create_sync_connector(
            identity=self._identity, connector_remote_type=ConnectorRemoteType.Binder, address=self._agent_address
        )

        logger.info(f"Processor[{self.pid}] connecting to object storage at {self._object_storage_address}...")
        self._connector_storage: SyncObjectStorageConnector = self._backend.create_sync_object_storage_connector(
            identity=self._identity, address=self._object_storage_address, security_config=self._security_config
        )

        self._object_cache = ObjectCache(
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
        )
        self._object_cache.start()

        self.__register_signals()

        # Execute optional preload hook if provided
        if self._preload is not None:
            try:
                execute_preload(self._preload)
            except Exception as e:
                raise RuntimeError(
                    f"Processor[{self.pid}] initialization failed due to preload error: {self._preload}"
                ) from e

    def __register_signals(self):
        if sys.platform != "win32":
            # Windows signal.signal() accepts SIGTERM but the OS does not deliver it from external commands,
            # so the handler would never run. Skip registration to keep the Windows code path honest.
            self.__register_signal("SIGTERM", self.__interrupt)

        if self._resume_event is None:
            return

        if sys.platform == "win32":
            assert self._suspend_trigger is not None
            self._suspend_listener = threading.Thread(
                target=self.__suspend_listener_main, name="ProcessorSuspendListener", daemon=True
            )
            self._suspend_listener.start()
        else:
            self.__register_signal(SUSPEND_SIGNAL, self.__suspend)

    def __interrupt(self, *args):
        self._interrupted = True
        self._connector_agent.destroy()  # interrupts any blocking socket.
        self._connector_storage.destroy()

    def __suspend(self, *args):
        assert self._resume_event is not None
        assert self._resumed_event is not None

        self._resume_event.wait()  # stops any computation in the main thread until the event is triggered

        # Ensures the processor agent knows we stopped waiting on `_resume_event`, as to avoid re-entrant wait on the
        # event.
        self._resumed_event.set()

    def __suspend_listener_main(self):
        # Bridges suspend requests from the parent (worker agent) into a Py_AddPendingCall scheduled on the
        # main interpreter thread. This is the Windows substitute for SIGUSR1: it dispatches `__suspend` at
        # the next CPython eval-breaker check, with the same safe-point semantics as a Python signal handler.
        from scaler.utility import pending_call

        assert self._suspend_trigger is not None
        assert self._listener_shutdown is not None
        while not self._listener_shutdown.is_set():
            if not self._suspend_trigger.wait(timeout=0.1):
                continue
            self._suspend_trigger.clear()
            if self._listener_shutdown.is_set():
                break
            try:
                pending_call.schedule(self.__suspend)
            except RuntimeError:
                logger.exception(f"Processor[{self.pid}]: failed to schedule suspend pending call")

    def __run_forever(self):
        try:
            self._connector_agent.send(ProcessorInitialized())
            while True:
                message = self._connector_agent.receive()
                if message is None:
                    continue

                self.__on_connector_receive(message)

        except ymq.SocketStopRequestedError as e:
            self.__log_exit("agent connector stop requested", exception=e)

        except ObjectStorageException as e:
            # Never swallow this silently: a storage error mid-task orphans the task (no result is ever
            # sent) and the process exits 0, which the agent can only observe later as a zombie.
            self.__log_exit("object storage error", exception=e)

        except (KeyboardInterrupt, InterruptedError):
            self.__log_exit("interrupted")

        except SystemExit as e:
            # SystemExit is a BaseException, so none of the handlers above catch it; log it (with the
            # originating traceback), then re-raise so multiprocessing still reports the requested exit code.
            self.__log_exit("received SystemExit", exception=e)
            raise

        except Exception as e:
            if self.__is_closed_zmq_socket_exception(e):
                self.__log_exit("agent connector socket closed")
                return

            self.__log_exit("unhandled exception", exception=e)

        finally:
            # Wake the suspend listener (if running on Windows) and let it exit before the connectors go away,
            # so it never tries to schedule into a torn-down processor.
            if self._listener_shutdown is not None:
                self._listener_shutdown.set()
            if self._suspend_trigger is not None:
                self._suspend_trigger.set()
            if self._suspend_listener is not None:
                self._suspend_listener.join(timeout=1.0)

            self._object_cache.destroy()
            self._connector_agent.destroy()

            self._object_cache.join()
            self._connector_storage.destroy()

    def __log_exit(self, reason: str, exception: Optional[BaseException] = None) -> None:
        """Logs why the processor's main loop is exiting, escalating severity if a task was in flight."""
        task = self._current_task
        task_context = (
            f" while task_id={task.taskId.hex()} was in flight; no task result will be sent" if task is not None else ""
        )

        if self._interrupted:
            # __interrupt destroyed the connectors because the agent asked us to stop, so anything raised out
            # of an in-flight call is expected teardown, not a fault worth an error and a traceback. Still name
            # the exception, so a genuine fault that races with the teardown is not invisible at every level.
            exception_context = f" ({exception!r})" if exception is not None else ""
            logger.debug(f"Processor[{self.pid}]: stopped on agent request; {reason}{exception_context}{task_context}")
        elif exception is not None:
            logger.error(f"Processor[{self.pid}]: {reason}{task_context}, shutting down", exc_info=exception)
        elif task is not None:
            logger.warning(f"Processor[{self.pid}]: {reason}{task_context}, shutting down")
        else:
            logger.debug(f"Processor[{self.pid}]: {reason}, shutting down")

    def __on_connector_receive(self, message: BaseMessage):
        if isinstance(message, ObjectInstruction):
            self.__on_receive_object_instruction(message)
            return

        if isinstance(message, Task):
            self.__on_received_task(message)
            return

        logger.error(f"unknown {message=}")

    def __on_receive_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete:
            for object_id in instruction.objectMetadata.objectIds:
                self._object_cache.del_object(object_id)
            return

        logger.error(f"worker received unknown object instruction type {instruction=}")

    def __on_received_task(self, task: Task):
        self._current_task = task

        self.__cache_required_object_ids(task)

        self.__process_task(task)

    def __cache_required_object_ids(self, task: Task) -> None:
        required_object_ids = self.__get_required_object_ids_for_task(task)

        for object_id in required_object_ids:
            if self._object_cache.has_object(object_id):
                continue

            object_content = self._connector_storage.get_object(object_id)
            self._object_cache.add_object(task.source, object_id, bytes(object_content))

    @staticmethod
    def __get_required_object_ids_for_task(task: Task) -> list[ObjectID]:
        serializer_id = ObjectID.generate_serializer_object_id(task.source)
        object_ids = [
            serializer_id,
            ObjectID(task.funcObjectId),
            *(ObjectID(argument.data) for argument in task.functionArgs),
        ]
        return object_ids

    def __process_task(self, task: Task):
        task_flags = retrieve_task_flags_from_task(task)

        try:
            function = self._object_cache.get_object(ObjectID(task.funcObjectId))

            args = [self._object_cache.get_object(ObjectID(argument.data)) for argument in task.functionArgs]

            if task_flags.stream_output:
                with (
                    StreamingBuffer(task.taskId, TaskLog.LogType.stdout, self._connector_agent) as stdout_buf,
                    StreamingBuffer(task.taskId, TaskLog.LogType.stderr, self._connector_agent) as stderr_buf,
                    self.__processor_context(),
                    redirect_stdout(cast(IO[str], stdout_buf)),
                    redirect_stderr(cast(IO[str], stderr_buf)),
                ):
                    result = function(*args)
            else:
                with self.__processor_context():
                    result = function(*args)

            result_bytes = self._object_cache.serialize(task.source, result)
            task_result_type = TaskResultType.success

        except Exception as e:
            logger.exception(f"exception when processing task_id={task.taskId.hex()}:")
            task_result_type = TaskResultType.failed
            result_bytes = serialize_failure(e)

        self.__send_result(task.source, task.taskId, task_result_type, result_bytes)

    def __send_result(self, source: ClientID, task_id: TaskID, task_result_type: TaskResultType, result_bytes: bytes):
        result_object_id = self.__store_result(source, task_id, result_bytes)

        self.__send_to_agent(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.create,
                objectUser=source,
                objectMetadata=ObjectMetadata(
                    objectIds=(result_object_id,),
                    objectTypes=(ObjectMetadata.ObjectContentType.object,),
                    objectNames=(f"<res {repr(result_object_id)}>".encode(),),
                ),
            ),
            task_id,
            "announce the result object",
        )
        self.__send_to_agent(
            TaskResult(taskId=task_id, resultType=task_result_type, metadata=b"", results=[bytes(result_object_id)]),
            task_id,
            "send the task result",
        )

        # only once the result is fully handed off is the task no longer in flight, so a failure above still
        # reports which task got orphaned
        self._current_task = None

    def __store_result(self, source: ClientID, task_id: TaskID, result_bytes: bytes) -> ObjectID:
        """Writes a finished task's result to the object storage, returning the object ID it landed under.

        Every attempt uses a fresh ID, so a retry that follows a partially received request cannot announce
        one the server holds a half-written object under.
        """

        def store() -> ObjectID:
            result_object_id = ObjectID.generate_object_id(source)
            self._connector_storage.set_object(result_object_id, result_bytes)
            return result_object_id

        return self.__hand_off(store, task_id, "store the result")

    def __send_to_agent(self, message: BaseMessage, task_id: TaskID, description: str) -> None:
        self.__hand_off(lambda: self._connector_agent.send(message), task_id, description)

    def __hand_off(self, step: Callable[[], _T], task_id: TaskID, description: str) -> _T:
        """Runs one step of a finished task's result hand-off, retrying it if the transport drops under us.

        The work behind the result is already paid for, possibly hours of it, and cannot be redone, whereas
        letting the failure unwind the main loop exits the processor and loses it. Repeating a step is safe:
        the agent has not been told about the result yet. A teardown the agent asked for is never retried.

        Replaying is the sender's job. A socket reconnects on its own, but a write that was in flight when
        the connection dropped is failed rather than resent, and a storage request is a header and a payload
        the server frames in order, so a replay has to start at the header.
        """

        attempt = 1
        delay = RESULT_HAND_OFF_RETRY_DELAY_SECONDS

        while True:
            try:
                return step()
            except (ymq.SocketStopRequestedError, ObjectStorageException) as e:
                if self._interrupted or attempt >= RESULT_HAND_OFF_MAX_ATTEMPTS:
                    raise

                logger.warning(
                    f"Processor[{self.pid}]: failed to {description} of task_id={task_id.hex()} on attempt "
                    f"{attempt}/{RESULT_HAND_OFF_MAX_ATTEMPTS} ({e!r}), retrying in {delay} seconds"
                )

            time.sleep(delay)
            delay *= 2
            attempt += 1

    @staticmethod
    def __set_current_processor(context: Optional["Processor"]) -> Token:
        if context is not None and _current_processor.get() is not None:
            raise ValueError("cannot override a previously set processor context.")

        return _current_processor.set(context)

    @contextlib.contextmanager
    def __processor_context(self):
        self.__set_current_processor(self)
        try:
            yield
        finally:
            self.__set_current_processor(None)

    @staticmethod
    def __register_signal(signal_name: str, handler: Callable) -> None:
        signal_instance = getattr(signal, signal_name, None)
        if signal_instance is None:
            raise RuntimeError(f"unsupported platform, signal not available: {signal_name}.")

        signal.signal(signal_instance, handler)

    @staticmethod
    def __is_closed_zmq_socket_exception(exception: Exception) -> bool:
        """Validates whether exception represents a closed ZMQ socket error, lazily importing pyzmq."""

        if exception.__class__.__name__ != "ZMQError":
            return False

        import zmq

        assert isinstance(exception, zmq.error.ZMQError)

        return exception.errno == zmq.ENOTSOCK
