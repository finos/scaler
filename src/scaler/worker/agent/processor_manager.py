import asyncio
import logging
from typing import Optional

import tblib.pickling_support

# from scaler.utility.logging.utility import setup_logger
from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder, AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import (
    ObjectInstruction,
    ObjectMetadata,
    ProcessorInitialized,
    Task,
    TaskResult,
    TaskResultType,
)
from scaler.utility.exceptions import ProcessorDiedError
from scaler.utility.exitcode import describe_exitcode
from scaler.utility.identifiers import ObjectID, ProcessorID, TaskID, WorkerID
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager, ProcessorManager, ProfilingManager, TaskManager
from scaler.worker.agent.processor_holder import ProcessorHolder

logger = logging.getLogger(__name__)


class VanillaProcessorManager(ProcessorManager):
    def __init__(
        self,
        identity: WorkerID,
        event_loop: str,
        address_internal: AddressConfig,
        scheduler_address: AddressConfig,
        preload: Optional[str],
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_processor_suspend: bool,
        logging_paths: tuple[str, ...],
        logging_level: str,
        security_config: Optional[SecurityConfig] = None,
    ):
        tblib.pickling_support.install()

        self._identity = identity
        self._event_loop = event_loop
        self._scheduler_address = scheduler_address
        self._preload = preload
        self._security_config = security_config

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._hard_processor_suspend = hard_processor_suspend
        self._logging_paths = logging_paths
        self._logging_level = logging_level

        self._heartbeat_manager: Optional[HeartbeatManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._profiling_manager: Optional[ProfilingManager] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        self._address_internal: AddressConfig = address_internal

        self._current_holder: Optional[ProcessorHolder] = None
        self._suspended_holders_by_task_id: dict[bytes, ProcessorHolder] = {}
        self._holders_by_processor_id: dict[ProcessorID, ProcessorHolder] = {}

        self._can_accept_task_lock: asyncio.Lock = asyncio.Lock()

        self._binder_internal: Optional[AsyncBinder] = None

    def register(
        self,
        heartbeat_manager: HeartbeatManager,
        task_manager: TaskManager,
        profiling_manager: ProfilingManager,
        connector_external: AsyncConnector,
        binder_internal: AsyncBinder,
        connector_storage: AsyncObjectStorageConnector,
    ):
        self._heartbeat_manager = heartbeat_manager
        self._task_manager = task_manager
        self._profiling_manager = profiling_manager
        self._connector_external = connector_external
        self._binder_internal = binder_internal
        self._connector_storage = connector_storage

    async def initialize(self):
        await self._can_accept_task_lock.acquire()  # prevents any processor to accept task until initialized

        await self._connector_storage.wait_until_connected()

        self.__start_new_processor()  # we can start the processor now that we know the storage address.

    def can_accept_task(self) -> bool:
        return not self._can_accept_task_lock.locked()

    async def wait_until_can_accept_task(self):
        """
        Makes sure a processor is ready to start processing a new or suspended task.

        Must be called before any call to `on_task()` or `on_task_resume()`.
        """

        await self._can_accept_task_lock.acquire()

    async def on_processor_initialized(self, processor_id: ProcessorID, processor_initialized: ProcessorInitialized):
        assert self._current_holder is not None

        if self._current_holder.initialized():
            return

        self._holders_by_processor_id[processor_id] = self._current_holder
        self._current_holder.initialize(processor_id)

        self._can_accept_task_lock.release()

    async def on_task(self, task: Task) -> bool:
        assert self._can_accept_task_lock.locked()
        assert self.current_processor_is_initialized()

        holder = self._current_holder

        assert holder.task() is None
        holder.set_task(task)

        self._profiling_manager.on_task_start(holder.pid(), task.taskId)

        await self._binder_internal.send(holder.processor_id(), task, detached=True)

        return True

    async def on_cancel_task(self, task_id: TaskID) -> Optional[Task]:
        assert self._current_holder is not None

        if self.current_task_id() == task_id:
            current_task = self.current_task()
            self.__restart_current_processor(f"cancel task_id={task_id.hex()}")
            return current_task

        if task_id in self._suspended_holders_by_task_id:
            suspended_holder = self._suspended_holders_by_task_id.pop(task_id)
            task = suspended_holder.task()
            self.__kill_processor(f"cancel suspended task_id={task_id.hex()}", suspended_holder)
            return task

        return None

    async def on_failing_processor(self, processor_id: ProcessorID, process_status: str):
        assert self._current_holder is not None

        holder = self._holders_by_processor_id.get(processor_id)

        if holder is None:
            return

        task = holder.task()
        if task is not None:
            self._suspended_holders_by_task_id.pop(task.taskId, None)
            profile_result = self.__end_task(holder)  # profiling the task should happen before killing the processor
        else:
            profile_result = None

        # This only fires for zombie or dead processors, so the exit code is already readable here
        # (`Process.exitcode` reaps on read). Name the signal so an OOM kill (SIGKILL) is obvious in the logs
        # instead of just "zombie". Logged before the kill below, so the diagnosis is not separated from the
        # processor it describes by the restart's own logging.
        death = f"process_status={process_status!r} exitcode={describe_exitcode(holder.exitcode())}"
        task_note = f", task_id={task.taskId.hex()}" if task is not None else ""
        logger.warning(f"{self._identity!r}: Processor[{holder.pid()}] died: {death}{task_note}")

        reason = "process died"
        if holder == self._current_holder:
            self.__restart_current_processor(reason)
        else:
            self.__kill_processor(reason, holder)

        if task is not None:
            source = task.source
            task_id = task.taskId

            result_object_id = ObjectID.generate_object_id(source)
            result_object_bytes = serialize_failure(ProcessorDiedError(death))

            await self._connector_storage.set_object(result_object_id, result_object_bytes)
            await self._connector_external.send(
                ObjectInstruction(
                    instructionType=ObjectInstruction.ObjectInstructionType.create,
                    objectUser=source,
                    objectMetadata=ObjectMetadata(
                        objectIds=(result_object_id,),
                        objectTypes=(ObjectMetadata.ObjectContentType.object,),
                        objectNames=(b"",),
                    ),
                ),
                detached=True,
            )

            await self._task_manager.on_task_result(
                TaskResult(
                    taskId=task_id,
                    resultType=TaskResultType.failed,
                    metadata=profile_result.serialize(),
                    results=[bytes(result_object_id)],
                )
            )

    async def on_suspend_task(self, task_id: TaskID) -> bool:
        assert self._current_holder is not None
        holder = self._current_holder

        current_task = holder.task()

        if current_task is None or current_task.taskId != task_id:
            return False

        holder.suspend()
        self._suspended_holders_by_task_id[task_id] = holder

        logger.info(f"{self._identity!r}: suspend Processor[{holder.pid()}]")

        self.__start_new_processor()

        return True

    async def on_resume_task(self, task_id: TaskID) -> bool:
        assert self._can_accept_task_lock.locked()
        assert self.current_processor_is_initialized()

        if self.current_task() is not None:
            return False

        suspended_holder = self._suspended_holders_by_task_id.pop(task_id, None)

        if suspended_holder is None:
            return False

        # A suspended processor that died (e.g. killed by the OS' OOM killer) cannot be resumed. Clean it and keep the
        # current healthy processor.
        if not suspended_holder.resume():
            await self.on_failing_processor(suspended_holder.processor_id(), "died while suspended")
            self._can_accept_task_lock.release()
            return False

        self.__kill_processor("replaced by suspended processor", self._current_holder)

        self._current_holder = suspended_holder

        logger.info(f"{self._identity!r}: resume Processor[{self._current_holder.pid()}]")

        return True

    async def on_task_result(self, processor_id: ProcessorID, task_result: TaskResult):
        assert self._current_holder is not None
        task_id = task_result.taskId

        if task_id == self.current_task_id():
            assert self._current_holder.processor_id() == processor_id

            profile_result = self.__end_task(self._current_holder)

            release_task_lock = True
        elif task_id in self._suspended_holders_by_task_id:
            # Receiving a task result from a suspended processor is possible as the message might have been queued while
            # we were suspending the process.

            holder = self._suspended_holders_by_task_id.pop(task_id)
            assert holder.processor_id() == processor_id

            profile_result = self.__end_task(holder)

            self.__kill_processor("task finished in suspended processor", holder)

            release_task_lock = False
        else:
            return

        await self._task_manager.on_task_result(
            TaskResult(
                taskId=task_id,
                resultType=task_result.resultType,
                metadata=profile_result.serialize(),
                results=task_result.results,
            )
        )

        # task lock must be released after calling `TaskManager.on_task_result()`
        if release_task_lock:
            self._can_accept_task_lock.release()

    async def on_external_object_instruction(self, instruction: ObjectInstruction):
        processor_ids = list(self._holders_by_processor_id.keys())

        for processor_id in processor_ids:
            if processor_id not in self._holders_by_processor_id:
                continue  # processor got killed while we were iterating over the list

            await self._binder_internal.send(processor_id, instruction, detached=True)

    async def on_internal_object_instruction(self, processor_id: ProcessorID, instruction: ObjectInstruction):
        if not self.__processor_ready_to_process_object(processor_id):
            return

        await self._connector_external.send(instruction, detached=True)

    def destroy(self, reason: str):
        if self._connector_storage is not None:
            self._connector_external.destroy()

        self.__kill_all_processors(reason)

    def current_processor_is_initialized(self) -> bool:
        return self._current_holder is not None and self._current_holder.initialized()

    def current_task(self) -> Optional[Task]:
        if self._current_holder is None:  # worker is not yet initialized
            return None

        return self._current_holder.task()

    def current_task_id(self) -> Optional[TaskID]:
        task = self.current_task()

        if task is None:
            return None
        else:
            return task.taskId

    def processors(self) -> list[ProcessorHolder]:
        return list(self._holders_by_processor_id.values())

    def num_suspended_processors(self) -> int:
        return len(self._suspended_holders_by_task_id)

    def __start_new_processor(self):
        object_storage_address = self._heartbeat_manager.get_object_storage_address()

        self._current_holder = ProcessorHolder(
            self._event_loop,
            self._address_internal,
            self._scheduler_address,
            object_storage_address,
            self._preload,
            self._garbage_collect_interval_seconds,
            self._trim_memory_threshold_bytes,
            self._hard_processor_suspend,
            self._logging_paths,
            self._logging_level,
            self._security_config,
        )

        processor_pid = self._current_holder.pid()

        self._profiling_manager.on_process_start(processor_pid)

        logger.info(f"{self._identity!r}: start Processor[{processor_pid}]")

    def __kill_processor(self, reason: str, holder: ProcessorHolder):
        processor_pid = holder.pid()

        self._profiling_manager.on_process_end(processor_pid)

        if holder.initialized():
            self._holders_by_processor_id.pop(holder.processor_id(), None)

        holder.kill()

        logger.info(f"{self._identity!r}: stop Processor[{processor_pid}], reason: {reason}")

    def __restart_current_processor(self, reason: str):
        assert self._current_holder is not None

        self.__kill_processor(reason, self._current_holder)
        self.__start_new_processor()

    def __kill_all_processors(self, reason: str):
        if self._current_holder is not None:
            self.__kill_processor(reason, self._current_holder)
            self._current_holder = None

        for processor_holder in self._suspended_holders_by_task_id.values():
            self.__kill_processor(reason, processor_holder)

        self._suspended_holders_by_task_id = {}
        self._holders_by_processor_id = {}

    def __end_task(self, processor_holder: ProcessorHolder) -> ProfileResult:
        profile_result = self._profiling_manager.on_task_end(processor_holder.pid(), processor_holder.task().taskId)
        processor_holder.set_task(None)

        return profile_result

    def __processor_ready_to_process_object(self, processor_id: ProcessorID) -> bool:
        holder = self._holders_by_processor_id.get(processor_id)

        if holder is None:
            return False

        assert holder.initialized()

        if holder.task() is None:
            return False

        # TODO: check if the objects belong to the task

        return True
