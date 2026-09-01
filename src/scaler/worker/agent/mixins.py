import abc
from typing import Optional

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    ObjectInstruction,
    ProcessorInitialized,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeatEcho,
)
from scaler.utility.identifiers import ProcessorID, TaskID
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.worker.agent.processor_holder import ProcessorHolder


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_storage_address(self) -> AddressConfig:
        raise NotImplementedError()


class TimeoutManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update_last_seen_time(self):
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_task_new(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_cancel_task(self, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_queued_size(self):
        raise NotImplementedError()


class ProcessorManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def can_accept_task(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def wait_until_can_accept_task(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_processor_initialized(self, processor_id: ProcessorID, processor_initialized: ProcessorInitialized):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_cancel_task(self, task_id: TaskID) -> Optional[Task]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_failing_processor(self, processor_id: ProcessorID, process_status: str):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_suspend_task(self, task_id: TaskID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_resume_task(self, task_id: TaskID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, processor_id: ProcessorID, task_result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_external_object_instruction(self, instruction: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_internal_object_instruction(self, processor_id: ProcessorID, instruction: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self, reason: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def current_processor_is_initialized(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def current_task(self) -> Optional[Task]:
        raise NotImplementedError()

    @abc.abstractmethod
    def current_task_id(self) -> TaskID:
        raise NotImplementedError()

    @abc.abstractmethod
    def processors(self) -> list[ProcessorHolder]:
        raise NotImplementedError()

    @abc.abstractmethod
    def num_suspended_processors(self) -> int:
        raise NotImplementedError()


class ProfilingManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_process_start(self, pid: int):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_process_end(self, pid: int):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_start(self, pid: int, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_end(self, pid: int, task_id: TaskID) -> ProfileResult:
        raise NotImplementedError()
