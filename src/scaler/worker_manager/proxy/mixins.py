from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Tuple

from scaler.protocol.capnp import ProcessorStatus, Task, TaskCancel
from scaler.utility.identifiers import TaskID

TaskDeserializer = Callable[[Task], Awaitable[Tuple[Any, List[Any]]]]

if TYPE_CHECKING:
    from scaler.worker_manager.proxy.task_manager import TaskManager


class ProcessorStatusProvider(ABC):
    @abstractmethod
    def set_task_manager(self, task_manager: TaskManager) -> None: ...

    @abstractmethod
    def get_processor_statuses(self) -> List[ProcessorStatus]: ...


class TaskInputLoader(ABC):
    @abstractmethod
    async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]: ...

    @abstractmethod
    def register(self, load_task_inputs: TaskDeserializer) -> None: ...


class ExecutionBackend(ABC):
    @abstractmethod
    async def execute(self, task: Task) -> asyncio.Future: ...

    @abstractmethod
    async def on_cancel(self, task_cancel: TaskCancel) -> None: ...

    @abstractmethod
    def on_cleanup(self, task_id: TaskID) -> None: ...

    @abstractmethod
    async def routine(self) -> None: ...

    @abstractmethod
    def register(self, load_task_inputs: TaskDeserializer) -> None: ...

    @abstractmethod
    def close(self) -> None:
        """Release whatever the backend holds outside the process, before the worker exits.

        Called once from WorkerProcess after its event loop has stopped, so it cannot await. A backend
        whose remote resources outlive an un-torn-down process, or whose library needs an orderly
        shutdown, does that work here."""
        ...
