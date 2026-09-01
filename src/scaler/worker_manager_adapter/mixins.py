from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from scaler.protocol.capnp import ProcessorStatus, Task, TaskCancel
from scaler.utility.identifiers import TaskID

TaskDeserializer = Callable[[Task], Awaitable[tuple[Any, list[Any]]]]

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand
    from scaler.worker_manager_adapter.task_manager import TaskManager


class ProcessorStatusProvider(ABC):
    @abstractmethod
    def set_task_manager(self, task_manager: TaskManager) -> None: ...

    @abstractmethod
    def get_processor_statuses(self) -> list[ProcessorStatus]: ...


class TaskInputLoader(ABC):
    @abstractmethod
    async def load_task_inputs(self, task: Task) -> tuple[Any, list[Any]]: ...

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


class DeclarativeWorkerProvisioner(ABC):
    """Provisioner that converges toward a desired task concurrency via start_units/stop_units.

    A unit is the atomic resource this provisioner allocates - e.g. a VM, a container, or a
    process group. One unit may host one or more workers (see workers_per_provisioner_unit in
    WorkerManagerRunner). Units are identified by opaque strings whose meaning is
    implementation-defined (e.g. an EC2 instance ID).
    """

    @abstractmethod
    async def set_desired_task_concurrency(
        self, requests: list[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None: ...

    @abstractmethod
    async def start_units(self, count: int) -> None:
        """Launch `count` new units."""
        ...

    @abstractmethod
    async def stop_units(self, count: int) -> None:
        """Shut down `count` units."""
        ...

    @abstractmethod
    def active_unit_count(self) -> int:
        """Return the number of currently active units."""
        ...

    @abstractmethod
    async def terminate(self) -> None:
        """Cancel the capacity coordinator and stop all running units."""
        ...
