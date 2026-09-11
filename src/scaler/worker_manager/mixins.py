from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand


class DeclarativeWorkerProvisioner(ABC):
    """Provisioner that converges toward a desired task concurrency via start_units/stop_units.

    A unit is the atomic resource this provisioner allocates - e.g. a VM, a container, or a
    process group. One unit may host one or more workers (see workers_per_provisioner_unit in
    WorkerManagerRunner). Units are identified by opaque strings whose meaning is
    implementation-defined (e.g. an EC2 instance ID).
    """

    @abstractmethod
    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
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
