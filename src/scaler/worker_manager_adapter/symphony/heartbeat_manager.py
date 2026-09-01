from typing import TYPE_CHECKING

from scaler.protocol.capnp import ProcessorStatus
from scaler.worker_manager_adapter.mixins import ProcessorStatusProvider

if TYPE_CHECKING:
    from scaler.worker_manager_adapter.task_manager import TaskManager


class SymphonyProcessorStatusProvider(ProcessorStatusProvider):
    def set_task_manager(self, task_manager: "TaskManager") -> None:
        pass

    def get_processor_statuses(self) -> list[ProcessorStatus]:
        return []
