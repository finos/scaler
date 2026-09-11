from typing import TYPE_CHECKING, List

from scaler.protocol.capnp import ProcessorStatus
from scaler.worker_manager.proxy.mixins import ProcessorStatusProvider

if TYPE_CHECKING:
    from scaler.worker_manager.proxy.task_manager import TaskManager


class SymphonyProcessorStatusProvider(ProcessorStatusProvider):
    def set_task_manager(self, task_manager: "TaskManager") -> None:
        pass

    def get_processor_statuses(self) -> List[ProcessorStatus]:
        return []
