from typing import TYPE_CHECKING, List, Optional

from scaler.protocol.capnp import ProcessorStatus, Resource
from scaler.worker_manager.proxy.mixins import ProcessorStatusProvider

if TYPE_CHECKING:
    from scaler.worker_manager.proxy.task_manager import TaskManager


class AWSBatchProcessorStatusProvider(ProcessorStatusProvider):
    def __init__(self) -> None:
        self._task_manager: Optional["TaskManager"] = None

    def set_task_manager(self, task_manager: "TaskManager") -> None:
        self._task_manager = task_manager

    def get_processor_statuses(self) -> List[ProcessorStatus]:
        assert self._task_manager is not None, "set_task_manager() must be called before get_processor_statuses()"
        processing_tasks = self._task_manager.processing_task_count
        return [
            ProcessorStatus(
                pid=0, initialized=True, hasTask=processing_tasks > 0, suspended=False, resource=Resource(cpu=0, rss=0)
            )
        ]
