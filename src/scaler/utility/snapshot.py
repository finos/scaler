import dataclasses
from typing import Any

from scaler.utility.identifiers import TaskID, WorkerID


@dataclasses.dataclass
class InformationSnapshot:
    tasks: dict[TaskID, Any]
    workers: dict[WorkerID, Any]
