import dataclasses
import enum


@dataclasses.dataclass(frozen=True)
class WorkerManagerSnapshot:
    """Immutable snapshot of a worker manager's state, passed to stateless scaling policies."""

    worker_manager_id: bytes
    max_task_concurrency: int
    worker_count: int
    last_seen_s: float  # time.time() epoch seconds of last heartbeat
    capabilities: dict[str, int] = dataclasses.field(default_factory=dict)


class ScalingPolicyStrategy(enum.Enum):
    NO = "no"
    VANILLA = "vanilla"
    CAPABILITY = "capability"

    def __str__(self):
        return self.name
