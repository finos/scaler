import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig


@dataclasses.dataclass
class WorkerManagerConfig(ConfigClass):
    scheduler_address: AddressConfig = dataclasses.field(
        metadata=dict(positional=True, required=True, help="scheduler address the worker manager itself connects to")
    )

    worker_manager_id: str = dataclasses.field(
        metadata=dict(short="-wmi", required=True, help="worker manager ID to identify this manager")
    )

    worker_scheduler_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-wsa",
            help=(
                "scheduler address forwarded to spawned workers; defaults to scheduler_address if not set. "
                "Use this when the manager and workers are on different networks (e.g. NAT/EC2 setups) "
                "and the manager's local address is not reachable from remote workers."
            ),
        ),
    )

    object_storage_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(short="-osa", help="specify the object storage server address, e.g.: tcp://localhost:2346"),
    )

    max_task_concurrency: int = dataclasses.field(
        default=defaults.DEFAULT_MAX_TASK_CONCURRENCY,
        metadata=dict(
            short="-mtc",
            help=(
                "maximum number of workers that can be started, -1 means no limit."
                "for fixed native worker manager, this is exactly the number of workers that will be spawned"
            ),
        ),
    )

    scale_down_cooldown_seconds: float = dataclasses.field(
        default=defaults.DEFAULT_WORKER_MANAGER_SCALE_DOWN_COOLDOWN_SECONDS,
        metadata=dict(
            short="-sdc",
            help=(
                "minimum number of seconds a scale-down must be requested for before it is honored, "
                "to avoid flapping under intermittent load. 0 disables the cooldown."
            ),
        ),
    )

    @property
    def effective_worker_scheduler_address(self) -> AddressConfig:
        return self.worker_scheduler_address if self.worker_scheduler_address is not None else self.scheduler_address

    def __post_init__(self) -> None:
        if not self.worker_manager_id:
            raise ValueError("worker_manager_id cannot be an empty string.")
        if self.max_task_concurrency != -1 and self.max_task_concurrency < 0:
            raise ValueError("max_task_concurrency must be -1 (no limit) or a non-negative integer.")
        if self.scale_down_cooldown_seconds < 0:
            raise ValueError("scale_down_cooldown_seconds must be a non-negative number.")
