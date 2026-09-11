import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.security import SecurityConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class PolicyConfig(ConfigClass):
    policy_engine_type: str = dataclasses.field(
        default="simple", metadata=dict(short="-et", help="Specify the policy config type, default to legacy")
    )

    policy_content: str = dataclasses.field(
        default="allocate=even_load; scaling=vanilla",
        metadata=dict(short="-pc", help="Policy string: 'allocate=VAL; scaling=VAL'"),
    )


@dataclasses.dataclass
class SchedulerConfig(ConfigClass):
    bind_address: AddressConfig = dataclasses.field(
        metadata=dict(positional=True, required=True, help="scheduler address to bind to, e.g.: `tcp://0.0.0.0:6378`")
    )
    object_storage_address: AddressConfig = dataclasses.field(
        metadata=dict(
            short="-osa",
            required=True,
            help="specify the object storage server address for scheduler to connect to, e.g.: tcp://127.0.0.1:6379",
        )
    )
    advertised_object_storage_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-aosa",
            help=(
                "advertised object storage address forwarded to clients/workers; defaults to object_storage_address. "
                "Use this when scheduler is local/private but clients/workers connect via public IP/port."
            ),
        ),
    )
    monitor_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-ma",
            help="specify monitoring address, if not specified, the monitoring address is scheduler address with port "
            "number plus 2, e.g.: if scheduler address is tcp://localhost:2345, then monitoring address is "
            "tcp://localhost:2347",
        ),
    )
    status_report_interval_seconds: int = dataclasses.field(
        default=defaults.STATUS_REPORT_INTERVAL_SECONDS,
        metadata=dict(
            short="-sri",
            help="number of seconds between scheduler status reports to the monitors (scaler_top / scaler_gui). "
            "Each report serializes every worker and processor, so raise this to cut monitoring overhead when "
            "running thousands of workers or tasks.",
        ),
    )
    object_report_limit: int = dataclasses.field(
        default=defaults.OBJECT_REPORT_LIMIT,
        metadata=dict(
            short="-orl",
            help="how many of the biggest objects each status report carries, which is how many the web GUI "
            "can page through. Each one costs the report about 240 bytes.",
        ),
    )
    protected: bool = dataclasses.field(
        default=False,
        metadata=dict(
            short="-p", action="store_true", help="protect scheduler and worker from being shutdown by client"
        ),
    )
    max_number_of_tasks_waiting: int = dataclasses.field(
        default=defaults.DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        metadata=dict(short="-mt", help="max number of tasks can wait in scheduler while all workers are full"),
    )
    client_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_CLIENT_TIMEOUT_SECONDS,
        metadata=dict(short="-ct", help="discard client when timeout seconds reached"),
    )
    worker_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_TIMEOUT_SECONDS,
        metadata=dict(short="-wt", help="discard worker when timeout seconds reached"),
    )
    object_retention_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_OBJECT_RETENTION_SECONDS,
        metadata=dict(short="-ot", help="discard function in scheduler when timeout seconds reached"),
    )
    load_balance_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_LOAD_BALANCE_SECONDS,
        metadata=dict(
            short="-ls",
            help="number of seconds between load balance operations in scheduler; a negative value disables it",
        ),
    )
    load_balance_trigger_times: int = dataclasses.field(
        default=defaults.DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        metadata=dict(
            short="-lbt",
            help="exact number of repeated load balance advices when trigger load balance operation in scheduler",
        ),
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-it", help="set the number of io threads for io backend"),
    )
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    policy: PolicyConfig = dataclasses.field(default_factory=PolicyConfig)

    security: SecurityConfig = dataclasses.field(default_factory=SecurityConfig)

    def __post_init__(self):
        if self.max_number_of_tasks_waiting < -1:
            raise ValueError("max_number_of_tasks_waiting must be -1 (for unlimited) or non-negative.")
        if self.client_timeout_seconds <= 0 or self.worker_timeout_seconds <= 0 or self.object_retention_seconds <= 0:
            raise ValueError("All timeout/retention second values must be positive.")
        # A negative load_balance_seconds disables load balancing (create_async_loop_routine treats a
        # negative interval as "off"); zero would busy-loop the balancer, so only zero is rejected.
        if self.load_balance_seconds == 0:
            raise ValueError("load_balance_seconds must be non-zero (use a negative value to disable balancing).")
        if self.load_balance_trigger_times <= 0:
            raise ValueError("load_balance_trigger_times must be a positive integer.")
        if self.status_report_interval_seconds <= 0:
            raise ValueError("status_report_interval_seconds must be positive.")
