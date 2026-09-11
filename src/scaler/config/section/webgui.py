import dataclasses

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.security import SecurityConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig
from scaler.config.types.http import HTTPConfig


@dataclasses.dataclass
class WebGUIConfig(ConfigClass):
    monitor_address: AddressConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler monitor address to connect to")
    )
    gui_address: HTTPConfig = dataclasses.field(
        default_factory=lambda: HTTPConfig("0.0.0.0", 50001),
        metadata=dict(help="host and port for the web server (e.g. 0.0.0.0:50001)"),
    )
    broadcast_interval_seconds: float = dataclasses.field(
        default=defaults.DEFAULT_GUI_BROADCAST_INTERVAL_SECONDS,
        metadata=dict(
            short="-bi",
            help="how often (seconds) the web GUI pushes updates to browsers; drives the streaming chart "
            "smoothness. Raise it to cut browser and network load with many connected viewers.",
        ),
    )
    task_log_max_size: int = dataclasses.field(
        default=defaults.DEFAULT_GUI_TASK_LOG_MAX_SIZE,
        metadata=dict(
            short="-tl",
            help="how many completed tasks and task-log events the web GUI keeps; it pages through them "
            "server-side, so this bounds memory rather than what you can browse.",
        ),
    )
    status_report_interval_seconds: int = dataclasses.field(
        default=defaults.STATUS_REPORT_INTERVAL_SECONDS,
        metadata=dict(
            short="-sri",
            help="the scheduler's status report interval (its own -sri); the GUI marks the scheduler stale after "
            "about 5x this without an update, so set it to match the scheduler.",
        ),
    )

    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    security: SecurityConfig = dataclasses.field(default_factory=SecurityConfig)

    def __post_init__(self):
        if self.broadcast_interval_seconds <= 0:
            raise ValueError("broadcast_interval_seconds must be positive.")
        if self.task_log_max_size <= 0:
            raise ValueError("task_log_max_size must be positive.")
        if self.status_report_interval_seconds <= 0:
            raise ValueError("status_report_interval_seconds must be positive.")
