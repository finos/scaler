import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.utility.logging.utility import LoggingLevel


@dataclasses.dataclass
class LoggingConfig(ConfigClass):
    paths: tuple[str, ...] = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_PATHS,
        metadata=dict(
            type=str,
            long="--logging-paths",
            short="-lp",
            nargs="*",
            help="specify where the cluster's log should logged to, there can be multiple paths."
            '"/dev/stdout" means output to stdout, and is the default. "/dev/stderr" means output to stderr. '
            "each worker has its own log file with process id appended to the path",
        ),
    )
    config_file: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            long="--logging-config-file",
            short="-lcf",
            help="provide a separate config file for logging, in python's standard .conf format. "
            "this will supercede configuration passed to other --logging-* parameters, "
            "and also does not support per-worker logging",
        ),
    )
    level: str = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_LEVEL,
        metadata=dict(
            long="--logging-level",
            short="-ll",
            choices=[member.name for member in LoggingLevel if member is not LoggingLevel.NOTSET],
            help="set the logging level",
        ),
    )
