import dataclasses
import enum
import logging
import logging.config
import logging.handlers
import os
import typing

from scaler.config.defaults import DEFAULT_LOGGING_PATHS

logger = logging.getLogger(__name__)


class LogType(enum.Enum):
    Stdout = enum.auto()
    Stderr = enum.auto()
    File = enum.auto()


@dataclasses.dataclass
class LogPath:
    log_type: LogType
    path: str


class LoggingLevel(enum.Enum):
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


def setup_logger(
    log_paths: tuple[str, ...] = DEFAULT_LOGGING_PATHS,
    logging_config_file: typing.Optional[str] = None,
    logging_level: str = LoggingLevel.INFO.name,
    process_name: str = "scaler",
):
    if not log_paths and not logging_config_file:
        return

    if isinstance(log_paths, str):
        log_paths = (log_paths,)

    if logging_config_file is not None:
        print(f"use logging config file: {logging_config_file}")
        logging.config.fileConfig(logging_config_file, disable_existing_loggers=True)
        return

    resolved_log_paths = [LogPath(log_type=detect_log_type(file_name), path=file_name) for file_name in log_paths]
    __logging_config(log_paths=resolved_log_paths, logging_level=logging_level, process_name=process_name)
    logger.info(f"logging to {log_paths}")


def detect_log_type(file_name: str) -> LogType:
    """Classify a configured log path: "-" and "/dev/stdout" mean stdout, "/dev/stderr" means stderr, else a file."""
    if file_name in {"-", "/dev/stdout"}:
        return LogType.Stdout

    if file_name == "/dev/stderr":
        return LogType.Stderr

    return LogType.File


def __generate_log_config(process_name: str) -> dict:
    standard_format = f"%(asctime)s %(levelname)s {process_name}[%(process)d]: %(message)s"
    verbose_format = (
        f"%(asctime)s %(levelname)s {process_name}[%(process)d] " f"%(name)s:%(funcName)s:%(lineno)s: %(message)s"
    )
    return {
        "version": 1,
        "disable_existing_loggers": False,  # this fixes the problem
        "formatters": {
            "standard": {"format": standard_format, "datefmt": "%Y-%m-%d %H:%M:%S%z"},
            "verbose": {"format": verbose_format, "datefmt": "%Y-%m-%d %H:%M:%S%z"},
        },
        "handlers": {},
        # Configure only the "scaler" logger so we do not mutate the host
        # application's root logger (library-safety). propagate=False prevents
        # our handlers from re-emitting through any root handler the host set up.
        "loggers": {"scaler": {"handlers": [], "level": "DEBUG", "propagate": False}},
    }


def __logging_config(
    log_paths: list[LogPath], logging_level: str = LoggingLevel.INFO.name, process_name: str = "scaler"
):
    logging.addLevelName(logging.INFO, "INFO")
    logging.addLevelName(logging.WARNING, "WARNING")
    logging.addLevelName(logging.ERROR, "ERROR")
    logging.addLevelName(logging.DEBUG, "DEBUG")
    logging.addLevelName(logging.CRITICAL, "CRITICAL")

    config = __generate_log_config(process_name)
    handlers = config["handlers"]
    scaler_handlers = config["loggers"]["scaler"]["handlers"]

    for log_path in log_paths:
        if log_path.log_type == LogType.Stdout:
            if "console_stdout" not in handlers:
                handlers["console_stdout"] = __create_stream_handler(logging_level, "ext://sys.stdout")
                scaler_handlers.append("console_stdout")
            continue

        elif log_path.log_type == LogType.Stderr:
            if "console_stderr" not in handlers:
                handlers["console_stderr"] = __create_stream_handler(logging_level, "ext://sys.stderr")
                scaler_handlers.append("console_stderr")
            continue

        elif log_path.log_type == LogType.File:
            handlers[log_path.path] = __create_time_rotating_file_handler(logging_level, log_path.path)
            scaler_handlers.append(log_path.path)
            continue

        raise TypeError(f"Unsupported LogPath: {log_path}")

    logging.config.dictConfig(config)


def __create_stream_handler(logging_level: str, stream: str):
    return {"class": "logging.StreamHandler", "level": logging_level, "formatter": "standard", "stream": stream}


def __create_time_rotating_file_handler(logging_level: str, file_path: str):
    return {
        "class": "logging.handlers.TimedRotatingFileHandler",
        "level": logging_level,
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(file_path)),
        "when": "midnight",
    }


def __create_size_rotating_file_handler(log_path) -> dict:
    return {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "INFO",
        "formatter": "verbose",
        "filename": os.path.expandvars(os.path.expanduser(log_path)),
        "maxBytes": 10485760,
        "backupCount": 20,
        "encoding": "utf8",
    }


def __parse_logging_level(value):
    return LoggingLevel(value).value


def get_logger_info(logger: logging.Logger) -> tuple[str, str, tuple[str, ...]]:
    """
    Retrieves the format string, level string, and all active log paths from a logger's handlers.
    """
    log_level_str = logging.getLevelName(logger.getEffectiveLevel())
    log_format_str = ""
    log_paths: list[str] = []

    if logger.hasHandlers():
        first_handler = logger.handlers[0]
        if first_handler.formatter:
            log_format_str = getattr(first_handler.formatter, "_fmt", "")

        for handler in logger.handlers:
            if isinstance(handler, logging.handlers.BaseRotatingHandler):
                log_paths.append(handler.baseFilename)
            elif isinstance(handler, logging.StreamHandler) and hasattr(handler.stream, "name"):
                if "stdout" in handler.stream.name:
                    log_paths.append("/dev/stdout")
                elif "stderr" in handler.stream.name:
                    log_paths.append("/dev/stderr")

    # If no specific path was found, default to stdout
    if not log_paths:
        log_paths.append("/dev/stdout")

    return log_format_str, log_level_str, tuple(log_paths)
