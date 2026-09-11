import dataclasses
from typing import ClassVar, Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.python_worker_environment import PythonWorkerEnvironmentConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.network_backend import NetworkBackendType


@dataclasses.dataclass
class ORBAWSEC2WorkerManagerConfig(ConfigClass):
    """Configuration for the ORB AWS EC2 worker manager."""

    _tag: ClassVar[str] = "orb_aws_ec2"

    worker_manager_config: WorkerManagerConfig

    aws_region: str = dataclasses.field(
        metadata=dict(required=True, help="AWS region where ORB launches worker instances")
    )

    # ORB AWS EC2 Template configuration
    image_id: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            help="AMI ID for the worker instances. If not provided, the latest AL2023 AMI is discovered automatically."
        ),
    )

    python_worker_environment: PythonWorkerEnvironmentConfig = dataclasses.field(
        default_factory=PythonWorkerEnvironmentConfig
    )

    def __post_init__(self) -> None:
        if self.image_id is not None:
            if (
                self.python_worker_environment.python_version is not None
                or self.python_worker_environment.requirements_txt is not None
            ):
                raise ValueError("--image-id is mutually exclusive with --python-version and --requirements-txt")
        else:
            if (
                self.python_worker_environment.python_version is None
                or self.python_worker_environment.requirements_txt is None
            ):
                raise ValueError(
                    "Both --python-version and --requirements-txt must be provided when --image-id is not specified"
                )

    aws_profile: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            help=(
                "AWS named profile to use for authentication. "
                "Leave unset to use the default credential chain (env vars, instance role, etc.)."
            )
        ),
    )
    key_name: Optional[str] = dataclasses.field(
        default=None, metadata=dict(help="AWS key pair name for the instances (optional)")
    )
    subnet_id: Optional[str] = dataclasses.field(
        default=None, metadata=dict(help="AWS subnet ID where the instances will be launched (optional)")
    )

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    instance_type: str = dataclasses.field(default="t2.micro", metadata=dict(help="EC2 instance type"))
    security_group_ids: list[str] = dataclasses.field(
        default_factory=list,
        metadata=dict(
            type=lambda s: [x for x in s.split(",") if x], help="Comma-separated list of AWS security group IDs"
        ),
    )
    instance_tags: dict[str, str] = dataclasses.field(
        default_factory=dict,
        metadata=dict(
            type=lambda s: dict(kv.split("=", 1) for kv in s.split(",") if "=" in kv),
            help="Comma-separated Key=Value EC2 tags applied to worker instances (e.g. 'Name=my-worker,Env=prod')",
        ),
    )
    debug_dump_path: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(help="If set, dump config and template kwargs as JSON files to this directory for debugging"),
    )
    network_backend: NetworkBackendType = dataclasses.field(
        default=NetworkBackendType.zmq, metadata=dict(help="Network backend for worker communication (zmq or ymq)")
    )
