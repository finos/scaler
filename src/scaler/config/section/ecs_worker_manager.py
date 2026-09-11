import dataclasses
from typing import ClassVar, Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class ECSWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "aws_raw_ecs"

    worker_manager_config: WorkerManagerConfig

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    # AWS / ECS specific configuration
    aws_access_key_id: Optional[str] = dataclasses.field(
        default=None, metadata=dict(env_var="AWS_ACCESS_KEY_ID", help="AWS access key id")
    )
    aws_secret_access_key: Optional[str] = dataclasses.field(
        default=None, metadata=dict(env_var="AWS_SECRET_ACCESS_KEY", help="AWS secret access key")
    )
    aws_region: str = dataclasses.field(default="us-east-1", metadata=dict(help="AWS region for ECS cluster"))
    ecs_subnets: list[str] = dataclasses.field(
        default_factory=list,
        metadata=dict(
            type=lambda s: [x for x in s.split(",") if x],
            required=True,
            help="Comma-separated list of AWS subnet IDs for ECS tasks",
        ),
    )
    ecs_cluster: str = dataclasses.field(default="scaler-cluster", metadata=dict(help="ECS cluster name"))
    ecs_task_image: str = dataclasses.field(
        default="public.ecr.aws/v4u8j8r6/scaler:latest", metadata=dict(help="Container image used for ECS tasks")
    )
    ecs_python_requirements: str = dataclasses.field(
        default="tomli;pargraph;parfun;pandas", metadata=dict(help="Python requirements string passed to the ECS task")
    )
    ecs_python_version: str = dataclasses.field(default="3.12.11", metadata=dict(help="Python version for ECS task"))
    ecs_task_definition: str = dataclasses.field(
        default="scaler-task-definition", metadata=dict(help="ECS task definition")
    )
    ecs_task_cpu: int = dataclasses.field(
        default=4, metadata=dict(help="Number of vCPUs for task (used to derive worker count)")
    )
    ecs_task_memory: int = dataclasses.field(default=30, metadata=dict(help="Task memory in GB for Fargate"))

    def __post_init__(self):
        if self.ecs_task_cpu <= 0:
            raise ValueError("ecs_task_cpu must be a positive integer.")
        if self.ecs_task_memory <= 0:
            raise ValueError("ecs_task_memory must be a positive integer.")
        if not isinstance(self.ecs_subnets, list) or len(self.ecs_subnets) == 0:
            raise ValueError("ecs_subnets must be a non-empty list of subnet ids.")
        if not self.ecs_cluster:
            raise ValueError("ecs_cluster cannot be an empty string.")
        if not self.ecs_task_definition:
            raise ValueError("ecs_task_definition cannot be an empty string.")
        if not self.ecs_task_image:
            raise ValueError("ecs_task_image cannot be an empty string.")
