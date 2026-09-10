import dataclasses
from typing import ClassVar, Dict

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.python_worker_environment import PythonWorkerEnvironmentConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class KubernetesWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "k8s_raw"

    worker_manager_config: WorkerManagerConfig

    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    # Auth / cluster
    kubeconfig_path: str = dataclasses.field(
        default="",
        metadata=dict(
            help=(
                "Path to a kubeconfig file for cluster authentication. "
                "Leave empty to use in-cluster auth (default when running inside a Pod)."
            )
        ),
    )
    namespace: str = dataclasses.field(
        default="default", metadata=dict(help="Kubernetes namespace in which worker Pods are created.")
    )

    # Pod image
    pod_image: str = dataclasses.field(
        default="",
        metadata=dict(
            required=True,
            help="Container image used for worker Pods (e.g. 'myregistry/scaler-worker:latest'). Required.",
        ),
    )

    # Workers per pod
    workers_per_pod: int = dataclasses.field(
        default=1, metadata=dict(help="Number of scaler worker processes launched inside each Pod. Must be >= 1.")
    )

    # Python worker environment (reuses the shared config used by OCI / ORB managers)
    python_worker_environment: PythonWorkerEnvironmentConfig = dataclasses.field(
        default_factory=PythonWorkerEnvironmentConfig
    )

    pod_template: str = dataclasses.field(
        default="",
        metadata=dict(
            help=(
                "TOML multi-line string containing a partial Kubernetes pod template in YAML format. "
                "Parsed with yaml.safe_load and deep-merged into the generated pod dict. "
                "Explicit config fields (node_selector, resource_requests, etc.) override values from "
                "this template. Use this for advanced pod configuration not covered by explicit fields."
            )
        ),
    )

    node_selector: Dict[str, str] = dataclasses.field(
        default_factory=dict,
        metadata=dict(type=None, help="Node selector labels for pod scheduling (e.g. {nodepool = 'compute'})."),
    )
    service_account_name: str = dataclasses.field(
        default="", metadata=dict(help="Kubernetes service account name for worker Pods.")
    )
    image_pull_policy: str = dataclasses.field(
        default="",
        metadata=dict(help="Image pull policy for the worker container ('Always', 'Never', or 'IfNotPresent')."),
    )
    resource_requests: Dict[str, str] = dataclasses.field(
        default_factory=dict,
        metadata=dict(
            type=None, help="Kubernetes resource requests for the worker container (e.g. {cpu = '2', memory = '8Gi'})."
        ),
    )
    resource_limits: Dict[str, str] = dataclasses.field(
        default_factory=dict,
        metadata=dict(
            type=None, help="Kubernetes resource limits for the worker container (e.g. {cpu = '4', memory = '16Gi'})."
        ),
    )

    # Grace period on pod deletion
    delete_grace_period_seconds: int = dataclasses.field(
        default=30, metadata=dict(help="Grace period in seconds given to a Pod during deletion. Must be >= 0.")
    )

    def __post_init__(self) -> None:
        if not self.pod_image or not self.pod_image.strip():
            raise ValueError("pod_image cannot be empty or whitespace.")
        if self.workers_per_pod < 1:
            raise ValueError("workers_per_pod must be >= 1.")
        if self.delete_grace_period_seconds < 0:
            raise ValueError("delete_grace_period_seconds must be >= 0.")
