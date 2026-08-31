from typing import Union

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.kubernetes_worker_manager import KubernetesWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig

WorkerManagerUnion = Union[
    NativeWorkerManagerConfig,
    SymphonyWorkerManagerConfig,
    ECSWorkerManagerConfig,
    AWSBatchWorkerManagerConfig,
    ORBAWSEC2WorkerManagerConfig,
    OCIRawWorkerManagerConfig,
    OCIHPCWorkerManagerConfig,
    KubernetesWorkerManagerConfig,
]
