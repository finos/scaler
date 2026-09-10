"""
OCI Worker Manager for OpenGRIS Scaler.

Submits each Scaler task as an on-demand OCI Container Instance and reports
results back to the scheduler via the WorkerProcess pattern.

Architecture:
    Scheduler → WorkerManagerRunner → OCIWorkerProvisioner → WorkerProcess
                                                                      ↓
                                                          OCIExecutionBackend
                                                                      ↓
                                                          OCI Container Instances

Service Mapping (AWS → OCI):
    - AWS Batch          → OCI Container Instances
    - Amazon S3          → OCI Object Storage
    - Amazon ECR         → OCI Container Registry (OCIR)
    - Amazon CloudWatch  → OCI Logging
    - AWS IAM Role       → OCI Dynamic Group + IAM Policies
"""

from scaler.worker_manager.proxy.oci.execution_backend import OCIExecutionBackend
from scaler.worker_manager.proxy.oci.processor_status import OCIProcessorStatusProvider
from scaler.worker_manager.proxy.oci.worker import create_oci_worker
from scaler.worker_manager.proxy.oci.worker_manager import OCIWorkerManager, OCIWorkerProvisioner

__all__ = [
    "OCIExecutionBackend",
    "OCIWorkerManager",
    "OCIWorkerProvisioner",
    "OCIProcessorStatusProvider",
    "create_oci_worker",
]
