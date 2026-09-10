"""
AWS Batch Worker Manager for OpenGRIS Scaler.

Receives tasks from the scheduler and submits them as AWS Batch jobs.

Architecture (composition pattern):
    Scheduler Stream -> WorkerProcess -> AWSBatchExecutionBackend -> AWS Batch Jobs
                            |
                            v
                    Heartbeats to Scheduler
                            |
                            v
                Poll Results -> TaskResult to Scheduler

Components:
    - WorkerProcess: Process connecting to scheduler stream
    - AWSBatchExecutionBackend: Handles task queuing, priority, and AWS Batch submission
    - AWSBatchProcessorStatusProvider: Provides processor status for heartbeats
    - BatchJobCallback: Tracks task->job mappings
    - remote/job_runner.py: Script running inside AWS Batch containers
"""

from scaler.worker_manager.proxy.aws_batch.callback import BatchJobCallback
from scaler.worker_manager.proxy.aws_batch.execution_backend import AWSBatchExecutionBackend
from scaler.worker_manager.proxy.aws_batch.processor_status import AWSBatchProcessorStatusProvider
from scaler.worker_manager.proxy.aws_batch.worker import create_aws_batch_worker

__all__ = ["create_aws_batch_worker", "AWSBatchExecutionBackend", "AWSBatchProcessorStatusProvider", "BatchJobCallback"]
