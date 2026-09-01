"""
AWS Batch Execution Backend.

Handles task execution via AWS Batch, including array job batching, S3 payload
storage, job monitoring, and result fetching.
"""

import asyncio
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import cloudpickle

from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import TaskID
from scaler.worker_manager_adapter.mixins import ExecutionBackend, TaskDeserializer, TaskInputLoader

logger = logging.getLogger(__name__)

ARRAY_JOB_BATCH_WINDOW_SECONDS: float = 0.5
ARRAY_JOB_MIN_BATCH_SIZE: int = 2
ARRAY_JOB_MAX_BATCH_SIZE: int = 10000


class AWSBatchExecutionBackend(TaskInputLoader, ExecutionBackend):
    _loader: TaskDeserializer

    def __init__(
        self,
        job_queue: str,
        job_definition: str,
        aws_region: str,
        s3_bucket: str,
        s3_prefix: str = "scaler-tasks",
        job_timeout_seconds: int = 3600,
    ) -> None:
        self._job_queue = job_queue
        self._job_definition = job_definition
        self._aws_region = aws_region
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._job_timeout_seconds = job_timeout_seconds

        self._task_id_to_batch_job_id: dict[TaskID, str] = dict()

        self._batch_client: Any = None
        self._s3_client: Any = None

        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="aws-hpc")

        self._batch_pending: list[tuple[Task, Any, list[Any], Future]] = []
        self._batch_window_start: float = 0.0

    def register(self, load_task_inputs: TaskDeserializer) -> None:
        self._loader = load_task_inputs
        self._initialize_aws_clients()

    async def load_task_inputs(self, task: Task) -> tuple[Any, list[Any]]:
        return await self._loader(task)

    def _initialize_aws_clients(self) -> None:
        import boto3

        session = boto3.Session(region_name=self._aws_region)
        self._batch_client = session.client("batch")
        self._s3_client = session.client("s3")
        logger.info(f"AWS HPC task manager initialized: region={self._aws_region}, queue={self._job_queue}")

    async def routine(self) -> None:
        """Flush pending batch when the collection window has expired."""
        if not self._batch_pending:
            return

        elapsed = time.monotonic() - self._batch_window_start
        if elapsed >= ARRAY_JOB_BATCH_WINDOW_SECONDS:
            await self._flush_pending_batch()

    def on_cleanup(self, task_id: TaskID) -> None:
        self._task_id_to_batch_job_id.pop(task_id, None)

    async def on_cancel(self, task_cancel: TaskCancel) -> None:
        if task_cancel.taskId in self._task_id_to_batch_job_id:
            batch_job_id = self._task_id_to_batch_job_id[task_cancel.taskId]
            await self._cancel_batch_job(batch_job_id)

    async def execute(self, task: Task) -> asyncio.Future:
        function, arg_objects = await self.load_task_inputs(task)

        future: Future = Future()
        future.set_running_or_notify_cancel()

        self._batch_pending.append((task, function, arg_objects, future))
        if len(self._batch_pending) == 1:
            self._batch_window_start = time.monotonic()

        if len(self._batch_pending) >= ARRAY_JOB_MAX_BATCH_SIZE:
            await self._flush_pending_batch()

        return asyncio.wrap_future(future)

    async def _flush_pending_batch(self) -> None:
        if not self._batch_pending:
            return

        pending = self._batch_pending[:]
        self._batch_pending.clear()

        batch = [(t, f, a) for t, f, a, _ in pending]
        futures_map: dict[TaskID, Future] = {t.taskId: fut for t, _, _, fut in pending}

        await self._flush_batch(batch, futures_map)

    async def _flush_batch(self, batch: list[tuple[Task, Any, list[Any]]], futures_map: dict[TaskID, Future]) -> None:
        try:
            if len(batch) >= ARRAY_JOB_MIN_BATCH_SIZE:
                await self._submit_array_job(batch, futures_map)
            else:
                single_task, single_func, single_args = batch[0]
                single_future = futures_map[single_task.taskId]
                batch_job_id = await self._submit_single_batch_job(single_task, single_func, single_args)
                self._task_id_to_batch_job_id[single_task.taskId] = batch_job_id
                logger.info(f"Task {single_task.taskId.hex()[:8]} submitted as Batch job {batch_job_id}")
                asyncio.create_task(self._monitor_batch_job(batch_job_id, single_future, single_task.taskId))
        except Exception as e:
            logger.exception(f"Failed to submit batch of {len(batch)} tasks: {e}")
            for f in futures_map.values():
                if not f.done():
                    f.set_exception(e)

    async def _run_in_executor(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking AWS SDK call in the thread pool to avoid starving the event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))

    async def _submit_array_job(
        self, batch: list[tuple[Task, Any, list[Any]]], futures_map: dict[TaskID, Future]
    ) -> None:
        """Submit multiple tasks as a single AWS Batch array job and monitor child results."""
        import gzip
        import re
        import uuid

        from botocore.exceptions import ClientError

        array_id = uuid.uuid4().hex[:12]
        array_size = len(batch)

        s3_prefix_array = f"{self._s3_prefix}/array/{array_id}"
        index_to_task_id: dict[int, TaskID] = {}

        for index, (task, function, arguments) in enumerate(batch):
            task_data = {
                "task_id": task.taskId.hex(),
                "source": task.source.hex(),
                "function": function,
                "arguments": arguments,
            }
            payload = cloudpickle.dumps(task_data)
            if len(payload) > 4 * 1024:
                payload = gzip.compress(payload)

            s3_key = f"{s3_prefix_array}/{index}.pkl"
            await self._run_in_executor(self._s3_client.put_object, Bucket=self._s3_bucket, Key=s3_key, Body=payload)
            index_to_task_id[index] = task.taskId

        _, first_func, _ = batch[0]
        func_name = getattr(first_func, "__name__", "unknown")
        safe_func_name = re.sub(r"[^a-zA-Z0-9_-]", "_", func_name)[:50]
        job_name = f"{safe_func_name}-array-{array_id}"

        submit_kwargs: dict[str, Any] = dict(
            jobName=job_name,
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            arrayProperties={"size": array_size},
            parameters={
                "task_id": "array",
                "payload": "none",
                "compressed": "0",
                "s3_bucket": self._s3_bucket,
                "s3_prefix": self._s3_prefix,
                "s3_key": f"{s3_prefix_array}/ARRAY_INDEX.pkl",
            },
            timeout={"attemptDurationSeconds": self._job_timeout_seconds},
        )

        try:
            response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
        except ClientError as e:
            if "ExpiredToken" in str(e) or "expired" in str(e).lower():
                logger.warning("AWS credentials expired, refreshing...")
                self._initialize_aws_clients()
                response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
            else:
                raise

        parent_job_id = response["jobId"]
        logger.info(
            f"Submitted array job {parent_job_id} with {array_size} tasks "
            f"(s3://{self._s3_bucket}/{s3_prefix_array}/)"
        )

        for index, task_id in index_to_task_id.items():
            child_job_id = f"{parent_job_id}:{index}"
            self._task_id_to_batch_job_id[task_id] = child_job_id

        asyncio.create_task(self._monitor_array_job(parent_job_id, index_to_task_id, futures_map, s3_prefix_array))

    async def _monitor_array_job(
        self,
        parent_job_id: str,
        index_to_task_id: dict[int, TaskID],
        futures_map: dict[TaskID, Future],
        s3_prefix_array: str,
    ) -> None:
        """Poll child job statuses and resolve each task future as jobs complete or fail."""
        import gzip

        poll_interval_seconds = 3.0
        resolved: set[int] = set()
        total = len(index_to_task_id)

        while len(resolved) < total:
            await asyncio.sleep(poll_interval_seconds)

            try:
                unresolved_indices = [i for i in index_to_task_id if i not in resolved]
                for batch_start in range(0, len(unresolved_indices), 100):
                    batch_indices = unresolved_indices[batch_start : batch_start + 100]
                    child_job_ids = [f"{parent_job_id}:{i}" for i in batch_indices]

                    response = await self._run_in_executor(self._batch_client.describe_jobs, jobs=child_job_ids)
                    for job in response.get("jobs", []):
                        child_id = job["jobId"]
                        index = int(child_id.split(":")[-1])
                        status = job["status"]
                        task_id = index_to_task_id[index]
                        future = futures_map.get(task_id)
                        logger.debug(f"Array child {child_id}: status={status}, index={index}")

                        if future is None or future.done():
                            resolved.add(index)
                            continue

                        if status == "SUCCEEDED":
                            result_key = f"{self._s3_prefix}/results/{parent_job_id}:{index}.pkl"
                            try:
                                s3_resp = None
                                for attempt in range(5):
                                    for key_candidate in [
                                        result_key,
                                        result_key + ".gz",
                                        f"{self._s3_prefix}/results/{child_id}.pkl",
                                        f"{self._s3_prefix}/results/{child_id}.pkl.gz",
                                    ]:
                                        try:
                                            s3_resp = await self._run_in_executor(
                                                self._s3_client.get_object, Bucket=self._s3_bucket, Key=key_candidate
                                            )
                                            result_key = key_candidate
                                            break
                                        except self._s3_client.exceptions.NoSuchKey:
                                            continue
                                    if s3_resp is not None:
                                        break
                                    await asyncio.sleep(3)

                                if s3_resp is None:
                                    raise RuntimeError(f"Result not found for index {index} after 5 retries")
                                result_bytes = s3_resp["Body"].read()
                                if len(result_bytes) >= 2 and result_bytes[0:2] == b"\x1f\x8b":
                                    result_bytes = gzip.decompress(result_bytes)
                                result = cloudpickle.loads(result_bytes)
                                future.set_result(result)
                                await self._run_in_executor(
                                    self._s3_client.delete_object, Bucket=self._s3_bucket, Key=result_key
                                )
                            except Exception as e:
                                future.set_exception(RuntimeError(f"Failed to fetch result for index {index}: {e}"))
                            resolved.add(index)

                        elif status == "FAILED":
                            reason = job.get("statusReason", "Unknown failure")
                            future.set_exception(RuntimeError(f"Array job child {index} failed: {reason}"))
                            resolved.add(index)

            except Exception as e:
                logger.exception(f"Error monitoring array job {parent_job_id}: {e}")

        try:
            for index in index_to_task_id:
                await self._run_in_executor(
                    self._s3_client.delete_object, Bucket=self._s3_bucket, Key=f"{s3_prefix_array}/{index}.pkl"
                )
        except Exception as e:
            logger.warning(f"Failed to cleanup array payloads: {e}")

    async def _submit_single_batch_job(self, task: Task, function: Any, arguments: list[Any]) -> str:
        import base64
        import gzip
        import re

        from botocore.exceptions import ClientError

        task_id_hex = task.taskId.hex()
        func_name = getattr(function, "__name__", "unknown")

        task_data = {"task_id": task_id_hex, "source": task.source.hex(), "function": function, "arguments": arguments}

        payload = cloudpickle.dumps(task_data)
        payload_size = len(payload)

        compressed = False
        if payload_size > 4 * 1024:
            payload = gzip.compress(payload)
            compressed = True

        safe_func_name = re.sub(r"[^a-zA-Z0-9_-]", "_", func_name)[:50]
        job_name = f"{safe_func_name}-{task_id_hex[:12]}"

        max_inline_size = 28 * 1024

        if len(payload) <= max_inline_size:
            encoded_payload = base64.b64encode(payload).decode("ascii")
            s3_key = "none"
        else:
            s3_key = f"{self._s3_prefix}/inputs/{task_id_hex}.pkl"
            if compressed:
                s3_key += ".gz"
            await self._run_in_executor(self._s3_client.put_object, Bucket=self._s3_bucket, Key=s3_key, Body=payload)
            encoded_payload = ""

        submit_kwargs: dict[str, Any] = dict(
            jobName=job_name,
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            parameters={
                "task_id": task_id_hex,
                "payload": encoded_payload,
                "compressed": "1" if compressed else "0",
                "s3_bucket": self._s3_bucket,
                "s3_prefix": self._s3_prefix,
                "s3_key": s3_key,
            },
            timeout={"attemptDurationSeconds": self._job_timeout_seconds},
        )

        try:
            response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
        except ClientError as e:
            if "ExpiredToken" in str(e) or "expired" in str(e).lower():
                logger.warning("AWS credentials expired, refreshing...")
                self._initialize_aws_clients()
                response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
            else:
                raise

        return response["jobId"]

    async def _monitor_batch_job(self, job_id: str, future: Future, task_id: TaskID) -> None:
        """Poll a single Batch job until it succeeds or fails, then resolve the future."""
        import gzip

        poll_interval_seconds = 2.0

        while True:
            await asyncio.sleep(poll_interval_seconds)

            try:
                response = await self._run_in_executor(self._batch_client.describe_jobs, jobs=[job_id])
                if not response.get("jobs"):
                    continue

                job = response["jobs"][0]
                status = job["status"]

                if status == "SUCCEEDED":
                    result_key = f"{self._s3_prefix}/results/{job_id}.pkl"
                    try:
                        try:
                            response = await self._run_in_executor(
                                self._s3_client.get_object, Bucket=self._s3_bucket, Key=result_key
                            )
                        except self._s3_client.exceptions.NoSuchKey:
                            result_key += ".gz"
                            response = await self._run_in_executor(
                                self._s3_client.get_object, Bucket=self._s3_bucket, Key=result_key
                            )
                        result_bytes = response["Body"].read()

                        if len(result_bytes) >= 2 and result_bytes[0:2] == b"\x1f\x8b":
                            result_bytes = gzip.decompress(result_bytes)

                        result = cloudpickle.loads(result_bytes)
                        future.set_result(result)

                        await self._run_in_executor(
                            self._s3_client.delete_object, Bucket=self._s3_bucket, Key=result_key
                        )
                    except Exception as e:
                        future.set_exception(RuntimeError(f"Failed to fetch result: {e}"))
                    return

                elif status == "FAILED":
                    reason = job.get("statusReason", "Unknown failure")
                    log_output = await self._fetch_job_logs(job_id)
                    error_msg = f"Batch job failed: {reason}"
                    if log_output:
                        error_msg += f"\n\n{log_output}"
                    future.set_exception(RuntimeError(error_msg))
                    return

                elif status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
                    continue
                else:
                    logger.warning(f"Unknown job status: {status}")

            except Exception as e:
                logger.exception(f"Error monitoring job {job_id}: {e}")

    async def _cancel_batch_job(self, job_id: str) -> None:
        try:
            await self._run_in_executor(self._batch_client.terminate_job, jobId=job_id, reason="Canceled by Scaler")
            logger.info(f"Canceled Batch job {job_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel Batch job {job_id}: {e}")

    async def _fetch_job_logs(self, job_id: str) -> str:
        try:
            import boto3

            logs_client = boto3.client("logs", region_name=self._aws_region)

            log_group = "/aws/batch/job"

            job_response = await self._run_in_executor(self._batch_client.describe_jobs, jobs=[job_id])
            if not job_response.get("jobs"):
                return "(Job not found)"

            job = job_response["jobs"][0]
            container = job.get("container", {})
            log_stream = container.get("logStreamName", "")

            debug_info = [
                f"Job status: {job.get('status')}",
                f"Status reason: {job.get('statusReason', 'N/A')}",
                f"Container exit code: {container.get('exitCode', 'N/A')}",
                f"Container reason: {container.get('reason', 'N/A')}",
            ]

            if not log_stream:
                return "Job debug info:\n" + "\n".join(debug_info) + "\n\n(Could not determine log stream name)"

            await asyncio.sleep(2)

            try:
                response = await self._run_in_executor(
                    logs_client.get_log_events,
                    logGroupName=log_group,
                    logStreamName=log_stream,
                    limit=100,
                    startFromHead=True,
                )

                events = response.get("events", [])
                if not events:
                    return "Job debug info:\n" + "\n".join(debug_info) + "\n\n(No log events found)"

                log_lines = [event.get("message", "") for event in events]
                return "Job debug info:\n" + "\n".join(debug_info) + "\n\nContainer logs:\n" + "\n".join(log_lines)

            except logs_client.exceptions.ResourceNotFoundException:
                return "Job debug info:\n" + "\n".join(debug_info) + f"\n\n(Log stream not found: {log_stream})"

        except Exception as e:
            logger.warning(f"Failed to fetch logs for job {job_id}: {e}")
            return f"(Failed to fetch logs: {e})"
