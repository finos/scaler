"""
AWS Batch Job Callback Handler.

Manages the mapping between task IDs and AWS Batch job futures,
handling job completion and failure callbacks.
"""

import concurrent.futures
import logging
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BatchJobCallback:
    """
    Callback handler for AWS Batch job completions.

    Similar to Symphony's SessionCallback but adapted for AWS Batch's
    polling-based job status model.
    """

    def __init__(self) -> None:
        self._callback_lock = threading.Lock()
        self._task_id_to_future: Dict[str, concurrent.futures.Future] = {}
        self._task_id_to_batch_job_id: Dict[str, str] = {}
        self._batch_job_id_to_task_id: Dict[str, str] = {}

    def on_job_succeeded(self, batch_job_id: str, result: Any) -> None:
        """
        Handle successful job completion.

        Args:
            batch_job_id: AWS Batch job ID
            result: Deserialized result from job output
        """
        with self._callback_lock:
            task_id = self._batch_job_id_to_task_id.get(batch_job_id)
            if task_id is None:
                logger.warning(f"Received result for unknown batch job: {batch_job_id}")
                return

            future = self._task_id_to_future.pop(task_id, None)
            if future is None:
                logger.warning(f"No future found for task: {task_id}")
                return

            self._cleanup_job_mapping(task_id, batch_job_id)

            if not future.done():
                future.set_result(result)

    def on_job_failed(self, batch_job_id: str, exception: Exception) -> None:
        """
        Handle job failure.

        Args:
            batch_job_id: AWS Batch job ID
            exception: Exception that caused the failure
        """
        with self._callback_lock:
            task_id = self._batch_job_id_to_task_id.get(batch_job_id)
            if task_id is None:
                logger.warning(f"Received failure for unknown batch job: {batch_job_id}")
                return

            future = self._task_id_to_future.pop(task_id, None)
            if future is None:
                logger.warning(f"No future found for task: {task_id}")
                return

            self._cleanup_job_mapping(task_id, batch_job_id)

            if not future.done():
                future.set_exception(exception)

    def on_exception(self, exception: Exception) -> None:
        """
        Handle global exception affecting all pending tasks.

        Args:
            exception: Exception to propagate to all futures
        """
        with self._callback_lock:
            for task_id, future in list(self._task_id_to_future.items()):
                if not future.done():
                    future.set_exception(exception)

            self._task_id_to_future.clear()
            self._task_id_to_batch_job_id.clear()
            self._batch_job_id_to_task_id.clear()

    def submit_task(self, task_id: str, batch_job_id: str, future: concurrent.futures.Future) -> None:
        """
        Register a task submission for callback tracking.

        Args:
            task_id: Scaler task ID
            batch_job_id: AWS Batch job ID
            future: Future to resolve when job completes
        """
        with self._callback_lock:
            self._task_id_to_future[task_id] = future
            self._task_id_to_batch_job_id[task_id] = batch_job_id
            self._batch_job_id_to_task_id[batch_job_id] = task_id

    def cancel_task(self, task_id: str) -> Optional[str]:
        """
        Cancel a task and return its batch job ID for termination.

        Args:
            task_id: Scaler task ID to cancel

        Returns:
            AWS Batch job ID if found, None otherwise
        """
        with self._callback_lock:
            future = self._task_id_to_future.pop(task_id, None)
            batch_job_id = self._task_id_to_batch_job_id.pop(task_id, None)

            if batch_job_id:
                self._batch_job_id_to_task_id.pop(batch_job_id, None)

            if future and not future.done():
                future.cancel()

            return batch_job_id

    def get_batch_job_id(self, task_id: str) -> Optional[str]:
        """Get the AWS Batch job ID for a task."""
        with self._callback_lock:
            return self._task_id_to_batch_job_id.get(task_id)

    def get_pending_job_ids(self) -> List[str]:
        """Get all pending AWS Batch job IDs."""
        with self._callback_lock:
            return list(self._batch_job_id_to_task_id.keys())

    def get_callback_lock(self) -> threading.Lock:
        """Get the callback lock for external synchronization."""
        return self._callback_lock

    def _cleanup_job_mapping(self, task_id: str, batch_job_id: str) -> None:
        """Clean up internal mappings after job completion."""
        self._task_id_to_batch_job_id.pop(task_id, None)
        self._batch_job_id_to_task_id.pop(batch_job_id, None)
