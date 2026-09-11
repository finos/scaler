import asyncio
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import cloudpickle
from bidict import bidict

from scaler import Serializer
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import (
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
)
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager
from scaler.worker.agent.mixins import TaskManager as TaskManagerMixin
from scaler.worker_manager.proxy.mixins import ExecutionBackend

logger = logging.getLogger(__name__)


class TaskManager(Looper, TaskManagerMixin):
    def __init__(
        self, base_concurrency: int, execution_backend: ExecutionBackend, idle_sleep_seconds: float = 0.0
    ) -> None:
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a positive integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._execution_backend = execution_backend
        self._idle_sleep_seconds = idle_sleep_seconds

        self._executor_semaphore = asyncio.Semaphore(value=self._base_concurrency)

        self._task_id_to_task: Dict[TaskID, Task] = dict()
        self._task_id_to_future: bidict[TaskID, asyncio.Future] = bidict()

        self._serializers: Dict[bytes, Serializer] = dict()

        self._queued_task_id_queue = AsyncPriorityQueue()
        self._queued_task_ids: Set[TaskID] = set()

        self._acquiring_task_ids: Set[TaskID] = set()
        self._processing_task_ids: Set[TaskID] = set()
        self._canceled_task_ids: Set[TaskID] = set()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._heartbeat_manager: Optional[HeartbeatManager] = None

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        heartbeat_manager: HeartbeatManager,
    ) -> None:
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._heartbeat_manager = heartbeat_manager
        self._execution_backend.register(self.load_task_inputs)

    async def on_object_instruction(self, instruction: ObjectInstruction) -> None:
        if instruction.instructionType == ObjectInstruction.ObjectInstructionType.delete:
            for object_id in instruction.objectMetadata.objectIds:
                self._serializers.pop(object_id, None)
            return

        logger.error(f"worker received unknown object instruction type {instruction=}")

    async def on_task_new(self, task: Task) -> None:
        task_priority = self._get_task_priority(task)

        if self._executor_semaphore.locked():
            for acquired_task_id in self._acquiring_task_ids:
                acquired_task = self._task_id_to_task[acquired_task_id]
                acquired_task_priority = self._get_task_priority(acquired_task)
                if task_priority <= acquired_task_priority:
                    break
            else:
                self._task_id_to_task[task.taskId] = task
                self._processing_task_ids.add(task.taskId)
                # Bypass tasks intentionally exceed base_concurrency to service higher-priority requests immediately.
                self._task_id_to_future[task.taskId] = await self._execution_backend.execute(task)
                return

        self._task_id_to_task[task.taskId] = task
        self._queued_task_id_queue.put_nowait((-task_priority, task.taskId))
        self._queued_task_ids.add(task.taskId)

    async def on_cancel_task(self, task_cancel: TaskCancel) -> None:
        task_queued = task_cancel.taskId in self._queued_task_ids
        task_processing = task_cancel.taskId in self._processing_task_ids

        if not task_queued and not task_processing:
            await self._connector_external.send(
                TaskCancelConfirm(taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelNotFound),
                detached=True,
            )
            return

        if task_processing and not task_cancel.flags.force:
            await self._connector_external.send(
                TaskCancelConfirm(taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.cancelFailed),
                detached=True,
            )
            return

        if task_queued:
            self._queued_task_ids.remove(task_cancel.taskId)
            self._queued_task_id_queue.remove(task_cancel.taskId)
            self._task_id_to_task.pop(task_cancel.taskId)

        if task_processing:
            future = self._task_id_to_future[task_cancel.taskId]
            future.cancel()
            await self._execution_backend.on_cancel(task_cancel)
            self._processing_task_ids.remove(task_cancel.taskId)
            self._canceled_task_ids.add(task_cancel.taskId)

        await self._connector_external.send(
            TaskCancelConfirm(taskId=task_cancel.taskId, cancelConfirmType=TaskCancelConfirmType.canceled),
            detached=True,
        )

    async def on_task_result(self, result: TaskResult) -> None:
        # Required by TaskManagerMixin but not dispatched from WorkerProcess.__on_receive_external.
        # WorkerProcess drives result handling via resolve_tasks() instead.
        # NOTE: _queued_task_ids is not cleaned up by resolve_tasks(), so completed tasks whose IDs
        # are still in _queued_task_ids will be cleared here if this method is ever called, but in
        # normal operation those IDs accumulate until the WorkerProcess exits.
        if result.taskId in self._queued_task_ids:
            self._queued_task_ids.remove(result.taskId)
            self._queued_task_id_queue.remove(result.taskId)

        self._processing_task_ids.remove(result.taskId)
        self._task_id_to_task.pop(result.taskId)

        await self._connector_external.send(result, detached=True)

    def get_queued_size(self) -> int:
        return self._queued_task_id_queue.qsize()

    def can_accept_task(self) -> bool:
        return not self._executor_semaphore.locked()

    async def resolve_tasks(self) -> None:
        if not self._task_id_to_future:
            await asyncio.sleep(self._idle_sleep_seconds)
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            task = self._task_id_to_task.get(task_id)

            if task is None:
                logger.warning(f"Cannot find task in worker queue: task_id={task_id.hex()}")
                continue

            if task_id in self._processing_task_ids:
                self._processing_task_ids.remove(task_id)

                if future.exception() is None:
                    serializer_id = ObjectID.generate_serializer_object_id(task.source)
                    serializer = self._serializers[serializer_id]
                    result_bytes = serializer.serialize(future.result())
                    result_type = TaskResultType.success
                else:
                    result_bytes = serialize_failure(cast(Exception, future.exception()))
                    result_type = TaskResultType.failed

                result_object_id = ObjectID.generate_object_id(task.source)

                await self._connector_storage.set_object(result_object_id, result_bytes)
                await self._connector_external.send(
                    ObjectInstruction(
                        instructionType=ObjectInstruction.ObjectInstructionType.create,
                        objectUser=task.source,
                        objectMetadata=ObjectMetadata(
                            objectIds=(result_object_id,),
                            objectTypes=(ObjectMetadata.ObjectContentType.object,),
                            objectNames=(f"<res {result_object_id.hex()[:6]}>".encode(),),
                        ),
                    ),
                    detached=True,
                )

                await self._connector_external.send(
                    TaskResult(taskId=task_id, resultType=result_type, metadata=b"", results=[bytes(result_object_id)]),
                    detached=True,
                )

            elif task_id in self._canceled_task_ids:
                self._canceled_task_ids.remove(task_id)

            else:
                raise ValueError(f"task_id {task_id.hex()} not found in processing or canceled tasks")

            if task_id in self._acquiring_task_ids:
                self._acquiring_task_ids.remove(task_id)
                self._executor_semaphore.release()

            self._task_id_to_task.pop(task_id)
            self._execution_backend.on_cleanup(task_id)

    async def routine(self) -> None:
        pass

    async def process_task(self) -> None:
        await self._executor_semaphore.acquire()

        _, task_id = await self._queued_task_id_queue.get()
        task = self._task_id_to_task[task_id]

        self._acquiring_task_ids.add(task_id)
        self._processing_task_ids.add(task_id)
        # _queued_task_ids intentionally not cleared here; on_cancel_task and on_task_result clear it.
        self._task_id_to_future[task.taskId] = await self._execution_backend.execute(task)

    @property
    def processing_task_count(self) -> int:
        return len(self._processing_task_ids)

    async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]:
        serializer_id = ObjectID.generate_serializer_object_id(task.source)

        if serializer_id not in self._serializers:
            serializer_bytes = await self._connector_storage.get_object(serializer_id)
            serializer = cloudpickle.loads(serializer_bytes)
            self._serializers[serializer_id] = serializer
        else:
            serializer = self._serializers[serializer_id]

        get_tasks = [
            self._connector_storage.get_object(object_id)
            for object_id in [ObjectID(task.funcObjectId), *(ObjectID(argument.data) for argument in task.functionArgs)]
        ]

        function_bytes, *arg_bytes = await asyncio.gather(*get_tasks)

        function = serializer.deserialize(function_bytes)
        arg_objects = [serializer.deserialize(object_bytes) for object_bytes in arg_bytes]
        return function, arg_objects

    @staticmethod
    def _get_task_priority(task: Task) -> int:
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
