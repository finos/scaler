import asyncio
import unittest
from typing import Any, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import (
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
)
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker.agent.mixins import HeartbeatManager
from scaler.worker_manager.proxy.mixins import ExecutionBackend, TaskDeserializer, TaskInputLoader
from scaler.worker_manager.proxy.task_manager import TaskManager
from tests.utility.utility import logging_test_name


def _make_task(priority: int = 0, source: Optional[ClientID] = None, task_id: Optional[TaskID] = None) -> Task:
    source = source or ClientID.generate_client_id()
    task_id = task_id or TaskID.generate_task_id()
    return Task(
        taskId=task_id,
        source=source,
        metadata=TaskFlags(priority=priority).serialize(),
        funcObjectId=ObjectID.generate_object_id(source),
        functionArgs=[],
        capabilities={},
    )


def _make_task_cancel(task_id: TaskID, force: bool = False) -> TaskCancel:
    return TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=force))


def _make_backend() -> MagicMock:
    backend = MagicMock(spec=ExecutionBackend)
    backend.execute = AsyncMock()
    backend.on_cancel = AsyncMock()
    backend.register = MagicMock()
    backend.on_cleanup = MagicMock()
    return backend


class TestTaskManagerInit(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()

    def test_negative_concurrency_raises(self) -> None:
        with self.assertRaises(ValueError):
            TaskManager(0, self.backend)
        with self.assertRaises(ValueError):
            TaskManager(-1, self.backend)

    def test_idle_sleep_defaults_to_zero(self) -> None:
        tm = TaskManager(1, self.backend)
        self.assertEqual(tm._idle_sleep_seconds, 0.0)

    def test_idle_sleep_custom_value_stored(self) -> None:
        tm = TaskManager(1, self.backend, idle_sleep_seconds=0.1)
        self.assertEqual(tm._idle_sleep_seconds, 0.1)

    def test_valid_concurrency_empty_state(self) -> None:
        tm = TaskManager(2, self.backend)
        self.assertEqual(len(tm._task_id_to_task), 0)
        self.assertEqual(len(tm._task_id_to_future), 0)
        self.assertEqual(len(tm._serializers), 0)
        self.assertEqual(tm._queued_task_ids, set())
        self.assertEqual(tm._processing_task_ids, set())
        self.assertEqual(tm._acquiring_task_ids, set())
        self.assertEqual(tm._canceled_task_ids, set())
        self.assertEqual(tm.get_queued_size(), 0)

    def test_register_calls_backend_register_with_callable(self) -> None:
        tm = TaskManager(1, self.backend)
        connector_external = AsyncMock(spec=AsyncConnector)
        connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        heartbeat_manager = MagicMock(spec=HeartbeatManager)
        tm.register(connector_external, connector_storage, heartbeat_manager)
        self.backend.register.assert_called_once()
        registered_callable = self.backend.register.call_args[0][0]
        self.assertTrue(callable(registered_callable))


class TestTaskManagerOnTaskNew(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def test_task_queued_when_semaphore_available(self) -> None:
        task = _make_task()
        await self.tm.on_task_new(task)
        self.assertIn(task.taskId, self.tm._queued_task_ids)
        self.assertIn(task.taskId, self.tm._task_id_to_task)
        self.assertNotIn(task.taskId, self.tm._processing_task_ids)
        self.backend.execute.assert_not_called()

    async def test_queued_task_appears_in_priority_queue(self) -> None:
        await self.tm.on_task_new(_make_task())
        self.assertEqual(self.tm.get_queued_size(), 1)

    async def test_priority_bypass_strictly_higher(self) -> None:
        acquiring_task = _make_task(priority=1)
        await self.tm._executor_semaphore.acquire()
        self.tm._task_id_to_task[acquiring_task.taskId] = acquiring_task
        self.tm._acquiring_task_ids.add(acquiring_task.taskId)

        new_task = _make_task(priority=5)
        fut = asyncio.get_running_loop().create_future()
        self.backend.execute = AsyncMock(return_value=fut)
        await self.tm.on_task_new(new_task)

        self.assertIn(new_task.taskId, self.tm._processing_task_ids)
        self.assertNotIn(new_task.taskId, self.tm._queued_task_ids)
        self.backend.execute.assert_called_once_with(new_task)

    async def test_equal_priority_does_not_bypass(self) -> None:
        acquiring_task = _make_task(priority=3)
        await self.tm._executor_semaphore.acquire()
        self.tm._task_id_to_task[acquiring_task.taskId] = acquiring_task
        self.tm._acquiring_task_ids.add(acquiring_task.taskId)

        new_task = _make_task(priority=3)
        await self.tm.on_task_new(new_task)

        self.assertIn(new_task.taskId, self.tm._queued_task_ids)
        self.assertNotIn(new_task.taskId, self.tm._processing_task_ids)
        self.backend.execute.assert_not_called()

    async def test_lower_priority_does_not_bypass(self) -> None:
        acquiring_task = _make_task(priority=5)
        await self.tm._executor_semaphore.acquire()
        self.tm._task_id_to_task[acquiring_task.taskId] = acquiring_task
        self.tm._acquiring_task_ids.add(acquiring_task.taskId)

        new_task = _make_task(priority=2)
        await self.tm.on_task_new(new_task)

        self.assertIn(new_task.taskId, self.tm._queued_task_ids)
        self.assertNotIn(new_task.taskId, self.tm._processing_task_ids)
        self.backend.execute.assert_not_called()


class TestTaskManagerOnCancelTask(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def test_cancel_nonexistent_sends_cancel_not_found(self) -> None:
        task_cancel = _make_task_cancel(TaskID.generate_task_id())
        await self.tm.on_cancel_task(task_cancel)
        self.connector_external.send.assert_called_once()
        sent = self.connector_external.send.call_args[0][0]
        self.assertEqual(sent.cancelConfirmType, TaskCancelConfirmType.cancelNotFound)
        self.assertEqual(sent.taskId, task_cancel.taskId)

    async def test_cancel_processing_without_force_sends_cancel_failed(self) -> None:
        task = _make_task()
        fut = asyncio.get_running_loop().create_future()
        self.tm._task_id_to_task[task.taskId] = task
        self.tm._processing_task_ids.add(task.taskId)
        self.tm._task_id_to_future[task.taskId] = fut

        task_cancel = _make_task_cancel(task.taskId, force=False)
        await self.tm.on_cancel_task(task_cancel)

        self.connector_external.send.assert_called_once()
        sent = self.connector_external.send.call_args[0][0]
        self.assertEqual(sent.cancelConfirmType, TaskCancelConfirmType.cancelFailed)
        self.assertIn(task.taskId, self.tm._processing_task_ids)
        self.assertFalse(fut.cancelled())

    async def test_cancel_queued_task_clears_queue_and_sends_canceled(self) -> None:
        task = _make_task()
        await self.tm.on_task_new(task)
        self.connector_external.send.reset_mock()

        task_cancel = _make_task_cancel(task.taskId)
        await self.tm.on_cancel_task(task_cancel)

        self.connector_external.send.assert_called_once()
        sent = self.connector_external.send.call_args[0][0]
        self.assertEqual(sent.cancelConfirmType, TaskCancelConfirmType.canceled)
        self.assertNotIn(task.taskId, self.tm._queued_task_ids)
        self.assertNotIn(task.taskId, self.tm._task_id_to_task)
        self.assertEqual(self.tm.get_queued_size(), 0)

    async def test_cancel_processing_with_force_cancels_future_and_sends_canceled(self) -> None:
        task = _make_task()
        fut = asyncio.get_running_loop().create_future()
        self.tm._task_id_to_task[task.taskId] = task
        self.tm._processing_task_ids.add(task.taskId)
        self.tm._task_id_to_future[task.taskId] = fut

        task_cancel = _make_task_cancel(task.taskId, force=True)
        await self.tm.on_cancel_task(task_cancel)

        self.assertTrue(fut.cancelled())
        self.backend.on_cancel.assert_called_once_with(task_cancel)
        self.assertIn(task.taskId, self.tm._canceled_task_ids)
        self.assertNotIn(task.taskId, self.tm._processing_task_ids)
        self.connector_external.send.assert_called_once()
        sent = self.connector_external.send.call_args[0][0]
        self.assertEqual(sent.cancelConfirmType, TaskCancelConfirmType.canceled)


class TestTaskManagerOnObjectInstruction(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def test_delete_removes_serializer_from_cache(self) -> None:
        client_id = ClientID.generate_client_id()
        obj_id = ObjectID.generate_object_id(client_id)
        self.tm._serializers[obj_id] = MagicMock()
        instruction = ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.delete,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(objectIds=(obj_id,), objectTypes=(), objectNames=()),
        )
        await self.tm.on_object_instruction(instruction)
        self.assertNotIn(obj_id, self.tm._serializers)

    async def test_delete_unknown_id_does_not_raise(self) -> None:
        client_id = ClientID.generate_client_id()
        obj_id = ObjectID.generate_object_id(client_id)
        instruction = ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.delete,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(objectIds=(obj_id,), objectTypes=(), objectNames=()),
        )
        await self.tm.on_object_instruction(instruction)
        self.assertEqual(len(self.tm._serializers), 0)

    async def test_unknown_instruction_type_logs_error(self) -> None:
        client_id = ClientID.generate_client_id()
        obj_id = ObjectID.generate_object_id(client_id)
        instruction = ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.create,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(
                objectIds=(obj_id,), objectTypes=(ObjectMetadata.ObjectContentType.object,), objectNames=(b"name",)
            ),
        )
        with self.assertLogs("scaler", level="ERROR"):
            await self.tm.on_object_instruction(instruction)


class TestTaskManagerGetTaskPriority(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_negative_priority_raises(self) -> None:
        task = _make_task(priority=-1)
        with self.assertRaises(ValueError):
            TaskManager._get_task_priority(task)

    def test_zero_priority_returns_zero(self) -> None:
        self.assertEqual(TaskManager._get_task_priority(_make_task(priority=0)), 0)

    def test_positive_priority_returns_value(self) -> None:
        self.assertEqual(TaskManager._get_task_priority(_make_task(priority=7)), 7)

    def test_empty_metadata_returns_zero(self) -> None:
        source = ClientID.generate_client_id()
        task = Task(
            taskId=TaskID.generate_task_id(),
            source=source,
            metadata=b"",
            funcObjectId=ObjectID.generate_object_id(source),
            functionArgs=[],
            capabilities={},
        )
        self.assertEqual(TaskManager._get_task_priority(task), 0)


class TestTaskManagerCanAcceptTaskAndQueuedSize(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    def test_can_accept_task_true_when_semaphore_free(self) -> None:
        self.assertTrue(self.tm.can_accept_task())

    async def test_can_accept_task_false_when_semaphore_locked(self) -> None:
        await self.tm._executor_semaphore.acquire()
        self.assertFalse(self.tm.can_accept_task())

    async def test_get_queued_size_increments(self) -> None:
        self.assertEqual(self.tm.get_queued_size(), 0)
        await self.tm.on_task_new(_make_task())
        self.assertEqual(self.tm.get_queued_size(), 1)
        await self.tm.on_task_new(_make_task())
        self.assertEqual(self.tm.get_queued_size(), 2)


class TestTaskManagerProcessTask(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def test_process_task_dequeues_and_calls_execute(self) -> None:
        task = _make_task()
        await self.tm.on_task_new(task)

        fut = asyncio.get_running_loop().create_future()
        self.backend.execute = AsyncMock(return_value=fut)
        await self.tm.process_task()

        self.backend.execute.assert_called_once_with(task)
        self.assertIn(task.taskId, self.tm._processing_task_ids)
        self.assertIn(task.taskId, self.tm._acquiring_task_ids)
        self.assertIs(self.tm._task_id_to_future[task.taskId], fut)
        self.assertEqual(self.tm.get_queued_size(), 0)
        self.assertIn(task.taskId, self.tm._queued_task_ids)
        self.assertTrue(self.tm._executor_semaphore.locked())


class TestTaskManagerResolveTasks(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def test_no_futures_returns_without_sending(self) -> None:
        await self.tm.resolve_tasks()
        self.connector_external.send.assert_not_called()

    async def test_success_path_stores_result_and_sends_messages(self) -> None:
        client_id = ClientID.generate_client_id()
        task = _make_task(source=client_id)

        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"serialized_result"
        serializer_id = ObjectID.generate_serializer_object_id(client_id)
        self.tm._serializers[serializer_id] = mock_serializer

        fut = asyncio.get_running_loop().create_future()
        fut.set_result("the_return_value")
        self.tm._task_id_to_future[task.taskId] = fut
        self.tm._task_id_to_task[task.taskId] = task
        self.tm._processing_task_ids.add(task.taskId)
        self.tm._acquiring_task_ids.add(task.taskId)
        await self.tm._executor_semaphore.acquire()

        await self.tm.resolve_tasks()

        mock_serializer.serialize.assert_called_once_with("the_return_value")
        self.connector_storage.set_object.assert_called_once()
        self.assertEqual(self.connector_external.send.call_count, 2)
        obj_instruction = self.connector_external.send.call_args_list[0][0][0]
        self.assertEqual(obj_instruction.instructionType, ObjectInstruction.ObjectInstructionType.create)
        self.assertEqual(obj_instruction.objectUser, client_id)
        task_result = self.connector_external.send.call_args_list[1][0][0]
        self.assertEqual(task_result.resultType, TaskResultType.success)
        self.assertEqual(task_result.taskId, task.taskId)
        self.assertNotIn(task.taskId, self.tm._processing_task_ids)
        self.assertNotIn(task.taskId, self.tm._task_id_to_task)
        self.assertNotIn(task.taskId, self.tm._acquiring_task_ids)
        self.backend.on_cleanup.assert_called_once_with(task.taskId)
        self.assertFalse(self.tm._executor_semaphore.locked())

    async def test_failure_path_sends_failed_task_result(self) -> None:
        client_id = ClientID.generate_client_id()
        task = _make_task(source=client_id)

        fut = asyncio.get_running_loop().create_future()
        fut.set_exception(RuntimeError("boom"))
        self.tm._task_id_to_future[task.taskId] = fut
        self.tm._task_id_to_task[task.taskId] = task
        self.tm._processing_task_ids.add(task.taskId)
        self.tm._acquiring_task_ids.add(task.taskId)
        await self.tm._executor_semaphore.acquire()

        await self.tm.resolve_tasks()

        self.connector_storage.set_object.assert_called_once()
        self.assertEqual(self.connector_external.send.call_count, 2)
        task_result = self.connector_external.send.call_args_list[1][0][0]
        self.assertEqual(task_result.resultType, TaskResultType.failed)
        self.assertEqual(task_result.taskId, task.taskId)
        self.backend.on_cleanup.assert_called_once_with(task.taskId)

    async def test_canceled_path_sends_nothing(self) -> None:
        task = _make_task()

        fut = asyncio.get_running_loop().create_future()
        fut.cancel()
        self.tm._task_id_to_future[task.taskId] = fut
        self.tm._task_id_to_task[task.taskId] = task
        self.tm._canceled_task_ids.add(task.taskId)
        self.tm._acquiring_task_ids.add(task.taskId)
        await self.tm._executor_semaphore.acquire()

        await self.tm.resolve_tasks()

        self.connector_external.send.assert_not_called()
        self.assertNotIn(task.taskId, self.tm._canceled_task_ids)
        self.assertNotIn(task.taskId, self.tm._acquiring_task_ids)
        self.assertNotIn(task.taskId, self.tm._task_id_to_task)
        self.backend.on_cleanup.assert_called_once_with(task.taskId)
        self.assertFalse(self.tm._executor_semaphore.locked())


class TestExecutionBackendSentinel(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_load_task_inputs_after_register_does_not_raise(self) -> None:
        async def _loader(task: Task) -> Tuple[Any, List[Any]]:
            return None, []

        class _ConcreteBackend(TaskInputLoader, ExecutionBackend):
            _loader: TaskDeserializer

            def register(self, load_task_inputs: TaskDeserializer) -> None:
                self._loader = load_task_inputs

            async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]:
                return await self._loader(task)

            async def execute(self, task: Task) -> asyncio.Future:
                return asyncio.get_running_loop().create_future()

            async def on_cancel(self, task_cancel: TaskCancel) -> None:
                pass

            def on_cleanup(self, task_id: TaskID) -> None:
                pass

            async def routine(self) -> None:
                pass

        backend = _ConcreteBackend()
        backend.register(_loader)
        func, args = await backend.load_task_inputs(_make_task())
        self.assertIsNone(func)
        self.assertEqual(args, [])


class TestTaskManagerOnTaskResult(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(1, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def test_forwards_result_and_cleans_up(self) -> None:
        task = _make_task()
        self.tm._task_id_to_task[task.taskId] = task
        self.tm._processing_task_ids.add(task.taskId)
        result = TaskResult(taskId=task.taskId, resultType=TaskResultType.success, metadata=b"", results=[])
        await self.tm.on_task_result(result)

        self.connector_external.send.assert_called_once_with(result, detached=True)
        self.assertNotIn(task.taskId, self.tm._processing_task_ids)
        self.assertNotIn(task.taskId, self.tm._task_id_to_task)
        self.backend.on_cleanup.assert_not_called()
