import logging
import unittest
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import (
    ClientDisconnect,
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
    WorkerManagerCommand,
)
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker_manager.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager.proxy.worker_process import WorkerProcess
from scaler.worker_manager.runner import WorkerManagerRunner
from tests.utility.utility import logging_test_name


class TestWorkerManagerHandleCommand(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.provisioner = MagicMock(spec=DeclarativeWorkerProvisioner)
        self.provisioner.set_desired_task_concurrency = AsyncMock()
        self.send_mock = AsyncMock()
        self.runner = WorkerManagerRunner(
            address=MagicMock(),
            name="test_runner",
            heartbeat_interval_seconds=5,
            capabilities={"cpu": 4},
            max_provisioner_units=4,
            worker_manager_id=b"mgr",
            worker_provisioner=self.provisioner,
        )
        connector = AsyncMock()
        connector.send = self.send_mock
        self.runner._connector_external = connector

    async def test_set_desired_task_concurrency_calls_declarative_provisioner(self) -> None:
        requests = [MagicMock()]
        cmd = MagicMock(spec=WorkerManagerCommand)
        cmd.setDesiredTaskConcurrencyRequests = requests

        await self.runner._handle_command(cmd)

        self.provisioner.set_desired_task_concurrency.assert_called_once_with(requests)
        self.send_mock.assert_not_called()

    async def test_unknown_command_payload_logs_warning_without_crashing(self) -> None:
        cmd = MagicMock(spec=WorkerManagerCommand)
        # Remove the only recognized payload field to simulate an unknown variant from a
        # newer scheduler (or a remote worker manager) that this runner does not understand.
        del cmd.setDesiredTaskConcurrencyRequests

        with self.assertLogs("scaler", level=logging.WARNING) as captured:
            await self.runner._handle_command(cmd)

        self.assertTrue(any("Unknown action" in m for m in captured.output))
        self.provisioner.set_desired_task_concurrency.assert_not_called()
        self.send_mock.assert_not_called()

    async def test_unknown_message_type_logs_warning_without_crashing(self) -> None:
        class _Unknown:
            pass

        with self.assertLogs("scaler", level=logging.WARNING) as captured:
            await self.runner._on_receive_external(_Unknown())  # type: ignore[arg-type]

        self.assertTrue(any("Unknown action" in m or "unrecognized" in m for m in captured.output))
        self.send_mock.assert_not_called()


class TestWorkerProcessOnReceiveExternal(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.on_heartbeat_echo = AsyncMock()
        self.on_task_new = AsyncMock()
        self.on_cancel_task = AsyncMock()
        self.on_object_instruction = AsyncMock()
        self.task_cancel = MagicMock()

        heartbeat_manager = MagicMock()
        heartbeat_manager.on_heartbeat_echo = self.on_heartbeat_echo
        task_manager = MagicMock()
        task_manager.on_task_new = self.on_task_new
        task_manager.on_cancel_task = self.on_cancel_task
        task_manager.on_object_instruction = self.on_object_instruction

        self.wp = WorkerProcess(
            name="test_worker",
            address=MagicMock(),
            object_storage_address=None,
            capabilities={},
            base_concurrency=1,
            heartbeat_interval_seconds=1,
            death_timeout_seconds=10,
            task_queue_size=10,
            io_threads=1,
            event_loop="asyncio",
            worker_manager_id=b"mgr",
            processor_status_provider_factory=MagicMock(),
            execution_backend_factory=MagicMock(),
        )
        self.wp._heartbeat_manager = heartbeat_manager
        self.wp._task_manager = task_manager
        self.wp._task = self.task_cancel

    async def _dispatch(self, message: object) -> None:
        await self.wp._WorkerProcess__on_receive_external(message)

    async def test_messages_before_heartbeat_echo_are_queued(self) -> None:
        task = _make_task()
        await self._dispatch(task)

        self.assertEqual(len(self.wp._backoff_message_queue), 1)
        self.assertIs(self.wp._backoff_message_queue[0], task)
        self.on_task_new.assert_not_called()

    async def test_queued_messages_replayed_in_order_after_heartbeat_echo(self) -> None:
        task1 = _make_task()
        task2 = _make_task()
        await self._dispatch(task1)
        await self._dispatch(task2)
        self.assertEqual(len(self.wp._backoff_message_queue), 2)

        echo = WorkerHeartbeatEcho()
        await self._dispatch(echo)

        self.assertTrue(self.wp._heartbeat_received)
        self.assertEqual(len(self.wp._backoff_message_queue), 0)
        self.on_heartbeat_echo.assert_called_once_with(echo)
        calls = self.on_task_new.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIs(calls[0][0][0], task1)
        self.assertIs(calls[1][0][0], task2)

    async def test_task_routes_to_on_task_new(self) -> None:
        self.wp._heartbeat_received = True
        task = _make_task()
        await self._dispatch(task)
        self.on_task_new.assert_called_once_with(task)

    async def test_task_cancel_routes_to_on_cancel_task(self) -> None:
        self.wp._heartbeat_received = True
        cancel = _make_task_cancel()
        await self._dispatch(cancel)
        self.on_cancel_task.assert_called_once_with(cancel)

    async def test_object_instruction_routes_to_on_object_instruction(self) -> None:
        self.wp._heartbeat_received = True
        instruction = _make_object_instruction()
        await self._dispatch(instruction)
        self.on_object_instruction.assert_called_once_with(instruction)

    async def test_client_disconnect_shutdown_raises_client_shutdown_exception(self) -> None:
        self.wp._heartbeat_received = True
        msg = ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown)
        with self.assertRaises(ClientShutdownException):
            await self._dispatch(msg)

    async def test_unknown_message_type_raises_type_error(self) -> None:
        self.wp._heartbeat_received = True

        class _Unknown:
            pass

        with self.assertRaises(TypeError):
            await self._dispatch(_Unknown())


def _make_task(source: Optional[ClientID] = None) -> Task:
    source = source or ClientID.generate_client_id()
    return Task(
        taskId=TaskID.generate_task_id(),
        source=source,
        metadata=TaskFlags(priority=0).serialize(),
        funcObjectId=ObjectID.generate_object_id(source),
        functionArgs=[],
        capabilities={},
    )


def _make_task_cancel() -> TaskCancel:
    return TaskCancel(taskId=TaskID.generate_task_id(), flags=TaskCancel.TaskCancelFlags(force=False))


def _make_object_instruction() -> ObjectInstruction:
    client_id = ClientID.generate_client_id()
    return ObjectInstruction(
        instructionType=ObjectInstruction.ObjectInstructionType.delete,
        objectUser=client_id,
        objectMetadata=ObjectMetadata(
            objectIds=(ObjectID.generate_object_id(client_id),), objectTypes=(), objectNames=()
        ),
    )
