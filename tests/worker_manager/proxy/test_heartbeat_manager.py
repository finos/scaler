import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import WorkerHeartbeat
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.mixins import TimeoutManager
from scaler.worker_manager.proxy.heartbeat_manager import HeartbeatManager
from scaler.worker_manager.proxy.mixins import ProcessorStatusProvider
from scaler.worker_manager.proxy.task_manager import TaskManager
from tests.utility.utility import logging_test_name


def _make_heartbeat_manager() -> HeartbeatManager:
    provider = MagicMock(spec=ProcessorStatusProvider)
    provider.get_processor_statuses.return_value = []
    return HeartbeatManager(
        object_storage_address=None,
        capabilities={},
        task_queue_size=10,
        worker_manager_id=b"test_manager",
        processor_status_provider=provider,
    )


class TestHeartbeatManagerTaskLock(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.hm = _make_heartbeat_manager()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.task_manager = MagicMock(spec=TaskManager)
        self.task_manager.get_queued_size.return_value = 0
        self.timeout_manager = MagicMock(spec=TimeoutManager)
        self.hm.register(self.connector_external, self.connector_storage, self.task_manager, self.timeout_manager)

    async def test_task_lock_false_when_semaphore_free(self) -> None:
        self.task_manager.can_accept_task.return_value = True
        with patch("psutil.virtual_memory") as mock_vm, patch("psutil.Process"):
            mock_vm.return_value.available = 0
            await self.hm.routine()

        self.connector_external.send.assert_called_once()
        sent: WorkerHeartbeat = self.connector_external.send.call_args[0][0]
        self.assertIsInstance(sent, WorkerHeartbeat)
        self.assertFalse(sent.taskLock)

    async def test_task_lock_true_when_semaphore_locked(self) -> None:
        self.task_manager.can_accept_task.return_value = False
        with patch("psutil.virtual_memory") as mock_vm, patch("psutil.Process"):
            mock_vm.return_value.available = 0
            await self.hm.routine()

        self.connector_external.send.assert_called_once()
        sent: WorkerHeartbeat = self.connector_external.send.call_args[0][0]
        self.assertIsInstance(sent, WorkerHeartbeat)
        self.assertTrue(sent.taskLock)

    async def test_routine_skipped_when_timestamp_nonzero(self) -> None:
        self.hm._start_timestamp_ns = 12345
        await self.hm.routine()
        self.connector_external.send.assert_not_called()
