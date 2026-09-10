import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager_adapter.baremetal.native import NativeWorkerProvisioner


def _make_provisioner(max_task_concurrency: int = -1) -> NativeWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.worker_manager_config.max_task_concurrency = max_task_concurrency
    config.worker_manager_config.worker_manager_id = "test-wm"
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    config.worker_type = "NAT"
    return NativeWorkerProvisioner(config)


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [MagicMock(key=k, value=v) for k, v in capabilities.items()]
    return request


def _make_worker(pid: int = 1234) -> MagicMock:
    worker = MagicMock()
    worker.pid = pid
    worker.identity = f"NAT|worker-{pid}"
    return worker


class TestNativeWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_passes_task_concurrency_directly_as_desired_unit_count(self) -> None:
        provisioner = _make_provisioner()
        request = _make_request(task_concurrency=3, capabilities={})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 3)


class TestNativeWorkerProvisionerStopUnits(unittest.IsolatedAsyncioTestCase):
    async def test_stop_units_more_than_available_does_not_raise(self) -> None:
        provisioner = _make_provisioner()
        workers = [_make_worker(pid=3000 + i) for i in range(2)]
        with patch.object(provisioner, "_create_worker", side_effect=workers):
            await provisioner.start_units(2)
        with patch("os.kill"), patch("psutil.Process"):
            await provisioner.stop_units(5)
        self.assertEqual(provisioner._workers, [])
