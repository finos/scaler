import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager_adapter.aws_hpc.worker_manager import BatchWorkerProvisioner


def _make_provisioner(max_concurrent_jobs: int = 100) -> BatchWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.max_concurrent_jobs = max_concurrent_jobs
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    return BatchWorkerProvisioner(config)


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [MagicMock(key=k, value=v) for k, v in capabilities.items()]
    return request


class TestBatchWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_converts_task_concurrency_to_process_count(self) -> None:
        provisioner = _make_provisioner(max_concurrent_jobs=100)
        request = _make_request(task_concurrency=150, capabilities={})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 2)  # ceil(150 / 100) = 2

    async def test_task_concurrency_zero_sets_desired_to_zero(self) -> None:
        provisioner = _make_provisioner()
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([_make_request(task_concurrency=100, capabilities={})])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 1)
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([_make_request(task_concurrency=0, capabilities={})])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 0)
