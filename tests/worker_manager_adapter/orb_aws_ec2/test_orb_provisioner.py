import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


def _make_provisioner(workers_per_instance: int = 1, max_instances: int = -1) -> ORBWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {"cpu": 4}
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    sdk = MagicMock()
    return ORBWorkerProvisioner(
        config=config,
        max_instances=max_instances,
        sdk=sdk,
        template_id="tmpl-123",
        workers_per_instance=workers_per_instance,
    )


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [SimpleNamespace(name=k, value=v) for k, v in capabilities.items()]
    return request


class TestORBWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_converts_workers_to_instance_count(self) -> None:
        provisioner = _make_provisioner(workers_per_instance=16)
        request = _make_request(task_concurrency=100, capabilities={"cpu": 4})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 7)  # ceil(100 / 16) = 7
