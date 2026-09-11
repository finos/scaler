import unittest
from typing import Any

from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.address import AddressConfig


def _config(**overrides: Any) -> SchedulerConfig:
    kwargs: dict[str, Any] = dict(
        bind_address=AddressConfig.from_string("tcp://127.0.0.1:0"),
        object_storage_address=AddressConfig.from_string("tcp://127.0.0.1:0"),
    )
    kwargs.update(overrides)
    return SchedulerConfig(**kwargs)


class TestSchedulerConfigValidation(unittest.TestCase):
    def test_negative_load_balance_seconds_disables_balancing(self) -> None:
        # create_async_loop_routine treats a negative interval as "disabled"; the config must accept it so
        # a user can turn load balancing off, rather than crash the scheduler at startup (which then only
        # surfaces to clients as an opaque connection error).
        self.assertEqual(_config(load_balance_seconds=-1).load_balance_seconds, -1)

    def test_zero_load_balance_seconds_rejected(self) -> None:
        # Zero would busy-loop the balancer; a user disabling it means a negative value.
        with self.assertRaises(ValueError):
            _config(load_balance_seconds=0)

    def test_positive_load_balance_seconds_accepted(self) -> None:
        self.assertEqual(_config(load_balance_seconds=5).load_balance_seconds, 5)

    def test_non_positive_timeouts_rejected(self) -> None:
        for field in ("client_timeout_seconds", "worker_timeout_seconds", "object_retention_seconds"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                _config(**{field: 0})


if __name__ == "__main__":
    unittest.main()
