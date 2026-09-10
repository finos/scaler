"""
Tests that the example_config.toml shown in the README can be parsed correctly
by each component's config class.

The TOML content is copied verbatim from the README.  Only positional arguments
(addresses) are supplied on the command line, as the README commands show.
"""

import unittest
from unittest.mock import mock_open, patch

from scaler.config.section.native_worker_manager import NativeWorkerManagerMode
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.webgui import WebGUIConfig

README_TOML = b"""
[scheduler]
bind_address = "tcp://127.0.0.1:6378"
object_storage_address = "tcp://127.0.0.1:6379"
monitor_address = "tcp://127.0.0.1:6380"
logging_level = "INFO"
logging_paths = ["/dev/stdout", "/var/log/scaler/scheduler.log"]
policy_engine_type = "simple"
policy_content = "allocate=even_load; scaling=vanilla"

[[worker_manager]]
type = "baremetal_native"
mode = "fixed"
max_task_concurrency = 8
worker_manager_id = "my-manager"
per_worker_capabilities = "linux,cpu=8"
task_timeout_seconds = 600
logging_level = "INFO"
logging_paths = ["/dev/stdout", "/var/log/scaler/worker.log"]

[object_storage_server]
bind_address = "tcp://127.0.0.1:6379"

[gui]
gui_address = "127.0.0.1:8081"
"""


class TestReadmeConfig(unittest.TestCase):
    @patch(
        "sys.argv",
        [
            "scaler_scheduler",
            "tcp://127.0.0.1:6378",
            "--object-storage-address",
            "tcp://127.0.0.1:6379",
            "--config",
            "config.toml",
        ],
    )
    @patch("builtins.open", mock_open(read_data=README_TOML))
    def test_scheduler_section(self) -> None:
        config = SchedulerConfig.parse("scaler_scheduler", "scheduler")

        self.assertEqual(str(config.object_storage_address), "tcp://127.0.0.1:6379")
        self.assertEqual(str(config.monitor_address), "tcp://127.0.0.1:6380")
        self.assertEqual(config.logging_config.level, "INFO")
        self.assertIn("/dev/stdout", config.logging_config.paths)
        self.assertIn("/var/log/scaler/scheduler.log", config.logging_config.paths)
        self.assertEqual(config.policy.policy_engine_type, "simple")
        self.assertEqual(config.policy.policy_content, "allocate=even_load; scaling=vanilla")

    def test_baremetal_native_section(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        toml_data = tomllib.loads(README_TOML.decode())
        entries = toml_data.get("worker_manager", [])
        (section_data,) = [e for e in entries if e.get("type") == "baremetal_native"]

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", section_data, argv=["tcp://127.0.0.1:6378"]
        )

        self.assertIsInstance(config.mode, NativeWorkerManagerMode)
        self.assertEqual(config.mode, NativeWorkerManagerMode.FIXED)
        self.assertEqual(config.worker_manager_config.max_task_concurrency, 8)
        self.assertEqual(config.worker_manager_config.worker_manager_id, "my-manager")
        self.assertEqual(config.worker_config.task_timeout_seconds, 600)
        self.assertIn("linux", config.worker_config.per_worker_capabilities.capabilities)
        self.assertEqual(config.logging_config.level, "INFO")
        self.assertIn("/dev/stdout", config.logging_config.paths)
        self.assertIn("/var/log/scaler/worker.log", config.logging_config.paths)

    @patch("sys.argv", ["scaler_gui", "tcp://127.0.0.1:6380", "--config", "config.toml"])
    @patch("builtins.open", mock_open(read_data=README_TOML))
    def test_webgui_section(self) -> None:
        config = WebGUIConfig.parse("scaler_gui", "gui")

        self.assertEqual(config.gui_address.host, "127.0.0.1")
        self.assertEqual(config.gui_address.port, 8081)
