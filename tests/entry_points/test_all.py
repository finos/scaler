import dataclasses
import unittest
from typing import List, Optional
from unittest.mock import patch

from scaler.config.config_class import ConfigClass
from scaler.config.reconstruction import _from_args

# ---------------------------------------------------------------------------
# Minimal stub configs for section= tests
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _SimpleSchedulerConfig(ConfigClass):
    host: str = "localhost"
    port: int = 8516


@dataclasses.dataclass
class _SimpleWorkerConfig(ConfigClass):
    workers: int = 1


@dataclasses.dataclass
class _SectionTestConfig(ConfigClass):
    scheduler: Optional[_SimpleSchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    workers: List[_SimpleWorkerConfig] = dataclasses.field(default_factory=list, metadata=dict(section="workers"))


class TestSectionMetadata(unittest.TestCase):
    """Tests the section= metadata path in ConfigClass / _from_args."""

    def _build(self, toml_data):
        return _from_args(_SectionTestConfig, {}, toml_data)

    def test_single_table_populates_optional(self) -> None:
        toml = {"scheduler": {"host": "192.168.1.1", "port": 9999}}
        config = self._build(toml)
        self.assertIsNotNone(config.scheduler)
        self.assertEqual(config.scheduler.host, "192.168.1.1")
        self.assertEqual(config.scheduler.port, 9999)

    def test_absent_section_gives_none(self) -> None:
        config = self._build({})
        self.assertIsNone(config.scheduler)

    def test_absent_list_section_gives_empty_list(self) -> None:
        config = self._build({})
        self.assertEqual(config.workers, [])

    def test_single_dict_in_list_section_gives_one_element(self) -> None:
        toml = {"workers": {"workers": 4}}
        config = self._build(toml)
        self.assertEqual(len(config.workers), 1)
        self.assertEqual(config.workers[0].workers, 4)

    def test_array_of_tables_gives_multiple_elements(self) -> None:
        toml = {"workers": [{"workers": 2}, {"workers": 8}]}
        config = self._build(toml)
        self.assertEqual(len(config.workers), 2)
        self.assertEqual(config.workers[0].workers, 2)
        self.assertEqual(config.workers[1].workers, 8)

    def test_both_sections_populated(self) -> None:
        toml = {"scheduler": {"host": "10.0.0.1", "port": 1234}, "workers": [{"workers": 3}]}
        config = self._build(toml)
        self.assertIsNotNone(config.scheduler)
        self.assertEqual(config.scheduler.host, "10.0.0.1")
        self.assertEqual(len(config.workers), 1)
        self.assertEqual(config.workers[0].workers, 3)


# ---------------------------------------------------------------------------
# Minimal ScalerAllConfig-like stub for end-to-end tests
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _StubScalerAllConfig(ConfigClass):
    scheduler: Optional[_SimpleSchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    workers: List[_SimpleWorkerConfig] = dataclasses.field(default_factory=list, metadata=dict(section="workers"))


class TestScalerAllEndToEnd(unittest.TestCase):
    """End-to-end tests for the section= flow through ConfigClass.parse."""

    def _make_toml_data(self, data):
        """Return a mock _load_toml that yields the given data dict."""
        return patch("scaler.config.config_class._load_toml", return_value=data)

    @patch("sys.argv", ["scaler", "--config", "test.toml"])
    def test_no_recognized_sections_exits(self) -> None:
        with self._make_toml_data({}):
            config = _StubScalerAllConfig.parse("scaler", "all")
        self.assertIsNone(config.scheduler)
        self.assertEqual(config.workers, [])

    @patch("sys.argv", ["scaler", "--config", "test.toml"])
    def test_scheduler_section_populated(self) -> None:
        toml = {"scheduler": {"host": "127.0.0.1", "port": 8516}}
        with self._make_toml_data(toml):
            config = _StubScalerAllConfig.parse("scaler", "all")
        self.assertIsNotNone(config.scheduler)
        self.assertEqual(config.scheduler.host, "127.0.0.1")

    @patch("sys.argv", ["scaler", "--config", "test.toml"])
    def test_workers_list_populated(self) -> None:
        toml = {"workers": [{"workers": 4}, {"workers": 8}]}
        with self._make_toml_data(toml):
            config = _StubScalerAllConfig.parse("scaler", "all")
        self.assertEqual(len(config.workers), 2)

    @patch("sys.argv", ["scaler", "--help"])
    def test_help_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _StubScalerAllConfig.parse("scaler", "all")


class TestScalerMain(unittest.TestCase):
    """Tests for scaler main() process spawning logic."""

    def test_no_sections_exits_with_code_1(self) -> None:
        from scaler.entry_points.scaler import main

        with (
            patch("scaler.config.config_class._load_toml", return_value={}),
            patch("sys.argv", ["scaler", "test.toml"]),
        ):
            with self.assertRaises(SystemExit) as ctx:
                main()
        self.assertEqual(ctx.exception.code, 1)


# ---------------------------------------------------------------------------
# Tests for the real ScalerAllConfig shape
# ---------------------------------------------------------------------------


class TestScalerAllConfigShape(unittest.TestCase):
    """Tests that ScalerAllConfig reads the [[worker_manager]] sections."""

    def _parse(self, toml_data):
        from scaler.entry_points.scaler import ScalerAllConfig

        with (
            patch("scaler.config.config_class._load_toml", return_value=toml_data),
            patch("sys.argv", ["scaler", "test.toml"]),
        ):
            return ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)

    def test_worker_manager_native_parsed_from_toml(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        toml = {
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
            }
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 1)
        self.assertIsInstance(config.worker_managers[0], NativeWorkerManagerConfig)

    def test_per_manager_worker_io_threads_from_toml(self) -> None:
        toml = {
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
                "io_threads": 4,
            }
        }
        config = self._parse(toml)
        self.assertEqual(config.worker_managers[0].worker_config.io_threads, 4)

    def test_per_manager_event_loop_from_toml(self) -> None:
        toml = {
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
                "event_loop": "builtin",
            }
        }
        config = self._parse(toml)
        self.assertEqual(config.worker_managers[0].worker_config.event_loop, "builtin")

    def test_multiple_worker_managers_from_toml(self) -> None:
        toml = {
            "worker_manager": [
                {"type": "baremetal_native", "scheduler_address": "tcp://127.0.0.1:6378", "worker_manager_id": "wm-1"},
                {"type": "baremetal_native", "scheduler_address": "tcp://127.0.0.1:6378", "worker_manager_id": "wm-2"},
            ]
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 2)

    def _native_base(self, **extra):
        return {
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
                **extra,
            }
        }

    def test_logging_level_long_name(self) -> None:
        """logging_level (long name) should be accepted in [[worker_manager]] sections."""
        config = self._parse(self._native_base(logging_level="WARNING"))
        self.assertEqual(config.worker_managers[0].logging_config.level, "WARNING")

    def test_logging_paths_long_name(self) -> None:
        """logging_paths (long name) should be accepted in [[worker_manager]] sections."""
        config = self._parse(self._native_base(logging_paths=["/tmp/wm.log"]))
        self.assertIn("/tmp/wm.log", config.worker_managers[0].logging_config.paths)

    def test_worker_config_per_worker_capabilities(self) -> None:
        config = self._parse(self._native_base(per_worker_capabilities="linux,cpu=4"))
        caps = config.worker_managers[0].worker_config.per_worker_capabilities
        self.assertIn("linux", caps.capabilities)

    def test_worker_config_task_timeout_seconds(self) -> None:
        config = self._parse(self._native_base(task_timeout_seconds=300))
        self.assertEqual(config.worker_managers[0].worker_config.task_timeout_seconds, 300)

    def test_mode_value_based_string(self) -> None:
        """mode = "fixed" (value-based, lowercase) should work the same as for scaler_worker_manager."""
        from scaler.config.section.native_worker_manager import NativeWorkerManagerMode

        config = self._parse(self._native_base(mode="fixed"))
        self.assertEqual(config.worker_managers[0].mode, NativeWorkerManagerMode.FIXED)

    def test_orb_aws_ec2_worker_manager_parsed_from_toml(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        toml = {
            "worker_manager": {
                "type": "orb_aws_ec2",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-orb",
                "image_id": "ami-0528819f94f4f5fa5",
                "aws_region": "us-east-1",
            }
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 1)
        self.assertIsInstance(config.worker_managers[0], ORBAWSEC2WorkerManagerConfig)

    def test_orb_aws_ec2_fields_from_toml(self) -> None:
        toml = {
            "worker_manager": {
                "type": "orb_aws_ec2",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-orb",
                "image_id": "ami-0528819f94f4f5fa5",
                "instance_type": "t3.medium",
                "aws_region": "eu-west-1",
            }
        }
        config = self._parse(toml)
        self.assertEqual(config.worker_managers[0].image_id, "ami-0528819f94f4f5fa5")
        self.assertEqual(config.worker_managers[0].instance_type, "t3.medium")
        self.assertEqual(config.worker_managers[0].aws_region, "eu-west-1")

    def test_mixed_native_and_orb_aws_ec2_worker_managers(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        toml = {
            "worker_manager": [
                {"type": "baremetal_native", "scheduler_address": "tcp://127.0.0.1:6378", "worker_manager_id": "wm-1"},
                {
                    "type": "orb_aws_ec2",
                    "scheduler_address": "tcp://127.0.0.1:6378",
                    "worker_manager_id": "wm-orb",
                    "image_id": "ami-0528819f94f4f5fa5",
                    "aws_region": "us-east-1",
                },
            ]
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 2)
        self.assertIsInstance(config.worker_managers[0], NativeWorkerManagerConfig)
        self.assertIsInstance(config.worker_managers[1], ORBAWSEC2WorkerManagerConfig)

    def test_oci_hpc_worker_manager_parsed_from_toml(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        toml = {
            "worker_manager": {
                "type": "oci_hpc",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-oci-hpc",
                "compartment_id": "ocid1.compartment.oc1..example",
                "availability_domain": "AD-1",
                "subnet_id": "ocid1.subnet.oc1.phx.example",
                "container_image": "phx.ocir.io/namespace/scaler:latest",
                "object_storage_namespace": "namespace",
                "object_storage_bucket": "bucket",
            }
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 1)
        self.assertIsInstance(config.worker_managers[0], OCIHPCWorkerManagerConfig)

    def test_oci_raw_worker_manager_parsed_from_toml(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        toml = {
            "worker_manager": {
                "type": "oci_raw",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-oci-raw",
                "compartment_id": "ocid1.compartment.oc1..example",
                "availability_domain": "AD-1",
                "subnet_id": "ocid1.subnet.oc1.phx.example",
                "container_image": "phx.ocir.io/namespace/scaler:latest",
                "python_version": "3.12",
                "requirements_txt": "opengris-scaler>=1.26.6",
            }
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 1)
        self.assertIsInstance(config.worker_managers[0], OCIRawWorkerManagerConfig)


class TestRunWorkerManager(unittest.TestCase):
    """Tests that _run_worker_manager calls register_event_loop and bootstrap_process from the per-manager config."""

    def _make_native_config(self, event_loop="builtin", logging_level="INFO"):
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.common.worker import WorkerConfig
        from scaler.config.common.worker_manager import WorkerManagerConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
        from scaler.config.types.address import AddressConfig

        wmc = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://localhost:6378"), worker_manager_id="wm-test"
        )
        return NativeWorkerManagerConfig(
            worker_manager_config=wmc,
            worker_config=WorkerConfig(event_loop=event_loop),
            logging_config=LoggingConfig(level=logging_level),
        )

    def test_register_event_loop_called_with_config_event_loop(self) -> None:
        from scaler.entry_points.scaler import _run_worker_manager

        config = self._make_native_config(event_loop="builtin")

        with (
            patch("scaler.entry_points.scaler.register_event_loop") as mock_reg,
            patch("scaler.entry_points.scaler.bootstrap_process"),
            patch("scaler.worker_manager.native.worker_manager.NativeWorkerManager") as mock_nm,
        ):
            mock_nm.return_value.run.return_value = None
            _run_worker_manager(config)

        mock_reg.assert_called_once_with("builtin")

    def test_bootstrap_process_called_with_logging_config(self) -> None:
        from scaler.entry_points.scaler import _run_worker_manager

        config = self._make_native_config(logging_level="WARNING")

        with (
            patch("scaler.entry_points.scaler.bootstrap_process") as mock_log,
            patch("scaler.entry_points.scaler.register_event_loop"),
            patch("scaler.worker_manager.native.worker_manager.NativeWorkerManager") as mock_nm,
        ):
            mock_nm.return_value.run.return_value = None
            _run_worker_manager(config)

        mock_log.assert_called_once_with(
            config.logging_config.paths, config.logging_config.config_file, "WARNING", process_name=config._tag
        )

    def _make_orb_aws_ec2_config(self, event_loop="builtin", logging_level="INFO"):
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.common.worker import WorkerConfig
        from scaler.config.common.worker_manager import WorkerManagerConfig
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
        from scaler.config.types.address import AddressConfig

        wmc = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://localhost:6378"), worker_manager_id="wm-test"
        )
        return ORBAWSEC2WorkerManagerConfig(
            worker_manager_config=wmc,
            image_id="ami-0528819f94f4f5fa5",
            aws_region="us-east-1",
            worker_config=WorkerConfig(event_loop=event_loop),
            logging_config=LoggingConfig(level=logging_level),
        )

    def test_orb_aws_ec2_run_worker_manager_dispatches_correctly(self) -> None:
        from scaler.entry_points.scaler import _run_worker_manager

        config = self._make_orb_aws_ec2_config()

        with (
            patch("scaler.entry_points.scaler.bootstrap_process"),
            patch("scaler.entry_points.scaler.register_event_loop"),
            patch("scaler.worker_manager.nested.orb_aws_ec2.worker_manager.ORBAWSEC2WorkerManager") as mock_orb,
        ):
            mock_orb.return_value.run.return_value = None
            _run_worker_manager(config)

        mock_orb.assert_called_once_with(config)
        mock_orb.return_value.run.assert_called_once()

    def test_orb_aws_ec2_register_event_loop_called(self) -> None:
        from scaler.entry_points.scaler import _run_worker_manager

        config = self._make_orb_aws_ec2_config(event_loop="builtin")

        with (
            patch("scaler.entry_points.scaler.register_event_loop") as mock_reg,
            patch("scaler.entry_points.scaler.bootstrap_process"),
            patch("scaler.worker_manager.nested.orb_aws_ec2.worker_manager.ORBAWSEC2WorkerManager") as mock_orb,
        ):
            mock_orb.return_value.run.return_value = None
            _run_worker_manager(config)

        mock_reg.assert_called_once_with("builtin")
