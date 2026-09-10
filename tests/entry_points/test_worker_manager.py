import dataclasses
import importlib.util
import unittest
from typing import Optional
from unittest.mock import mock_open, patch

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.python_worker_environment import PythonWorkerEnvironmentConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
from scaler.config.types.address import AddressConfig


@dataclasses.dataclass
class _LeafConfig(ConfigClass):
    value: int = 0
    name: str = "default"


@dataclasses.dataclass
class _RootConfig(ConfigClass):
    foo: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="foo_section"))
    bar: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="bar_section"))


@dataclasses.dataclass
class _RootWithCommonConfig(ConfigClass):
    log_level: str = "INFO"
    foo: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="foo_section"))
    bar: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="bar_section"))


# Two-level nested subcommands for nesting tests
@dataclasses.dataclass
class _Level2Config(ConfigClass):
    depth: int = 2


@dataclasses.dataclass
class _Level1Config(ConfigClass):
    inner: Optional[_Level2Config] = dataclasses.field(default=None, metadata=dict(subcommand="level2_section"))


@dataclasses.dataclass
class _NestedRootConfig(ConfigClass):
    level1: Optional[_Level1Config] = dataclasses.field(default=None, metadata=dict(subcommand="level1_section"))


class TestWorkerManagerSubcommands(unittest.TestCase):
    """Tests the subcommand= metadata path in ConfigClass."""

    @patch("sys.argv", ["prog", "foo", "--value", "42"])
    def test_foo_subcommand_selected(self) -> None:
        config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertIsNone(config.bar)
        self.assertEqual(config.foo.value, 42)

    @patch("sys.argv", ["prog", "bar", "--value", "7"])
    def test_bar_subcommand_selected(self) -> None:
        config = _RootConfig.parse("prog", "")
        self.assertIsNone(config.foo)
        self.assertIsNotNone(config.bar)
        self.assertEqual(config.bar.value, 7)

    @patch("sys.argv", ["prog", "foo"])
    def test_default_values_used(self) -> None:
        config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertEqual(config.foo.value, 0)
        self.assertEqual(config.foo.name, "default")

    @patch("sys.argv", ["prog", "foo", "--log-level", "DEBUG"])
    def test_root_level_fields_populated(self) -> None:
        config = _RootWithCommonConfig.parse("prog", "")
        self.assertEqual(config.log_level, "DEBUG")
        self.assertIsNotNone(config.foo)

    @patch("sys.argv", ["prog", "foo", "--value", "5"])
    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [foo_section]
            value = 99
            name = "from_toml"
            """),
    )
    def test_cli_overrides_toml(self) -> None:
        with patch("sys.argv", ["prog", "--config", "cfg.toml", "foo", "--value", "5"]):
            config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        # CLI --value 5 should override TOML value 99
        self.assertEqual(config.foo.value, 5)
        # name not provided on CLI -> TOML value used
        self.assertEqual(config.foo.name, "from_toml")

    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [foo_section]
            value = 77
            """),
    )
    def test_config_after_subcommand(self) -> None:
        """--config appearing after the sub-command name must still be loaded."""
        with patch("sys.argv", ["prog", "foo", "--config", "cfg.toml"]):
            config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertEqual(config.foo.value, 77)

    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [foo_section]
            value = 55
            """),
    )
    def test_config_before_subcommand(self) -> None:
        """--config appearing before the sub-command name must still be loaded."""
        with patch("sys.argv", ["prog", "--config", "cfg.toml", "foo"]):
            config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertEqual(config.foo.value, 55)

    @patch("sys.argv", ["prog"])
    def test_no_subcommand_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _RootConfig.parse("prog", "")

    @patch("sys.argv", ["prog", "bad_cmd"])
    def test_unknown_subcommand_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _RootConfig.parse("prog", "")

    @patch("sys.argv", ["prog", "--help"])
    def test_help_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _RootConfig.parse("prog", "")

    @patch("sys.argv", ["prog", "level1", "inner", "--depth", "99"])
    def test_nested_subcommands_route_correctly(self) -> None:
        config = _NestedRootConfig.parse("prog", "")
        self.assertIsNotNone(config.level1)
        self.assertIsNotNone(config.level1.inner)
        self.assertEqual(config.level1.inner.depth, 99)

    @patch("sys.argv", ["prog", "level1", "inner"])
    def test_nested_subcommands_unselected_are_none(self) -> None:
        config = _NestedRootConfig.parse("prog", "")
        self.assertIsNotNone(config.level1)
        self.assertIsNotNone(config.level1.inner)
        self.assertEqual(config.level1.inner.depth, 2)  # default


# ---------------------------------------------------------------------------
# Tests for the scaler_worker_manager subcommand interface
# ---------------------------------------------------------------------------

_NATIVE_BASE_ARGS = [
    "scaler_worker_manager",
    "baremetal_native",
    "--worker-manager-id",
    "wm-test",
    "tcp://127.0.0.1:6378",
]


class TestWorkerManagerConfigFields(unittest.TestCase):
    """Tests that the subcommand interface correctly parses per-manager fields from CLI and TOML."""

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--logging-level", "DEBUG"])
    def test_logging_level_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--logging-level", "DEBUG"],
        )
        self.assertEqual(config.logging_config.level, "DEBUG")

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--logging-paths", "/tmp/scaler.log"])
    def test_logging_paths_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--logging-paths", "/tmp/scaler.log"],
        )
        self.assertIn("/tmp/scaler.log", config.logging_config.paths)

    def test_logging_defaults(self) -> None:
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378"]
        )
        self.assertEqual(config.logging_config.level, LoggingConfig().level)

    def test_logging_level_from_toml(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        section_data = {
            "type": "baremetal_native",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "logging_level": "DEBUG",
        }
        config = NativeWorkerManagerConfig.parse_with_section("scaler_worker_manager", section_data, argv=[])
        self.assertEqual(config.logging_config.level, "DEBUG")

    def test_cli_overrides_toml_logging(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        section_data = {
            "type": "baremetal_native",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "logging_level": "DEBUG",
        }
        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", section_data, argv=["--logging-level", "WARNING"]
        )
        self.assertEqual(config.logging_config.level, "WARNING")

    def test_worker_io_threads_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--io-threads", "4"],
        )
        self.assertEqual(config.worker_config.io_threads, 4)

    def test_event_loop_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--event-loop", "builtin"],
        )
        self.assertEqual(config.worker_config.event_loop, "builtin")

    def test_per_manager_config_defaults(self) -> None:
        from scaler.config.common.worker import WorkerConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378"]
        )
        self.assertEqual(config.worker_config.io_threads, WorkerConfig().io_threads)
        self.assertEqual(config.worker_config.event_loop, WorkerConfig().event_loop)


_ORB_AWS_EC2_IMAGE_ARGV = [
    "tcp://127.0.0.1:6378",
    "--worker-manager-id",
    "wm-test",
    "--aws-region",
    "us-east-1",
    "--image-id",
    "ami-0528819f94f4f5fa5",
]
_ORB_AWS_EC2_AUTO_ARGV = [
    "tcp://127.0.0.1:6378",
    "--worker-manager-id",
    "wm-test",
    "--aws-region",
    "us-east-1",
    "--python-version",
    "3.13",
    "--requirements-txt",
    "opengris-scaler>=1.26.6",
]


_OCI_HPC_BASE_ARGV = [
    "tcp://127.0.0.1:6378",
    "--worker-manager-id",
    "wm-test",
    "--compartment-id",
    "ocid1.compartment.oc1..example",
    "--availability-domain",
    "AD-1",
    "--subnet-id",
    "ocid1.subnet.oc1.phx.example",
    "--container-image",
    "phx.ocir.io/namespace/scaler:latest",
    "--object-storage-namespace",
    "namespace",
    "--object-storage-bucket",
    "bucket",
]


class TestOCIHPCWorkerManagerConfig(unittest.TestCase):
    """Tests that OCIHPCWorkerManagerConfig correctly parses fields from CLI and TOML."""

    def test_required_fields_parsed(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        config = OCIHPCWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=_OCI_HPC_BASE_ARGV)
        self.assertIsInstance(config, OCIHPCWorkerManagerConfig)
        self.assertEqual(config.container_instance_config.compartment_id, "ocid1.compartment.oc1..example")
        self.assertEqual(config.container_instance_config.availability_domain, "AD-1")
        self.assertEqual(config.container_instance_config.subnet_id, "ocid1.subnet.oc1.phx.example")
        self.assertEqual(config.container_instance_config.container_image, "phx.ocir.io/namespace/scaler:latest")
        self.assertEqual(config.object_storage_namespace, "namespace")
        self.assertEqual(config.object_storage_bucket, "bucket")

    def test_defaults(self) -> None:
        from scaler.config.common.oci_container_instance import DEFAULT_OCI_INSTANCE_SHAPE, DEFAULT_OCI_REGION
        from scaler.config.section.oci_hpc_worker_manager import (
            DEFAULT_OCI_HPC_JOB_TIMEOUT_SECONDS,
            DEFAULT_OCI_HPC_MAX_CONCURRENT_JOBS,
            DEFAULT_OCI_OBJECT_STORAGE_PREFIX,
            OCIHPCWorkerManagerConfig,
        )
        from scaler.config.types.oci_auth_type import OCIAuthType

        config = OCIHPCWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=_OCI_HPC_BASE_ARGV)
        self.assertEqual(config.object_storage_prefix, DEFAULT_OCI_OBJECT_STORAGE_PREFIX)
        self.assertEqual(config.instance_ocpus, 1.0)
        self.assertEqual(config.instance_memory_gb, 6.0)
        self.assertEqual(config.base_concurrency, DEFAULT_OCI_HPC_MAX_CONCURRENT_JOBS)
        self.assertEqual(config.job_timeout_seconds, DEFAULT_OCI_HPC_JOB_TIMEOUT_SECONDS)
        self.assertEqual(config.container_instance_config.instance_shape, DEFAULT_OCI_INSTANCE_SHAPE)
        self.assertEqual(config.container_instance_config.oci_region, DEFAULT_OCI_REGION)
        self.assertEqual(config.container_instance_config.auth_type, OCIAuthType.config_file)

    def test_optional_cli_fields(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        config = OCIHPCWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=[
                *_OCI_HPC_BASE_ARGV,
                "--base-concurrency",
                "10",
                "--job-timeout-seconds",
                "1200",
                "--instance-ocpus",
                "2.0",
                "--instance-memory-gb",
                "12.0",
                "--oci-region",
                "eu-frankfurt-1",
            ],
        )
        self.assertEqual(config.base_concurrency, 10)
        self.assertEqual(config.job_timeout_seconds, 1200)
        self.assertEqual(config.instance_ocpus, 2.0)
        self.assertEqual(config.instance_memory_gb, 12.0)
        self.assertEqual(config.container_instance_config.oci_region, "eu-frankfurt-1")

    def test_logging_level_from_cli(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        config = OCIHPCWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=[*_OCI_HPC_BASE_ARGV, "--logging-level", "DEBUG"]
        )
        self.assertEqual(config.logging_config.level, "DEBUG")

    def test_missing_compartment_id_raises(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--object-storage-namespace",
            "namespace",
            "--object-storage-bucket",
            "bucket",
        ]
        with self.assertRaises(SystemExit):
            OCIHPCWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_object_storage_namespace_raises(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--object-storage-bucket",
            "bucket",
        ]
        with self.assertRaises(SystemExit):
            OCIHPCWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_object_storage_bucket_raises(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--object-storage-namespace",
            "namespace",
        ]
        with self.assertRaises(SystemExit):
            OCIHPCWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_invalid_instance_ocpus_raises(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        with self.assertRaises(ValueError):
            OCIHPCWorkerManagerConfig.parse_with_section(
                "scaler_worker_manager", {}, argv=[*_OCI_HPC_BASE_ARGV, "--instance-ocpus", "0"]
            )

    def test_invalid_base_concurrency_raises(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        with self.assertRaises(ValueError):
            OCIHPCWorkerManagerConfig.parse_with_section(
                "scaler_worker_manager", {}, argv=[*_OCI_HPC_BASE_ARGV, "--base-concurrency", "0"]
            )

    def test_from_toml(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        section_data = {
            "type": "oci_hpc",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "compartment_id": "ocid1.compartment.oc1..example",
            "availability_domain": "AD-1",
            "subnet_id": "ocid1.subnet.oc1.phx.example",
            "container_image": "phx.ocir.io/namespace/scaler:latest",
            "object_storage_namespace": "namespace",
            "object_storage_bucket": "bucket",
            "base_concurrency": 20,
            "job_timeout_seconds": 900,
        }
        config = OCIHPCWorkerManagerConfig.parse_with_section("scaler_worker_manager", section_data, argv=[])
        self.assertEqual(config.base_concurrency, 20)
        self.assertEqual(config.job_timeout_seconds, 900)

    def test_cli_overrides_toml(self) -> None:
        from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig

        section_data = {
            "type": "oci_hpc",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "compartment_id": "ocid1.compartment.oc1..example",
            "availability_domain": "AD-1",
            "subnet_id": "ocid1.subnet.oc1.phx.example",
            "container_image": "phx.ocir.io/namespace/scaler:latest",
            "object_storage_namespace": "namespace",
            "object_storage_bucket": "bucket",
            "base_concurrency": 20,
        }
        config = OCIHPCWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", section_data, argv=["--base-concurrency", "5"]
        )
        self.assertEqual(config.base_concurrency, 5)


_OCI_RAW_BASE_ARGV = [
    "tcp://127.0.0.1:6378",
    "--worker-manager-id",
    "wm-test",
    "--compartment-id",
    "ocid1.compartment.oc1..example",
    "--availability-domain",
    "AD-1",
    "--subnet-id",
    "ocid1.subnet.oc1.phx.example",
    "--container-image",
    "phx.ocir.io/namespace/scaler:latest",
    "--python-version",
    "3.12",
    "--requirements-txt",
    "opengris-scaler>=1.26.6",
]


class TestOCIRawWorkerManagerConfig(unittest.TestCase):
    """Tests that OCIRawWorkerManagerConfig correctly parses fields from CLI and TOML."""

    def test_required_fields_parsed(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        config = OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=_OCI_RAW_BASE_ARGV)
        self.assertIsInstance(config, OCIRawWorkerManagerConfig)
        self.assertEqual(config.container_instance_config.compartment_id, "ocid1.compartment.oc1..example")
        self.assertEqual(config.container_instance_config.availability_domain, "AD-1")
        self.assertEqual(config.container_instance_config.subnet_id, "ocid1.subnet.oc1.phx.example")
        self.assertEqual(config.container_instance_config.container_image, "phx.ocir.io/namespace/scaler:latest")
        self.assertEqual(config.python_worker_environment.python_version, "3.12")
        self.assertEqual(config.python_worker_environment.requirements_txt, "opengris-scaler>=1.26.6")

    def test_defaults(self) -> None:
        from scaler.config.common.oci_container_instance import DEFAULT_OCI_INSTANCE_SHAPE, DEFAULT_OCI_REGION
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
        from scaler.config.types.oci_auth_type import OCIAuthType

        config = OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=_OCI_RAW_BASE_ARGV)
        self.assertEqual(config.instance_ocpus, 4.0)
        self.assertEqual(config.instance_memory_gb, 30.0)
        self.assertEqual(config.container_instance_config.instance_shape, DEFAULT_OCI_INSTANCE_SHAPE)
        self.assertEqual(config.container_instance_config.oci_region, DEFAULT_OCI_REGION)
        self.assertEqual(config.container_instance_config.auth_type, OCIAuthType.config_file)

    def test_optional_cli_fields(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        config = OCIRawWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=[
                *_OCI_RAW_BASE_ARGV,
                "--instance-ocpus",
                "8.0",
                "--instance-memory-gb",
                "64.0",
                "--oci-region",
                "eu-frankfurt-1",
            ],
        )
        self.assertEqual(config.instance_ocpus, 8.0)
        self.assertEqual(config.instance_memory_gb, 64.0)
        self.assertEqual(config.container_instance_config.oci_region, "eu-frankfurt-1")

    def test_logging_level_from_cli(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        config = OCIRawWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=[*_OCI_RAW_BASE_ARGV, "--logging-level", "DEBUG"]
        )
        self.assertEqual(config.logging_config.level, "DEBUG")

    def test_missing_compartment_id_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--python-version",
            "3.12",
            "--requirements-txt",
            "opengris-scaler>=1.26.6",
        ]
        with self.assertRaises(SystemExit):
            OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_availability_domain_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--python-version",
            "3.12",
            "--requirements-txt",
            "opengris-scaler>=1.26.6",
        ]
        with self.assertRaises(SystemExit):
            OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_subnet_id_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--availability-domain",
            "AD-1",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--python-version",
            "3.12",
            "--requirements-txt",
            "opengris-scaler>=1.26.6",
        ]
        with self.assertRaises(SystemExit):
            OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_container_image_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--python-version",
            "3.12",
            "--requirements-txt",
            "opengris-scaler>=1.26.6",
        ]
        with self.assertRaises(SystemExit):
            OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_python_version_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--requirements-txt",
            "opengris-scaler>=1.26.6",
        ]
        with self.assertRaises(ValueError):
            OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_missing_requirements_txt_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        argv = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--compartment-id",
            "ocid1.compartment.oc1..example",
            "--availability-domain",
            "AD-1",
            "--subnet-id",
            "ocid1.subnet.oc1.phx.example",
            "--container-image",
            "phx.ocir.io/namespace/scaler:latest",
            "--python-version",
            "3.12",
        ]
        with self.assertRaises(ValueError):
            OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv)

    def test_invalid_instance_ocpus_raises(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        with self.assertRaises(ValueError):
            OCIRawWorkerManagerConfig.parse_with_section(
                "scaler_worker_manager", {}, argv=[*_OCI_RAW_BASE_ARGV, "--instance-ocpus", "0"]
            )

    def test_from_toml(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        section_data = {
            "type": "oci_raw",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "compartment_id": "ocid1.compartment.oc1..example",
            "availability_domain": "AD-1",
            "subnet_id": "ocid1.subnet.oc1.phx.example",
            "container_image": "phx.ocir.io/namespace/scaler:latest",
            "python_version": "3.12",
            "requirements_txt": "opengris-scaler>=1.26.6",
            "instance_ocpus": 8.0,
            "instance_memory_gb": 64.0,
        }
        config = OCIRawWorkerManagerConfig.parse_with_section("scaler_worker_manager", section_data, argv=[])
        self.assertEqual(config.instance_ocpus, 8.0)
        self.assertEqual(config.instance_memory_gb, 64.0)

    def test_cli_overrides_toml(self) -> None:
        from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig

        section_data = {
            "type": "oci_raw",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "compartment_id": "ocid1.compartment.oc1..example",
            "availability_domain": "AD-1",
            "subnet_id": "ocid1.subnet.oc1.phx.example",
            "container_image": "phx.ocir.io/namespace/scaler:latest",
            "python_version": "3.12",
            "requirements_txt": "opengris-scaler>=1.26.6",
            "instance_ocpus": 8.0,
        }
        config = OCIRawWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", section_data, argv=["--instance-ocpus", "2.0"]
        )
        self.assertEqual(config.instance_ocpus, 2.0)

    @unittest.skipUnless(importlib.util.find_spec("oci") is not None, "oci SDK not installed")
    def test_oci_raw_subcommand_dispatches_worker_manager(self) -> None:
        with (
            patch("sys.argv", ["scaler_worker_manager", "oci_raw", *_OCI_RAW_BASE_ARGV]),
            patch("scaler.entry_points.worker_manager.bootstrap_process"),
            patch("scaler.entry_points.worker_manager.register_event_loop"),
            patch("scaler.worker_manager.nested.oci.worker_manager.OCIWorkerManager") as mock_mgr,
        ):
            mock_mgr.return_value.run.return_value = None
            from scaler.entry_points.worker_manager import main

            main()

        mock_mgr.assert_called_once()
        mock_mgr.return_value.run.assert_called_once()


class TestORBAWSEC2WorkerManagerSubcommand(unittest.TestCase):
    """Tests that ORBAWSEC2WorkerManagerConfig is correctly parsed via parse_with_section."""

    def test_orb_aws_ec2_image_id_parsed(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=_ORB_AWS_EC2_IMAGE_ARGV
        )
        self.assertIsInstance(config, ORBAWSEC2WorkerManagerConfig)
        self.assertEqual(config.image_id, "ami-0528819f94f4f5fa5")
        self.assertIsNone(config.python_worker_environment.python_version)
        self.assertIsNone(config.python_worker_environment.requirements_txt)

    def test_orb_aws_ec2_auto_install_mode_parsed(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=_ORB_AWS_EC2_AUTO_ARGV
        )
        self.assertIsNone(config.image_id)
        self.assertEqual(config.python_worker_environment.python_version, "3.13")
        self.assertEqual(config.python_worker_environment.requirements_txt, "opengris-scaler>=1.26.6")

    def test_orb_aws_ec2_defaults(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=_ORB_AWS_EC2_IMAGE_ARGV
        )
        self.assertEqual(config.instance_type, "t2.micro")

    def test_orb_aws_ec2_missing_aws_region_raises(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        argv_without_region = [
            "tcp://127.0.0.1:6378",
            "--worker-manager-id",
            "wm-test",
            "--image-id",
            "ami-0528819f94f4f5fa5",
        ]
        with self.assertRaises(SystemExit):
            ORBAWSEC2WorkerManagerConfig.parse_with_section("scaler_worker_manager", {}, argv=argv_without_region)

    def test_orb_aws_ec2_instance_type_and_region_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=[*_ORB_AWS_EC2_IMAGE_ARGV, "--instance-type", "t3.medium", "--aws-region", "eu-west-1"],
        )
        self.assertEqual(config.instance_type, "t3.medium")
        self.assertEqual(config.aws_region, "eu-west-1")

    def test_orb_aws_ec2_logging_level_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=[*_ORB_AWS_EC2_IMAGE_ARGV, "--logging-level", "DEBUG"]
        )
        self.assertEqual(config.logging_config.level, "DEBUG")

    def test_orb_aws_ec2_no_mode_raises(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerManagerConfig.parse_with_section(
                "scaler_worker_manager",
                {},
                argv=["tcp://127.0.0.1:6378", "--worker-manager-id", "wm-test", "--aws-region", "us-east-1"],
            )

    def test_orb_aws_ec2_image_id_and_python_version_raises(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerManagerConfig.parse_with_section(
                "scaler_worker_manager",
                {},
                argv=[
                    "tcp://127.0.0.1:6378",
                    "--worker-manager-id",
                    "wm-test",
                    "--aws-region",
                    "us-east-1",
                    "--image-id",
                    "ami-0528819f94f4f5fa5",
                    "--python-version",
                    "3.13",
                    "--requirements-txt",
                    "opengris-scaler",
                ],
            )

    def test_orb_aws_ec2_python_version_without_requirements_raises(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerManagerConfig.parse_with_section(
                "scaler_worker_manager",
                {},
                argv=[
                    "tcp://127.0.0.1:6378",
                    "--worker-manager-id",
                    "wm-test",
                    "--aws-region",
                    "us-east-1",
                    "--python-version",
                    "3.13",
                ],
            )

    def test_orb_aws_ec2_instance_tags_default_empty(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=_ORB_AWS_EC2_IMAGE_ARGV
        )
        self.assertEqual(config.instance_tags, {})

    def test_orb_aws_ec2_instance_tags_single_tag_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=[*_ORB_AWS_EC2_IMAGE_ARGV, "--instance-tags", "Name=my-worker"]
        )
        self.assertEqual(config.instance_tags, {"Name": "my-worker"})

    def test_orb_aws_ec2_instance_tags_multiple_tags_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=[*_ORB_AWS_EC2_IMAGE_ARGV, "--instance-tags", "Name=my-worker,Env=prod,Team=data"],
        )
        self.assertEqual(config.instance_tags, {"Name": "my-worker", "Env": "prod", "Team": "data"})

    def test_orb_aws_ec2_instance_tags_value_with_equals_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        config = ORBAWSEC2WorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=[*_ORB_AWS_EC2_IMAGE_ARGV, "--instance-tags", "Tag=key=val"]
        )
        self.assertEqual(config.instance_tags, {"Tag": "key=val"})

    def test_orb_aws_ec2_instance_tags_from_toml(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        section_data = {
            "type": "orb_aws_ec2",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "aws_region": "us-east-1",
            "image_id": "ami-0528819f94f4f5fa5",
            "instance_tags": "Name=my-worker,Env=prod",
        }
        config = ORBAWSEC2WorkerManagerConfig.parse_with_section("scaler_worker_manager", section_data, argv=[])
        self.assertEqual(config.instance_tags, {"Name": "my-worker", "Env": "prod"})

    def test_orb_aws_ec2_requirements_without_python_version_raises(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig

        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerManagerConfig.parse_with_section(
                "scaler_worker_manager",
                {},
                argv=[
                    "tcp://127.0.0.1:6378",
                    "--worker-manager-id",
                    "wm-test",
                    "--aws-region",
                    "us-east-1",
                    "--requirements-txt",
                    "opengris-scaler",
                ],
            )


def _make_orb_config(
    *,
    image_id: Optional[str] = None,
    python_version: Optional[str] = None,
    requirements_txt: Optional[str] = None,
    aws_region: str = "us-east-1",
):
    wmc = WorkerManagerConfig(
        scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:6378"), worker_manager_id="wm-test"
    )
    return ORBAWSEC2WorkerManagerConfig(
        worker_manager_config=wmc,
        image_id=image_id,
        python_worker_environment=PythonWorkerEnvironmentConfig(
            python_version=python_version, requirements_txt=requirements_txt
        ),
        aws_region=aws_region,
        worker_config=WorkerConfig(),
        logging_config=LoggingConfig(),
    )


class TestORBAWSEC2CreateUserData(unittest.TestCase):
    """Tests for ORBAWSEC2WorkerManager._create_user_data covering the two environment modes."""

    def _make_worker_manager(self, **kwargs):
        from scaler.worker_manager.nested.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager

        return ORBAWSEC2WorkerManager(_make_orb_config(**kwargs))

    def test_image_id_mode_skips_install(self) -> None:
        worker_manager = self._make_worker_manager(image_id="ami-abc123")
        script = worker_manager._create_user_data()
        self.assertNotIn("dnf", script)
        self.assertNotIn("pip install", script)
        self.assertNotIn("venv", script)

    def test_auto_install_mode_installs_python(self) -> None:
        worker_manager = self._make_worker_manager(python_version="3.13", requirements_txt="opengris-scaler>=1.26.6")
        script = worker_manager._create_user_data()
        self.assertIn("--python 3.13", script)

    def test_auto_install_mode_embeds_literal_requirements(self) -> None:
        worker_manager = self._make_worker_manager(
            python_version="3.13", requirements_txt="opengris-scaler>=1.26.6\nboto3"
        )
        script = worker_manager._create_user_data()
        self.assertIn("opengris-scaler>=1.26.6", script)
        self.assertIn("boto3", script)
        self.assertIn("pip install -r /tmp/requirements.txt", script)

    def test_auto_install_mode_reads_requirements_file(self) -> None:
        import unittest.mock

        file_content = "opengris-scaler>=1.26.6\nboto3\nnumpy\n"
        with (
            unittest.mock.patch("os.path.isfile", return_value=True),
            unittest.mock.patch("builtins.open", unittest.mock.mock_open(read_data=file_content)),
        ):
            worker_manager = self._make_worker_manager(
                python_version="3.13", requirements_txt="/path/to/requirements.txt"
            )
            script = worker_manager._create_user_data()

        self.assertIn("opengris-scaler>=1.26.6", script)
        self.assertIn("numpy", script)
        self.assertIn("pip install -r /tmp/requirements.txt", script)

    def test_image_id_mode_launches_worker_manager(self) -> None:
        worker_manager = self._make_worker_manager(image_id="ami-abc123")
        script = worker_manager._create_user_data()
        self.assertIn("scaler_worker_manager baremetal_native", script)

    def test_auto_install_mode_launches_worker_manager(self) -> None:
        worker_manager = self._make_worker_manager(python_version="3.13", requirements_txt="opengris-scaler>=1.26.6")
        script = worker_manager._create_user_data()
        self.assertIn("scaler_worker_manager baremetal_native", script)


class TestWorkerManagerMain(unittest.TestCase):
    """Tests for the main() entry point dispatch and error handling."""

    def test_no_matching_type_exits(self) -> None:
        """When --config is provided but has no matching type, exit with error."""
        toml_content = b"""
[[worker_manager]]
type = "symphony"
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-sym"
service_name = "svc"
"""
        with patch("builtins.open", mock_open(read_data=toml_content)):
            with patch("sys.argv", ["scaler_worker_manager", "baremetal_native", "--config", "cfg.toml"]):
                from scaler.entry_points.worker_manager import main

                with self.assertRaises(SystemExit) as ctx:
                    main()
                self.assertEqual(ctx.exception.code, 1)

    def test_multiple_matching_types_exits(self) -> None:
        """When config has two entries of the same type, exit with error."""
        toml_content = b"""
[[worker_manager]]
type = "baremetal_native"
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-1"

[[worker_manager]]
type = "baremetal_native"
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-2"
"""
        with patch("builtins.open", mock_open(read_data=toml_content)):
            with patch("sys.argv", ["scaler_worker_manager", "baremetal_native", "--config", "cfg.toml"]):
                from scaler.entry_points.worker_manager import main

                with self.assertRaises(SystemExit) as ctx:
                    main()
                self.assertEqual(ctx.exception.code, 1)

    def test_unknown_type_exits(self) -> None:
        """Unknown subcommand exits with code 1."""
        with patch("sys.argv", ["scaler_worker_manager", "nonexistent"]):
            from scaler.entry_points.worker_manager import main

            with self.assertRaises(SystemExit) as ctx:
                main()
            self.assertEqual(ctx.exception.code, 1)

    @unittest.skipUnless(importlib.util.find_spec("oci") is not None, "oci SDK not installed")
    def test_oci_hpc_subcommand_dispatches_worker_manager(self) -> None:
        with (
            patch("sys.argv", ["scaler_worker_manager", "oci_hpc", *_OCI_HPC_BASE_ARGV]),
            patch("scaler.entry_points.worker_manager.bootstrap_process"),
            patch("scaler.entry_points.worker_manager.register_event_loop"),
            patch("scaler.worker_manager.proxy.oci.worker_manager.OCIWorkerManager") as mock_mgr,
        ):
            mock_mgr.return_value.run.return_value = None
            from scaler.entry_points.worker_manager import main

            main()

        mock_mgr.assert_called_once()
        mock_mgr.return_value.run.assert_called_once()


class TestWorkerSchedulerAddress(unittest.TestCase):
    """Tests for WorkerManagerConfig.worker_scheduler_address and effective_worker_scheduler_address."""

    def test_effective_address_falls_back_to_scheduler_address(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378"]
        )
        self.assertIsNone(config.worker_manager_config.worker_scheduler_address)
        self.assertEqual(
            config.worker_manager_config.effective_worker_scheduler_address,
            config.worker_manager_config.scheduler_address,
        )

    def test_worker_scheduler_address_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=[
                "--worker-manager-id",
                "wm-test",
                "tcp://127.0.0.1:6378",
                "--worker-scheduler-address",
                "tcp://203.0.113.5:6378",
            ],
        )
        self.assertIsNotNone(config.worker_manager_config.worker_scheduler_address)
        self.assertEqual(
            repr(config.worker_manager_config.effective_worker_scheduler_address), "tcp://203.0.113.5:6378"
        )
        self.assertEqual(repr(config.worker_manager_config.scheduler_address), "tcp://127.0.0.1:6378")

    def test_worker_scheduler_address_short_flag(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "-wsa", "tcp://203.0.113.5:6378"],
        )
        self.assertEqual(
            repr(config.worker_manager_config.effective_worker_scheduler_address), "tcp://203.0.113.5:6378"
        )

    def test_worker_scheduler_address_from_toml(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        section_data = {
            "type": "baremetal_native",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_scheduler_address": "tcp://203.0.113.5:6378",
            "worker_manager_id": "wm-test",
        }
        config = NativeWorkerManagerConfig.parse_with_section("scaler_worker_manager", section_data, argv=[])
        self.assertEqual(
            repr(config.worker_manager_config.effective_worker_scheduler_address), "tcp://203.0.113.5:6378"
        )
