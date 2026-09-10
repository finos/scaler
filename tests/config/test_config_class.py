import dataclasses
import unittest
from enum import Enum
from typing import Dict, List, Optional, Tuple
from unittest.mock import mock_open, patch

from scaler.config.config_class import ConfigClass
from scaler.config.mixins import ConfigType
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.type_utils import parse_bool

try:
    from typing import override  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import override

try:
    from typing import Self  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Self


class MockArgParser:
    args: List[Tuple[Tuple, Dict]]

    def __init__(self, *args, **kwargs) -> None:
        self.args = []

    def add_argument(self, *args, **kwargs) -> None:
        self.args.append((args, kwargs))


class TestConfigClass(unittest.TestCase):
    """Tests the behavior of ConfigClass"""

    def test_config_class(self) -> None:
        def parse_hex(s: str) -> int:
            return int(s, 16)

        class MyConfigType(ConfigType):
            _s: str

            def __init__(self, s: str) -> None:
                self._s = s

            @override
            @classmethod
            def from_string(cls, value: str) -> Self:
                return cls(value)

            @override
            def __str__(self) -> str:
                return self._s

        @dataclasses.dataclass
        class MyConfig(ConfigClass):
            my_int: int
            positional_one: str = dataclasses.field(metadata=dict(positional=True))
            positional_two: str = dataclasses.field(metadata=dict(positional=True))

            config_type: MyConfigType = dataclasses.field(metadata=dict(short="-ct", help="help"))

            optional_int: Optional[int]
            optional_config_type: Optional[MyConfigType]

            a_bool: bool
            flag: bool = dataclasses.field(metadata=dict(action="store_true"))

            list_one: List[int]
            list_two: List[int] = dataclasses.field(metadata=dict(nargs="+"))

            custom_type: int = dataclasses.field(metadata=dict(type=parse_hex))

            with_default: int = 42

            passthrough: int = dataclasses.field(
                default=-1, metadata=dict(any=0, field=1, can=2, be=3, passed=4, through=5)
            )

            renamed: int = dataclasses.field(default=0, metadata=dict(long="--new-name"))

        parser = MockArgParser()
        MyConfig.configure_parser(parser)  # type: ignore[arg-type]
        args = parser.args

        # Q: What is the "dest" kwarg?
        # A: It sets the key returned in dict when arguments are parsed
        # it helps us be more robust, we know exactly what the key will be
        self.assertEqual(args[0], (("--my-int",), {"type": int, "dest": "my_int"}))
        self.assertEqual(args[1], (("positional_one",), {"type": str}))
        self.assertEqual(args[2], (("positional_two",), {"type": str}))
        self.assertEqual(
            args[3],
            (("--config-type", "-ct"), {"type": MyConfigType.from_string, "help": "help", "dest": "config_type"}),
        )
        self.assertEqual(args[4], (("--optional-int",), {"type": int, "required": False, "dest": "optional_int"}))
        self.assertEqual(
            args[5],
            (
                ("--optional-config-type",),
                {"type": MyConfigType.from_string, "required": False, "dest": "optional_config_type"},
            ),
        )
        self.assertEqual(args[6], (("--a-bool",), {"type": parse_bool, "dest": "a_bool"}))
        self.assertEqual(args[7], (("--flag",), {"action": "store_true", "dest": "flag"}))
        self.assertEqual(args[8], (("--list-one",), {"type": int, "nargs": "*", "dest": "list_one"}))
        self.assertEqual(args[9], (("--list-two",), {"type": int, "nargs": "+", "dest": "list_two"}))
        self.assertEqual(args[10], (("--custom-type",), {"type": parse_hex, "dest": "custom_type"}))
        self.assertEqual(args[11], (("--with-default",), {"type": int, "default": 42, "dest": "with_default"}))
        self.assertEqual(
            args[12],
            (
                ("--passthrough",),
                {
                    "type": int,
                    "default": -1,
                    "any": 0,
                    "field": 1,
                    "can": 2,
                    "be": 3,
                    "passed": 4,
                    "through": 5,
                    "dest": "passthrough",
                },
            ),
        )
        self.assertEqual(args[13], (("--new-name",), {"type": int, "default": 0, "dest": "renamed"}))

    @patch("sys.argv", ["script"])
    def test_empty(self) -> None:
        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            pass

        MyConfigClass.parse("this is a test config", "my_config")

    @patch("sys.argv", ["script", "--config", "file", "--command-line", "99"])
    @patch.dict("os.environ", {"ENV_VAR_ONE": "99", "ENV_VAR_TWO": "98"})
    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [my_config]
            config-file = 99

            [unused_section]
            another-one = 97
            """),
    )
    def test_precedence(self) -> None:
        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            default: int = dataclasses.field(default=0)
            config_file: int = dataclasses.field(default=1)
            env_var: int = dataclasses.field(default=2, metadata=dict(env_var="ENV_VAR_ONE"))
            command_line: int = dataclasses.field(default=3, metadata=dict(env_var="ENV_VAR_TWO"))

        config = MyConfigClass.parse("this is a test config", "my_config")

        self.assertEqual(config, MyConfigClass(default=0, config_file=99, env_var=99, command_line=99))

    @patch("sys.argv", ["script", "--outer", "0", "--inner", "1"])
    def test_config_class_field(self) -> None:
        @dataclasses.dataclass
        class InnerConfig(ConfigClass):
            inner: int

        @dataclasses.dataclass
        class OuterConfig(ConfigClass):
            outer: int

            # nested
            inner_config: InnerConfig

        config = OuterConfig.parse(program_name="outer", section="the outer config")

        self.assertEqual(config.outer, 0)
        self.assertEqual(config.inner_config.inner, 1)

    @patch("sys.argv", ["script", "--color", "RED"])
    def test_enum_field(self) -> None:
        class Color(Enum):
            RED = "red"
            GREEN = "green"
            BLUE = "blue"

        @dataclasses.dataclass
        class MyConfig(ConfigClass):
            color: Color

        config = MyConfig.parse(program_name="script", section="script")

        self.assertEqual(config.color, Color.RED)

    @patch("sys.argv", ["script", "--config", "file"])
    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [my_config]
            my_int = 10
            my-other-int = 20
            """),
    )
    def test_underscore_toml_parsing(self) -> None:
        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            my_int: int
            my_other_int: int

        config = MyConfigClass.parse("test underscore", "my_config")
        self.assertEqual(config.my_int, 10)
        self.assertEqual(config.my_other_int, 20)

    @patch("sys.argv", ["script", "--config", "file"])
    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [my_config]
            color = "RED"
            """),
    )
    def test_enum_from_toml(self) -> None:
        """Enum fields loaded from TOML should be converted to the enum type, not left as strings."""

        class Color(Enum):
            RED = "red"
            GREEN = "green"
            BLUE = "blue"

        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            color: Color

        config = MyConfigClass.parse("test enum toml", "my_config")
        self.assertEqual(config.color, Color.RED)
        self.assertIsInstance(config.color, Color)

    @patch("sys.argv", ["script", "--config", "file"])
    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [my_config]
            my_value = "hello"
            """),
    )
    def test_config_type_from_toml(self) -> None:
        """ConfigType fields loaded from TOML should be converted via from_string(), not left as strings."""

        class MyConfigType(ConfigType):
            def __init__(self, value: str) -> None:
                self.value = value

            @classmethod
            def from_string(cls, value: str) -> "MyConfigType":
                return cls(value.upper())

            def __eq__(self, other: object) -> bool:
                return isinstance(other, MyConfigType) and self.value == other.value

            def __str__(self) -> str:
                return self.value

        @dataclasses.dataclass
        class MyConfigClass(ConfigClass):
            my_value: MyConfigType

        config = MyConfigClass.parse("test config_type toml", "my_config")
        self.assertIsInstance(config.my_value, MyConfigType)
        self.assertEqual(config.my_value, MyConfigType("HELLO"))


class TestPreloadCLIArgument(unittest.TestCase):
    """Tests that --preload is properly exposed as a CLI argument on worker manager configs."""

    def test_native_worker_manager_config_has_preload_argument(self) -> None:
        parser = MockArgParser()
        NativeWorkerManagerConfig.configure_parser(parser)  # type: ignore[arg-type]

        preload_args = [a for a in parser.args if "--preload" in a[0]]
        self.assertEqual(len(preload_args), 1)

        args, kwargs = preload_args[0]
        self.assertIn("--preload", args)
        self.assertEqual(kwargs["dest"], "preload")
        self.assertEqual(kwargs["required"], False)
        self.assertEqual(kwargs["default"], None)
        self.assertIn("help", kwargs)

    def test_ecs_worker_manager_config_has_preload_argument(self) -> None:
        parser = MockArgParser()
        ECSWorkerManagerConfig.configure_parser(parser)  # type: ignore[arg-type]

        preload_args = [a for a in parser.args if "--preload" in a[0]]
        self.assertEqual(len(preload_args), 1)

        args, kwargs = preload_args[0]
        self.assertIn("--preload", args)
        self.assertEqual(kwargs["dest"], "preload")
        self.assertEqual(kwargs["required"], False)
        self.assertEqual(kwargs["default"], None)
        self.assertIn("help", kwargs)

    @patch(
        "sys.argv",
        [
            "script",
            "tcp://127.0.0.1:8516",
            "--worker-manager-id",
            "test_wm",
            "--preload",
            "mypackage.init:setup('production')",
        ],
    )
    def test_native_worker_manager_config_parses_preload(self) -> None:
        config = NativeWorkerManagerConfig.parse("test", "native_worker_manager")
        self.assertEqual(config.worker_config.preload, "mypackage.init:setup('production')")

    @patch("sys.argv", ["script", "tcp://127.0.0.1:8516", "--worker-manager-id", "test_wm"])
    def test_native_worker_manager_config_preload_defaults_to_none(self) -> None:
        config = NativeWorkerManagerConfig.parse("test", "native_worker_manager")
        self.assertIsNone(config.worker_config.preload)
