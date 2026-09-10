import argparse
import dataclasses
import sys
from typing import Any, Dict, List, Optional, Type, TypeVar

import argcomplete

from scaler.config.loading import _env_defaults, _find_config_arg, _load_toml, _toml_section_defaults
from scaler.config.reconstruction import _from_args, _from_args_with_subcommands

# Re-export parse_bool so existing imports from this module continue to work.
from scaler.config.type_utils import get_optional_type, get_type_args, is_config_class, is_optional

T = TypeVar("T", bound="ConfigClass")


class ConfigClass:
    """
    An abstract interface for dataclasses where the fields define command line options,
    config file options, and environment variables.

    Subclasses of `ConfigClass` must be dataclasses.
    Subclasses of `ConfigClass` are called "config classes".

    ## Config Files

    All options with a long name can be parsed from a TOML config file specified with `--config` or `-c`.
    The section name is determined by the subclass' implementation of `.section_name()`.

    Keys in the config file correspond to the long name of the argument
    and may use underscores or hyphens interchangeably.

    For example, given the config class:
    ```python
    @dataclasses.dataclass
    class MyConfig(ConfigClass):
        my_field: int = 5
    ```

    Both of the following config files are valid:
    ```
    [my_section]
    my_field = 10
    ```

    ```
    [my_section]
    my-field = 10
    ```

    When a field has an explicit `long` name that differs from the field name,
    the TOML key must use the long name (without `--`):
    ```python
    @dataclasses.dataclass
    class MyConfig(ConfigClass):
        level: str = dataclasses.field(default="INFO", metadata=dict(long="--logging-level"))
    ```

    ```
    [my_section]
    logging_level = "DEBUG"   # correct - matches long name --logging-level
    level = "DEBUG"           # will NOT be recognised
    ```

    ## Environment Variables

    Any parameter can be configured to read from an environment variable by adding `env_var="NAME"` to the field
    metadata.

    ```python
    # can be set as --my-field on the command line or in a config file, or using the environment variable `NAME`
    my_field: int = dataclasses.field(metadata=dict(env_var="NAME"))
    ```

    ## Precedence

    When a parameter is supplied in multiple ways, the following precedence is observed:
    command line > environment variables > config file > defaults

    ## Customization

    Any key values included in the field's metadata will be passed through to `.add_argument()`
    and will always take precedence over derived values, except for `default`.

    The value for `default` is taken from the field's default. Providing `default` in the field
    metadata will result in a `TypeError`.

    ```python
    # passes through options
    custom_field: int = dataclasses.field(
        metadata=dict(
            choices=["apples", "oranges"],
            help="choose a fruit",
        )
    )

    # TypeError!
    bad: int = dataclasses.field(metadata=dict(default=0))
    ```

    ## Naming Fields

    The name of the dataclass fields (replacing underscores with hyphens) is used
    as the long option name for command line and config file parameters.
    A short name for the argument can be provided in the field metadata using the `short` key.
    The value is the short option name and must include the hyphen, e.g. `-n`.

    There is one restriction: `--config` and `-c` are reserved for the config file unless
    `disable_config_flag=True` is passed to `.parse()`.

    ```python
    # this will have long name --field-one, and no short name
    field_one: int = 5

    # sets a short name
    field_two: int = dataclasses.field(default=5, metadata=dict(short="-f2"))
    ```

    ## Default Values

    The default value of the field is also used as the default for the argument parser.

    ```python
    lang: str = "en"
    name: str = dataclasses.field(default="Arthur")
    ```

    ## Positional Parameters

    You can set `positional=True` in the metadata dict to make an argument positional.
    In this case long and short names are ignored, use `name` to override the name of the option.
    The position is dependent on field ordering.

    ```python
    # both of these are positional, and field one must be specified before field two
    field_one: int = dataclasses.field(metadata=dict(positional=True))
    field_two: int = dataclasses.field(metadata=dict(positional=True))
    ```

    ## Sub-commands

    A field with `subcommand="<toml_section>"` in its metadata declares a sub-command:
    - **Field name** -> CLI sub-command name
    - **`subcommand` value** -> TOML section to read when this sub-command is active
    - **Field type** -> `Optional[SomeConfigClass]` with `default=None`

    ## Section Fields

    A field with `section="<toml_section>"` in its metadata is populated from the TOML file only
    (no CLI argument). The field type may be `Optional[SomeConfigClass]` or `List[SomeConfigClass]`.

    ## Composition

    Config classes can be composed. If a config class has fields that are config classes,
    then the options of the child config class are inherited as if that child config class'
    fields were added to the parent, for the purpose of parsing arguments. When the dataclass
    is created, the structure is kept.

    Care needs to be taken so that field names do not conflict with each other.
    The name of fields in nested config classes are in the same namespace as those
    in the parent and other nested config classes.

    ## Enums

    Enums are parsed by their name, for instance the enum:

    ```python
    class Color(Enum):
        RED = "red"
        GREEN = "green"
        BLUE = "blue"
    ```

    Will accept "RED", "GREEN", and "BLUE" as arguments on the command line.
    As usual this can be overriden by explicitly setting the `type` field.

    ## Parameter Types

    The type of a field is used as the type in argument parsing, meaning that it must be able
    to be directly constructed from a string.

    Special handling is implemented for several common types:
    - `bool`: parses from "true" and "false", and all upper/lower case variants
    - subclasses of `ConfigType`: uses the `.from_string()` method
    - `Optional[T]`, `T | None`: parsed as `T` and sets `required=False`
    - `List[T]`, `list[T]`: parsed as `T`, and sets `nargs="*"`
    - sublcasses of `enum.Enum`: values are parsed by name

    For generic types, the `T` is parsed recursively following the rules here.
    For example, `Optional[T]` where `T` is a subclass of `ConfigType`
    will still use `T.from_string()`.

    as usual, all of these can be overriden by setting the option in the metadata.

    ```python
    # this provides a custom `type` to parse the input as hexadecimal
    # `type` must be a callable that accepts a string
    # refer to the argparse docs for more
    hex: int = dataclasses.field(metadata=dict(type=lambda s: int(s, 16)))

    class MyConfigType(ConfigType):
        ...

    # this will work as expected
    my_field: MyConfigType

    # this requires special handling
    tuples: Tuple[int, ...] = dataclasses.field(metadata=dict(type=int, nargs="*"))

    # works automatically, defaults to `nargs="*"`
    integers: List[int]

    # ... but we can override that
    integers2: List[int] = dataclasses.field(metadata=dict(nargs="+"))

    # this will automatically have `required=False` set
    maybe: Optional[str]
    """

    @classmethod
    def configure_parser(cls: type, parser: argparse.ArgumentParser) -> None:
        for field in dataclasses.fields(cls):
            if "subcommand" in field.metadata or "section" in field.metadata:
                continue  # handled by parse()

            if is_config_class(field.type):
                field.type.configure_parser(parser)  # type: ignore[union-attr]
                continue

            # Strip keys that argparse doesn't understand
            kwargs = {
                k: v
                for k, v in field.metadata.items()
                if k not in ("env_var", "positional", "long", "short", "name", "subcommand", "section")
            }

            is_positional = field.metadata.get("positional", False)
            if is_positional:
                args = [field.metadata.get("name", field.name)]
            else:
                long_name = field.metadata.get("long", f"--{field.name.replace('_', '-')}")
                args = [long_name, field.metadata["short"]] if "short" in field.metadata else [long_name]
                kwargs["dest"] = field.name

            if "default" in kwargs:
                raise TypeError("'default' cannot be provided in field metadata")

            if field.default is not dataclasses.MISSING:
                kwargs["default"] = field.default

            if field.default_factory is not dataclasses.MISSING:
                kwargs["default"] = field.default_factory()

            # when store_true or store_false is set, setting the type raises a type error
            if kwargs.get("action") not in ("store_true", "store_false"):
                # sometimes the user will set the type manually
                if "type" not in kwargs:
                    for key, value in get_type_args(field.type).items():
                        if key not in kwargs:
                            kwargs[key] = value

            if is_positional:
                kwargs.pop("required", None)  # argparse rejects required= for positionals
            parser.add_argument(*args, **kwargs)

    @classmethod
    def parse(cls: Type[T], program_name: str, section: str, disable_config_flag: bool = False) -> T:
        subcommand_fields = [
            field for field in dataclasses.fields(cls) if "subcommand" in field.metadata  # type: ignore[arg-type]
        ]

        if subcommand_fields:
            # Root parser: routes to sub-commands.
            parser = argparse.ArgumentParser(prog=program_name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
            parser.add_argument("--config", "-c", metavar="FILE", help="Path to the TOML configuration file.")

            # Pre-scan for --config before building the subparser tree so TOML
            # defaults can be injected into each subparser during construction.
            config_path = _find_config_arg(sys.argv)
            toml_data = _load_toml(config_path) if config_path else {}

            _build_subparser_tree(cls, parser, parent_cls=cls, sections=[section], dest="_sub", toml_data=toml_data)
            argcomplete.autocomplete(parser)

            kwargs = vars(parser.parse_args())
            kwargs.pop("config", None)

            return _from_args_with_subcommands(cls, kwargs, dest="_sub")

        # Normal path.
        parser = argparse.ArgumentParser(prog=program_name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        if not disable_config_flag:
            parser.add_argument("--config", "-c", metavar="FILE", help="Path to the TOML configuration file.")
        cls.configure_parser(parser)
        argcomplete.autocomplete(parser)

        # Pass 1: locate --config without failing on unrecognised args.
        config_path_str: Optional[str] = vars(parser.parse_known_args()[0]).get("config")

        # Load TOML and inject section values as defaults (below CLI, above hardcoded).
        toml_data: Dict[str, Any] = {}  # type: ignore[no-redef]
        injected_dests: Dict[str, Any] = {}
        if config_path_str:
            toml_data = _load_toml(config_path_str)
            toml_defs = _toml_section_defaults(toml_data.get(section, {}), cls)
            if toml_defs:
                parser.set_defaults(**toml_defs)
                injected_dests.update(toml_defs)

        # Env-var defaults override TOML but are overridden by CLI.
        env_defs = _env_defaults(cls)
        if env_defs:
            parser.set_defaults(**env_defs)
            injected_dests.update(env_defs)

        # argparse does not consider set_defaults() values as satisfying required=True.
        # Relax the required flag for any optional argument whose value was provided by
        # TOML or an env var so that those sources count as valid.
        for action in parser._actions:
            if action.required and action.dest in injected_dests:
                action.required = False

        # Pass 2: full parse - CLI wins.
        kwargs = vars(parser.parse_args())
        if not disable_config_flag:
            kwargs.pop("config", None)

        # Populate section= fields from the raw TOML (not from parsed args).
        section_fields = [f for f in dataclasses.fields(cls) if "section" in f.metadata]  # type: ignore[arg-type]
        full_toml = toml_data if section_fields else None

        return _from_args(cls, kwargs, full_toml)

    @classmethod
    def parse_with_section(
        cls: Type[T], program_name: str, section_data: Dict[str, Any], argv: Optional[List[str]] = None
    ) -> T:
        """Parse CLI args using section_data as pre-loaded TOML defaults.

        Unlike parse(), this method does not load any config file - the caller
        is responsible for pre-loading the relevant TOML section and passing it
        as section_data.  argv defaults to sys.argv[1:] when None.

        This is used by entry points that do their own TOML lookup (for example
        scaler_worker_manager, which scans [[worker_manager]] entries by type)
        before delegating field parsing to the appropriate config class.
        """
        parser = argparse.ArgumentParser(prog=program_name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        cls.configure_parser(parser)
        argcomplete.autocomplete(parser)

        toml_defs = _toml_section_defaults(section_data, cls)
        injected_dests: Dict[str, Any] = {}
        if toml_defs:
            parser.set_defaults(**toml_defs)
            injected_dests.update(toml_defs)

        env_defs = _env_defaults(cls)
        if env_defs:
            parser.set_defaults(**env_defs)
            injected_dests.update(env_defs)

        for action in parser._actions:
            if action.required and action.dest in injected_dests:
                action.required = False

        kwargs = vars(parser.parse_args(argv))
        return _from_args(cls, kwargs)


def _build_subparser_tree(
    cls: type,
    parser: argparse.ArgumentParser,
    parent_cls: type,
    sections: List[str],
    dest: str,
    toml_data: Dict[str, Any],
) -> None:
    """Recursively add subparsers to *parser* for every subcommand field in *cls*.

    Args:
        cls:        The config class whose subcommand fields are being registered.
        parser:     The (sub)parser to attach the new subparsers to.
        parent_cls: The config class that owns *parser*; its non-subcommand fields
                    are registered on each subparser so they are available at every level.
        sections:   Accumulated TOML section names from all ancestor levels.
        dest:       Unique argparse dest name for this level's chosen subcommand.
        toml_data:  Full parsed TOML dict loaded from --config (may be empty).
    """
    subcommand_fields = [f for f in dataclasses.fields(cls) if "subcommand" in f.metadata]
    if not subcommand_fields:
        return

    subparsers = parser.add_subparsers(dest=dest, required=True)

    for field in subcommand_fields:
        sub_section = field.metadata["subcommand"]
        config_cls = get_optional_type(field.type) if is_optional(field.type) else field.type
        level_sections = [s for s in sections + [sub_section] if s]

        subparser = subparsers.add_parser(field.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        subparser.add_argument("--config", "-c", metavar="FILE", help="Path to the TOML configuration file.")
        parent_cls.configure_parser(subparser)  # type: ignore[attr-defined]  # parent-level fields
        config_cls.configure_parser(subparser)  # type: ignore[union-attr]  # this sub-command's own fields

        # Inject TOML defaults: merge all ancestor sections + this sub-command's section.
        combined: Dict[str, Any] = {}
        for s in level_sections:
            combined.update(toml_data.get(s, {}))
        toml_defs = _toml_section_defaults(combined, config_cls)  # type: ignore[arg-type]
        injected_dests: Dict[str, Any] = {}
        if toml_defs:
            subparser.set_defaults(**toml_defs)
            injected_dests.update(toml_defs)

        # Inject TOML defaults for parent-class ConfigClass fields (e.g. logging) from
        # sections named after the field - e.g. [logging] -> LoggingConfig defaults.
        for parent_field in dataclasses.fields(parent_cls):
            if "subcommand" in parent_field.metadata or "section" in parent_field.metadata:
                continue
            if is_config_class(parent_field.type):
                parent_section_data = toml_data.get(parent_field.name, {})
                if isinstance(parent_section_data, dict):
                    parent_section_defs = _toml_section_defaults(
                        parent_section_data, parent_field.type  # type: ignore[arg-type]
                    )
                    if parent_section_defs:
                        subparser.set_defaults(**parent_section_defs)
                        injected_dests.update(parent_section_defs)

        # Env-var defaults override TOML.
        env_defs = _env_defaults(config_cls)  # type: ignore[arg-type]
        if env_defs:
            subparser.set_defaults(**env_defs)
            injected_dests.update(env_defs)

        # argparse does not consider set_defaults() values as satisfying required=True.
        # Relax the required flag for any optional argument whose value was provided by
        # TOML or an env var so that those sources count as valid.
        for action in subparser._actions:
            if action.required and action.dest in injected_dests:
                action.required = False

        _build_subparser_tree(
            config_cls,  # type: ignore[arg-type]
            subparser,
            config_cls,  # type: ignore[arg-type]
            sections=level_sections,
            dest=f"{dest}_{field.name}",
            toml_data=toml_data,
        )
