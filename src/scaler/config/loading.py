import dataclasses
import os
from typing import Any, Optional

from scaler.config.type_utils import get_type_args, is_config_class


def _find_config_arg(argv: list[str]) -> Optional[str]:
    """Pre-scan argv for --config/-c without invoking the full parser."""
    for i, arg in enumerate(argv):
        if arg in ("--config", "-c") and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--config="):
            return arg.split("=", 1)[1]
    return None


def _load_toml(config_path: str) -> dict[str, Any]:
    """Parse a TOML file and return its full contents as a dict."""
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        import tomli as tomllib
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def _toml_section_defaults(section_data: dict[str, Any], cls: type) -> dict[str, Any]:
    """Flatten a raw TOML section dict into argparse-compatible defaults.

    Keys correspond to the long CLI argument name (without '--'), and may use
    underscores or hyphens interchangeably. For fields without an explicit long
    name, the field name is used (which equals the auto-derived long name).
    Only returns keys that correspond to actual fields on cls (and its nested
    ConfigClass fields), so unrelated TOML keys are silently ignored.
    """
    # Map normalized TOML key -> argparse dest (field name).
    key_to_dest: dict[str, str] = {}
    for f in dataclasses.fields(cls):  # type: ignore[arg-type]
        if is_config_class(f.type):
            for ff in dataclasses.fields(f.type):  # type: ignore[arg-type]
                long = ff.metadata.get("long", f"--{ff.name.replace('_', '-')}")
                key_to_dest[long.lstrip("-").replace("-", "_")] = ff.name
        elif "subcommand" not in f.metadata and "section" not in f.metadata:
            long = f.metadata.get("long", f"--{f.name.replace('_', '-')}")
            key_to_dest[long.lstrip("-").replace("-", "_")] = f.name

    result: dict[str, Any] = {}
    for key, value in section_data.items():
        dest = key_to_dest.get(key.replace("-", "_"))
        if dest is not None:
            result[dest] = value
    return result


def _env_defaults(cls: type) -> dict[str, Any]:
    """Collect env-var values for fields that declare env_var= metadata.

    Applies the same type coercion that argparse would use for CLI values,
    so the value stored as a default is already the correct Python type.
    """
    result: dict[str, Any] = {}
    for field in dataclasses.fields(cls):  # type: ignore[arg-type]
        if is_config_class(field.type):
            result.update(_env_defaults(field.type))  # type: ignore[arg-type]
            continue
        env_name = field.metadata.get("env_var")
        if env_name and env_name in os.environ:
            raw = os.environ[env_name]
            type_func = field.metadata.get("type") or get_type_args(field.type).get("type")
            result[field.name] = type_func(raw) if type_func else raw
    return result
