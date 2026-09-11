import argparse
import enum
import typing
from typing import Any

from scaler.config.mixins import ConfigType


def parse_bool(s: str) -> bool:
    """parse a bool from a conventional string representation"""
    lower = s.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    raise argparse.ArgumentTypeError(f"'{s}' is not a valid bool")


def parse_enum(s: str, enumm: type[enum.Enum]) -> Any:
    try:
        return enumm[s]
    except KeyError as e:
        raise argparse.ArgumentTypeError(f"'{s}' is not a valid {enumm.__name__}") from e


def is_optional(ty: Any) -> bool:
    """determines if `ty` is typing.Optional"""
    return typing.get_origin(ty) is typing.Union and typing.get_args(ty)[1] is type(None)


def is_union(ty: Any) -> bool:
    """determines if `ty` is a typing.Union (excludes Optional, i.e. Union[T, None])"""
    if typing.get_origin(ty) is not typing.Union:
        return False
    return type(None) not in typing.get_args(ty)


def get_union_args(ty: Any) -> tuple:
    """get the args of a typing.Union[...]"""
    return typing.get_args(ty)


def get_optional_type(ty: Any) -> type:
    """get the `T` from a typing.Optional[T]"""
    return typing.get_args(ty)[0]


def is_list(ty: Any) -> bool:
    """determines if `ty` is list or list"""
    return typing.get_origin(ty) is list or ty is list


def get_list_type(ty: Any) -> type:
    """get the generic type of a list[T] or list[T]"""
    return typing.get_args(ty)[0]


def is_config_type(ty: Any) -> bool:
    """determines if ty is a subclass of ConfigType"""
    try:
        return issubclass(ty, ConfigType)
    except TypeError:
        return False


def is_config_class(ty: Any) -> bool:
    """determines if ty is a subclass of ConfigClass"""
    from scaler.config.config_class import ConfigClass

    try:
        return issubclass(ty, ConfigClass)
    except TypeError:
        return False


def is_enum(ty: Any) -> bool:
    """determines if ty is a subclass of Enum"""
    try:
        return issubclass(ty, enum.Enum)
    except TypeError:
        return False


def get_type_args(ty: Any) -> dict[str, Any]:
    """
    The type of a field implies several options for its argument parsing,
    such as `type`, `nargs`, and `required`

    For example a parameter of type Option[T] is parsed as `T`,
    has no implication on `nargs`, and is not required

    Similarly a parameter of list[T] is also parsed as `T`,
    might have `nargs="*"`, and has no implication on `required`

    This function determines these settings based upon a given type.
    """

    # bools have special parsing so that they behave as users expect
    if ty is bool:
        return {"type": parse_bool}

    # for subclasses of ConfigType, we use the .from_string() method
    if is_config_type(ty):
        return {"type": ty.from_string}

    # recursing handles the case where e.g. the inner type is a bool
    # or a subclass of ConfigType, both of which need special handling
    #
    # parameters with this type are optional so we set `required=False`
    if is_optional(ty):
        return {**get_type_args(get_optional_type(ty)), "required": False}

    # `nargs="*"` is a reasonable default for lists that be overriden by the user
    # if other behaviour is desired
    if is_list(ty):
        opts = get_type_args(get_list_type(ty))
        return {"type": opts["type"], "nargs": "*"}

    # for enums we get their value by name
    if is_enum(ty):
        return {"type": lambda name: parse_enum(name, ty)}

    # the default, just use the type as-is
    return {"type": ty}
