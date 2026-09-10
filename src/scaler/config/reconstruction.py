"""ConfigClass reconstruction helpers.

There are two distinct data sources that a ConfigClass can be built from, each
handled by a separate function family:

  TOML dict  ->  _from_toml(cls, dict)
  argparse   ->  _from_args(cls, kwargs)          (no subcommands)
               _from_args_with_subcommands(cls, kwargs, dest)   (with subcommands)

Call-graph summary
------------------
ConfigClass.from_args()
  +- _from_args_with_subcommands()   # if class has subcommand fields
       +- _from_args_with_subcommands()   # recurse for nested subcommands
       +- _from_args()              # for non-subcommand fields at each level
  +- _from_args()                  # no subcommands - direct path
       +- _from_toml()             # for every field that carries section= metadata

_from_toml() only calls itself (for nested ConfigClass fields inside a TOML
section).  It never calls _from_args or _from_args_with_subcommands.
"""

import dataclasses
from typing import Any, Dict, Optional, Type, TypeVar

from scaler.config.type_utils import (
    get_list_type,
    get_optional_type,
    get_type_args,
    get_union_args,
    is_config_class,
    is_list,
    is_optional,
    is_union,
)

T = TypeVar("T")


def _from_toml(config_cls: Type[T], data: Dict[str, Any]) -> T:
    """Build a ConfigClass instance from a raw TOML section dict.

    This is the TOML-only reconstruction path.  It is called by _from_args
    whenever a field carries ``section=`` metadata, meaning its value comes
    directly from a TOML section rather than from CLI args.

    Key behaviours
    --------------
    * Key normalisation: TOML keys may use hyphens (e.g. ``num-workers``);
      these are converted to underscores before matching against dataclass
      field names.

    * Long-name lookup: when a field has an explicit ``long`` name that
      differs from the field name (e.g. ``long="--logging-level"`` for a
      field named ``level``), only the normalised long name (``logging_level``)
      is accepted as a TOML key - not the field name.  This matches the
      behaviour of ``_toml_section_defaults`` so both entry points accept
      identical TOML keys.

    * Flat vs. nested layout: if a field's type is itself a ConfigClass, two
      TOML layouts are supported:
        - Nested (explicit sub-table): the value at ``data[field.name]`` is a
          dict, which is passed down recursively.
        - Flat: the field name is absent from ``data`` (or its value is not a
          dict), so the *same* ``data`` dict is passed down and the composed
          class pulls its own fields from it.  This lets multiple composed
          classes share a single flat TOML section.

    * Type coercion: the same precedence as ``_from_args`` is used - the
      field's explicit ``type`` metadata (if any) is tried first, then the
      type derived from the field's Python type annotation.  This ensures
      e.g. enum fields with a value-based constructor (``mode = "fixed"``) and
      ConfigType address fields work identically in both entry points.

    Note: this function reads ``data`` without mutating it.  Multiple composed
    ConfigClass fields inside the same parent can therefore all receive the
    same dict safely.
    """
    normalized = {k.replace("-", "_"): v for k, v in data.items()}
    result_kwargs: Dict[str, Any] = {}
    for field in dataclasses.fields(config_cls):  # type: ignore[arg-type]
        if is_config_class(field.type):
            if field.name in normalized and isinstance(normalized[field.name], dict):
                # Explicitly nested sub-table in TOML (e.g. [scheduler.zmq])
                result_kwargs[field.name] = _from_toml(field.type, normalized[field.name])  # type: ignore[arg-type]
            else:
                # Flat layout: let the composed class pick its own fields from parent data
                result_kwargs[field.name] = _from_toml(field.type, normalized)  # type: ignore[arg-type]
        else:
            # Look up using the long name (e.g. "logging_level" for a field with
            # long="--logging-level"), falling back to the field name when no explicit
            # long name is set.  This mirrors _toml_section_defaults, which only
            # accepts the long CLI name as a TOML key.
            long_key = field.metadata.get("long", f"--{field.name.replace('_', '-')}").lstrip("-").replace("-", "_")
            if long_key not in normalized:
                # Field not present in this TOML section - leave it out so the
                # dataclass default is used when config_cls(**result_kwargs) is called.
                continue
            value = normalized[long_key]
            # Apply type conversion using the same precedence as _from_args: explicit
            # metadata type first, then the type derived from the field's annotation.
            if isinstance(value, str):
                type_fn = field.metadata.get("type") or get_type_args(field.type).get("type")
                if type_fn and type_fn is not str:
                    value = type_fn(value)
            result_kwargs[field.name] = value
    return config_cls(**result_kwargs)


def _from_args(config_cls: Type[T], kwargs: Dict[str, Any], toml_data: Optional[Dict[str, Any]] = None) -> T:
    """Build a ConfigClass instance from a flat argparse-kwargs dict.

    This is the CLI reconstruction path, used for configs without subcommands
    (or for the non-subcommand fields within a subcommand wrapper - see
    _from_args_with_subcommands).

    ``kwargs`` is the mutable dict produced by ``vars(parser.parse_args())``.
    Fields are *popped* from it as they are consumed, so the caller (or a
    sibling recursive call) sees only the fields it owns.  This pop-based
    protocol is what allows a flat argparse namespace to be partitioned across
    a tree of nested ConfigClass objects.

    ``toml_data`` is the full top-level TOML dict (i.e. the entire parsed
    config file).  It is only needed when ``config_cls`` has fields that carry
    ``section=`` metadata, meaning those fields are populated from a named TOML
    section rather than from CLI args (e.g. ``[worker_manager]`` tables that
    can contain a variable number of entries).

    Field dispatch
    --------------
    For each field in config_cls the function picks one of three paths:

    1. ``section=`` metadata - value comes from TOML, not from argparse.
       The raw TOML section is located by name and reconstructed via
       _from_toml().  Handles three TOML shapes:
         a. Discriminated union list: each item has a ``type`` key (or
            whatever ``discriminator`` names) that selects the concrete class.
         b. Single dict (``[section]`` table): reconstructed as a single
            instance, wrapped in a list if the field type is List[...].
         c. List of dicts (``[[section]]`` array-of-tables): each item
            reconstructed independently.

    2. Nested ConfigClass (no ``section=``) - recurse into _from_args(),
       sharing the same mutable ``kwargs`` dict so the nested class consumes
       its own entries.

    3. Plain field - pop the value from ``kwargs`` and use it directly.
    """
    result_kwargs: Dict[str, Any] = {}
    for field in dataclasses.fields(config_cls):  # type: ignore[arg-type]
        if "section" in field.metadata:
            # This field is populated from a named TOML section, not from CLI args.
            section_name = field.metadata["section"]
            raw = (toml_data or {}).get(section_name)

            # Unwrap List[X] or Optional[X] to get the inner ConfigClass type
            # that _from_toml will be called with.
            inner_type: Any
            if is_list(field.type):
                inner_type = get_list_type(field.type)
            elif is_optional(field.type):
                inner_type = get_optional_type(field.type)
            else:
                inner_type = field.type

            if raw is None:
                # Section absent from TOML: use an empty list or the field default.
                if is_list(field.type):
                    result_kwargs[field.name] = []
                elif field.default_factory is not dataclasses.MISSING:
                    result_kwargs[field.name] = field.default_factory()
                else:
                    result_kwargs[field.name] = None
            elif is_list(field.type) and is_union(inner_type):
                # Discriminated union: e.g. List[Union[NativeConfig, SymphonyConfig]]
                # where each item has a "type" key that selects the concrete class.
                discriminator = field.metadata["discriminator"]
                tag_map = {m._tag: m for m in get_union_args(inner_type)}
                items = raw if isinstance(raw, list) else [raw]
                result_kwargs[field.name] = [_from_toml(tag_map[item[discriminator]], item) for item in items]
            elif isinstance(raw, dict):
                # Single TOML table [section]: reconstruct one instance.
                # If the field expects a list, wrap the single instance.
                instance: Any = _from_toml(inner_type, raw)
                result_kwargs[field.name] = [instance] if is_list(field.type) else instance
            else:  # list of dicts - TOML [[array of tables]]
                result_kwargs[field.name] = [_from_toml(inner_type, item) for item in raw]
        elif is_config_class(field.type):
            # Nested ConfigClass with no section= - recurse, sharing the same
            # mutable kwargs so the nested class pops its own fields out.
            result_kwargs[field.name] = _from_args(field.type, kwargs)  # type: ignore[arg-type]
        elif field.name in kwargs:
            value = kwargs.pop(field.name)
            # argparse applies type conversion for CLI values but not for defaults set via
            # set_defaults() (which is how TOML values are injected).  Apply the same
            # conversion here so that e.g. enum fields and ConfigType address fields work
            # correctly when their values come from a TOML file rather than the CLI.
            if isinstance(value, str):
                type_fn = field.metadata.get("type") or get_type_args(field.type).get("type")
                if type_fn and type_fn is not str:
                    value = type_fn(value)
            result_kwargs[field.name] = value
    return config_cls(**result_kwargs)


def _from_args_with_subcommands(cls: Type[T], kwargs: Dict[str, Any], dest: str) -> T:
    """Build a ConfigClass that has CLI subcommand fields from a flat argparse-kwargs dict.

    This is the entry point for configs that use the ``subcommand`` field
    metadata, which corresponds to argparse subparsers.  For example,
    ``ScalerWorkerManagerConfig`` has mutually exclusive subcommand fields
    (``baremetal_native``, ``symphony``, ``aws_raw_ecs``, ``aws_hpc``), only
    one of which is populated depending on what the user typed on the CLI.

    ``dest`` is the argparse ``dest`` name for the subparser choice at the
    current level (e.g. ``"_sub"`` at the top level, ``"_sub_baremetal_native"``
    one level deeper).  argparse stores the chosen subcommand name under this
    key in the parsed namespace.

    Base case
    ---------
    If ``cls`` has no subcommand fields it is a leaf config; delegate directly
    to _from_args() and return.

    Recursive case
    --------------
    1. Pop ``kwargs[dest]`` to find which subcommand was selected.
    2. Look up the matching field and unwrap Optional[X] if needed to get the
       concrete config class.
    3. Recurse into _from_args_with_subcommands() for the selected subcommand,
       using an extended dest key (``"{dest}_{subcommand}"``).  This handles
       configs with more than one level of nested subparsers.
    4. Build the wrapper instance: all subcommand fields default to None, then
       the selected one is set to the reconstructed sub-config.  Non-subcommand
       fields on the wrapper class are reconstructed via _from_args().

    As in _from_args(), ``kwargs`` is mutated throughout - each level pops the
    fields it owns, leaving the rest for sibling or parent calls.
    """
    subcommand_fields = [f for f in dataclasses.fields(cls) if "subcommand" in f.metadata]  # type: ignore[arg-type]

    # Base case: no subcommands at this level - plain CLI config.
    if not subcommand_fields:
        return _from_args(cls, kwargs)

    # Pop the argparse dest key to find which subcommand the user selected.
    subcommand = kwargs.pop(dest)
    sub_field = next(f for f in subcommand_fields if f.name == subcommand)
    config_cls = get_optional_type(sub_field.type) if is_optional(sub_field.type) else sub_field.type

    # Recurse: the selected subcommand config may itself have further subcommands.
    selected_config = _from_args_with_subcommands(config_cls, kwargs, f"{dest}_{subcommand}")  # type: ignore[arg-type]

    # All subcommand slots default to None; fill in only the selected one.
    wrapper_kwargs: Dict[str, Any] = {f.name: None for f in subcommand_fields}
    wrapper_kwargs[subcommand] = selected_config

    # Reconstruct any non-subcommand fields on the wrapper class itself.
    for field in dataclasses.fields(cls):  # type: ignore[arg-type]
        if "subcommand" in field.metadata:
            continue
        if is_config_class(field.type):
            wrapper_kwargs[field.name] = _from_args(field.type, kwargs)  # type: ignore[arg-type]
        elif field.name in kwargs:
            wrapper_kwargs[field.name] = kwargs.pop(field.name)

    return cls(**wrapper_kwargs)
