# PYTHON_ARGCOMPLETE_OK
import argparse
import sys
from typing import Any, Union, cast

import argcomplete

from scaler.config.config_class import ConfigClass
from scaler.config.loading import _load_toml
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.process_bootstrap import bootstrap_process

_AnyWorkerManagerConfig = Union[
    NativeWorkerManagerConfig,
    SymphonyWorkerManagerConfig,
    ECSWorkerManagerConfig,
    AWSBatchWorkerManagerConfig,
    ORBAWSEC2WorkerManagerConfig,
    OCIRawWorkerManagerConfig,
    OCIHPCWorkerManagerConfig,
]

_TYPE_MAP: dict[str, type[ConfigClass]] = {
    NativeWorkerManagerConfig._tag: NativeWorkerManagerConfig,
    SymphonyWorkerManagerConfig._tag: SymphonyWorkerManagerConfig,
    ECSWorkerManagerConfig._tag: ECSWorkerManagerConfig,
    AWSBatchWorkerManagerConfig._tag: AWSBatchWorkerManagerConfig,
    ORBAWSEC2WorkerManagerConfig._tag: ORBAWSEC2WorkerManagerConfig,
    OCIRawWorkerManagerConfig._tag: OCIRawWorkerManagerConfig,
    OCIHPCWorkerManagerConfig._tag: OCIHPCWorkerManagerConfig,
}


def main() -> None:
    # Build a full parser tree upfront so argcomplete can traverse subcommand-specific
    # options.  This parser is only used for tab completion; actual parsing is done by
    # the two-pass logic below.  argcomplete.autocomplete() exits immediately in
    # completion mode and is a no-op otherwise.
    _completion_parser = argparse.ArgumentParser(prog="scaler_worker_manager", add_help=False)
    _completion_parser.add_argument("--config", "-c")
    _subparsers = _completion_parser.add_subparsers(dest="subcommand")
    for _tag, _config_cls in _TYPE_MAP.items():
        _sub = _subparsers.add_parser(_tag, add_help=False)
        _config_cls.configure_parser(_sub)
    argcomplete.autocomplete(_completion_parser)

    # Pass 1: extract subcommand and --config before building the type-specific parser.
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", "-c")
    pre_parser.add_argument("subcommand", metavar="{" + ",".join(_TYPE_MAP.keys()) + "}")
    pre_args, remaining_argv = pre_parser.parse_known_args()

    wm_type = pre_args.subcommand
    if wm_type not in _TYPE_MAP:
        valid = ", ".join(_TYPE_MAP)
        print(f"scaler_worker_manager: unknown subcommand '{wm_type}', must be one of: {valid}", file=sys.stderr)
        sys.exit(1)

    # Load TOML and find the matching [[worker_manager]] entry.
    section_data: dict[str, Any] = {}
    if pre_args.config:
        toml_data = _load_toml(pre_args.config)
        entries = toml_data.get("worker_manager", [])
        if isinstance(entries, dict):
            entries = [entries]
        matching = [e for e in entries if e.get("type") == wm_type]
        if not matching:
            print(f"scaler_worker_manager: no worker manager of type '{wm_type}' found in config", file=sys.stderr)
            sys.exit(1)
        if len(matching) > 1:
            print(
                f"scaler_worker_manager: {len(matching)} worker managers of type '{wm_type}' found in config,"
                " expected exactly one",
                file=sys.stderr,
            )
            sys.exit(1)
        section_data = matching[0]

    # Pass 2: parse the type-specific config using TOML defaults + remaining CLI args.
    config_cls = _TYPE_MAP[wm_type]
    wm_config = cast(
        _AnyWorkerManagerConfig,
        config_cls.parse_with_section("scaler_worker_manager", section_data, argv=remaining_argv),
    )

    bootstrap_process(
        wm_config.logging_config.paths,
        wm_config.logging_config.config_file,
        wm_config.logging_config.level,
        process_name=wm_type,
    )
    register_event_loop(wm_config.worker_config.event_loop)

    if isinstance(wm_config, NativeWorkerManagerConfig):
        from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

        NativeWorkerManager(wm_config).run()
    elif isinstance(wm_config, SymphonyWorkerManagerConfig):
        from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(wm_config).run()
    elif isinstance(wm_config, ECSWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

        ECSWorkerManager(wm_config).run()
    elif isinstance(wm_config, AWSBatchWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_hpc.worker_manager import AWSHPCWorkerManager

        AWSHPCWorkerManager(wm_config).run()
    elif isinstance(wm_config, ORBAWSEC2WorkerManagerConfig):
        from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager

        ORBAWSEC2WorkerManager(wm_config).run()
    elif isinstance(wm_config, OCIRawWorkerManagerConfig):
        from scaler.worker_manager_adapter.oci_raw.worker_manager import OCIRawWorkerManager

        OCIRawWorkerManager(wm_config).run()
    elif isinstance(wm_config, OCIHPCWorkerManagerConfig):
        from scaler.worker_manager_adapter.oci_hpc.worker_manager import OCIHPCWorkerManager

        OCIHPCWorkerManager(wm_config).run()


if __name__ == "__main__":
    main()
