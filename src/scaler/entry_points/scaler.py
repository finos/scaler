# PYTHON_ARGCOMPLETE_OK
import dataclasses
import multiprocessing
import multiprocessing.connection
import signal
import sys
import time
from typing import List, Optional, cast

import psutil

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.object_storage_server import ObjectStorageServerConfig
from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.section.webgui import WebGUIConfig
from scaler.config.section.worker_manager_union import WorkerManagerUnion
from scaler.utility.event_loop import register_event_loop
from scaler.utility.process_bootstrap import bootstrap_process


@dataclasses.dataclass
class ScalerAllConfig(ConfigClass):
    config: str = dataclasses.field(metadata=dict(positional=True, help="Path to the TOML configuration file."))
    # Declaration order = startup order (object storage before scheduler, scheduler before workers).
    object_storage: Optional[ObjectStorageServerConfig] = dataclasses.field(
        default=None, metadata=dict(section="object_storage_server")
    )
    scheduler: Optional[SchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    worker_managers: List[WorkerManagerUnion] = dataclasses.field(
        default_factory=list, metadata=dict(section="worker_manager", discriminator="type")
    )
    gui: Optional[WebGUIConfig] = dataclasses.field(default=None, metadata=dict(section="gui"))


# Module-level functions required for multiprocessing spawn compatibility.


def _run_scheduler(config: SchedulerConfig) -> None:
    from scaler.entry_points.scheduler import main as _main

    _main(config)


def _run_worker_manager(config: WorkerManagerUnion) -> None:
    bootstrap_process(
        config.logging_config.paths,
        config.logging_config.config_file,
        config.logging_config.level,
        process_name=config._tag,
    )
    register_event_loop(config.worker_config.event_loop)
    if isinstance(config, NativeWorkerManagerConfig):
        from scaler.worker_manager.native.worker_manager import NativeWorkerManager

        NativeWorkerManager(config).run()
    elif isinstance(config, SymphonyWorkerManagerConfig):
        from scaler.worker_manager.proxy.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(config).run()
    elif isinstance(config, ECSWorkerManagerConfig):
        from scaler.worker_manager.nested.aws_ecs.worker_manager import ECSWorkerManager

        ECSWorkerManager(config).run()
    elif isinstance(config, AWSBatchWorkerManagerConfig):
        from scaler.worker_manager.proxy.aws_batch.worker_manager import AWSBatchWorkerManager

        AWSBatchWorkerManager(config).run()
    elif isinstance(config, ORBAWSEC2WorkerManagerConfig):
        from scaler.worker_manager.nested.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager

        ORBAWSEC2WorkerManager(config).run()
    elif isinstance(config, OCIRawWorkerManagerConfig):
        from scaler.worker_manager.nested.oci.worker_manager import OCIWorkerManager as NestedOCIWorkerManager

        NestedOCIWorkerManager(config).run()
    elif isinstance(config, OCIHPCWorkerManagerConfig):
        from scaler.worker_manager.proxy.oci.worker_manager import OCIWorkerManager as ProxyOCIWorkerManager

        ProxyOCIWorkerManager(config).run()


def _run_gui(config: WebGUIConfig) -> None:
    from scaler.entry_points.webgui import main as _main

    _main(config)


SHUTDOWN_JOIN_TIMEOUT_SECONDS = 10
FORCE_KILL_JOIN_TIMEOUT_SECONDS = 5


def _shutdown_processes(processes: List[multiprocessing.Process]) -> None:
    """Terminate the started child processes, then force-kill anything that did not exit in time.

    Children are terminated in reverse startup order (workers before the scheduler, the scheduler
    before object storage). Descendants are snapshotted before terminating so that grandchildren
    orphaned by a dying child (e.g. worker processors) can be reaped as well; a surviving orphan
    would keep its ports bound and also wedge interpreter exit, because the multiprocessing
    resource tracker waits for every inherited pipe handle to close.
    """

    started = [process for process in processes if process.pid is not None]

    descendants: List[psutil.Process] = []
    for process in started:
        try:
            descendants.extend(psutil.Process(process.pid).children(recursive=True))
        except psutil.NoSuchProcess:
            pass

    try:
        for process in reversed(started):
            if process.is_alive():
                process.terminate()

        deadline = time.monotonic() + SHUTDOWN_JOIN_TIMEOUT_SECONDS
        for process in reversed(started):
            process.join(max(0.0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        pass  # a repeated interrupt skips the graceful wait; force-kill below

    while True:
        try:
            for process in started:
                if process.is_alive():
                    process.kill()
            for process in started:
                process.join(FORCE_KILL_JOIN_TIMEOUT_SECONDS)

            alive_descendants = []
            for descendant in descendants:
                try:
                    if descendant.is_running():
                        descendant.kill()
                        alive_descendants.append(descendant)
                except psutil.NoSuchProcess:
                    pass
            psutil.wait_procs(alive_descendants, timeout=FORCE_KILL_JOIN_TIMEOUT_SECONDS)
            return
        except KeyboardInterrupt:
            continue


def main() -> None:
    config = ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)

    if config.object_storage is None and config.scheduler is None and not config.worker_managers and config.gui is None:
        print("scaler: no any recognized section found in config file", file=sys.stderr)
        sys.exit(1)

    def _raise_keyboard_interrupt(*_args: object) -> None:
        raise KeyboardInterrupt

    # Route SIGTERM through the same orderly-teardown path as Ctrl-C; without this, `kill <pid>`
    # ends this process silently and leaks the whole cluster, with the scheduler and object
    # storage children keeping their ports bound. On Windows SIGTERM cannot be delivered
    # externally, registering the handler is harmless there.
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    _spawn_process = multiprocessing.get_context("spawn").Process
    processes: List[multiprocessing.Process] = []

    exit_code = 0
    try:
        if config.object_storage is not None:
            oss_logging = config.object_storage.logging_config
            oss_process = ObjectStorageServerProcess(
                bind_address=config.object_storage.bind_address,
                identity=config.object_storage.identity,
                logging_paths=oss_logging.paths,
                logging_config_file=oss_logging.config_file,
                logging_level=oss_logging.level,
                security_config=config.object_storage.security,
            )
            processes.append(oss_process)
            oss_process.start()
            oss_process.wait_until_ready()

        if config.scheduler is not None:
            sched_process = _spawn_process(target=_run_scheduler, args=(config.scheduler,), name="scheduler")
            processes.append(sched_process)  # type: ignore[arg-type]
            sched_process.start()

        for wm_config in config.worker_managers:
            wm_process = _spawn_process(target=_run_worker_manager, args=(wm_config,), name=wm_config._tag)
            processes.append(wm_process)  # type: ignore[arg-type]
            wm_process.start()

        if config.gui is not None:
            gui_process = _spawn_process(target=_run_gui, args=(config.gui,), name="gui")
            processes.append(gui_process)  # type: ignore[arg-type]
            gui_process.start()

        sentinel_to_process = {p.sentinel: p for p in processes}
        done = multiprocessing.connection.wait(sentinel_to_process)
        exited = sentinel_to_process[cast(int, done[0])]
        exit_code = exited.exitcode or 0
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown_processes(processes)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
