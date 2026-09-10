"""A real object-storage server + scheduler in their own processes, exactly like production.

Unlike ``SchedulerClusterCombo`` this starts no workers of its own, so a test can attach its own worker
manager and observe the full provisioning control loop.
"""

from __future__ import annotations

import os
import signal
import socket
import tempfile
import time
from multiprocessing import get_context
from multiprocessing.process import BaseProcess
from typing import Optional

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.utility.network_util import get_available_tcp_port

POLL_INTERVAL_SECONDS = 0.05
# Only reached in full when a process refuses to exit, so it errs on the side of patience.
TERMINATION_TIMEOUT_SECONDS = 30
_READY_TIMEOUT_SECONDS = 30.0
# get_available_tcp_port() releases the port before the child binds it, so a since-freed ephemeral port
# can be grabbed in between (a TOCTOU race). Retry the whole bring-up with fresh ports a few times.
_START_ATTEMPTS = 3


def terminate_process(process: BaseProcess) -> None:
    """Reap a spawned process: terminate it gracefully, then force-kill if it does not exit."""
    if process.is_alive():
        process.terminate()
        process.join(timeout=TERMINATION_TIMEOUT_SECONDS)
    if process.is_alive():
        process.kill()
        process.join()


def _run_webgui(monitor_address: str, host: str, port: int) -> None:
    from scaler.config.section.webgui import WebGUIConfig
    from scaler.config.types.address import AddressConfig as _AddressConfig
    from scaler.config.types.http import HTTPConfig
    from scaler.ui.webgui import start_webgui

    start_webgui(
        WebGUIConfig(monitor_address=_AddressConfig.from_string(monitor_address), gui_address=HTTPConfig(host, port))
    )


class SchedulerHarness:
    """Register :meth:`shutdown` with ``addCleanup`` so it is torn down even if the test body raises."""

    def __init__(
        self,
        policy_content: str = "allocate=even_load; scaling=vanilla",
        policy_engine_type: str = "simple",
        gateway: Optional[str] = None,
        client_timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
        worker_timeout_seconds: int = DEFAULT_WORKER_TIMEOUT_SECONDS,
    ) -> None:
        # gateway: the docker-bridge gateway IP. The scheduler and object storage bind 0.0.0.0 and
        # advertise gateway-reachable addresses so workers in containers can connect back; the host
        # client still uses loopback.
        self._gateway = gateway
        # A cloud boot (minutes) can starve the scheduler and stall client heartbeats past the stock 60s,
        # so the EC2 backends raise these.
        self._client_timeout_seconds = client_timeout_seconds
        self._worker_timeout_seconds = worker_timeout_seconds
        self._object_storage: Optional[ObjectStorageServerProcess] = None
        self._scheduler: Optional[SchedulerProcess] = None
        self._webgui: Optional[BaseProcess] = None
        self._monitor_client_address: Optional[str] = None
        self._shutdown_done = False
        # Tee the scheduler log to a file so a test can detect an unhandled scheduler exception under
        # churn -- which may crash the scheduler OR leave it up but wedged (the client just times out).
        log_fd, self._scheduler_log_path = tempfile.mkstemp(prefix="scaler-it-scheduler-", suffix=".log")
        os.close(log_fd)

        errors: list = []
        for _attempt in range(_START_ATTEMPTS):
            try:
                self._bring_up(policy_content, policy_engine_type)
                break
            except (OSError, TimeoutError) as error:
                errors.append(error)
                self._kill_processes()
        else:
            raise RuntimeError(f"scheduler harness failed to start after {_START_ATTEMPTS} attempts: {errors!r}")

        self._start_webgui()

    def _bring_up(self, policy_content: str, policy_engine_type: str) -> None:
        scheduler_port, storage_port, monitor_port = (get_available_tcp_port() for _ in range(3))
        bind_host = "0.0.0.0" if self._gateway else "127.0.0.1"
        self.scheduler_address = f"tcp://127.0.0.1:{scheduler_port}"
        self.worker_scheduler_address = (
            f"tcp://{self._gateway}:{scheduler_port}" if self._gateway else self.scheduler_address
        )
        self._monitor_client_address = f"tcp://127.0.0.1:{monitor_port}"

        self._object_storage = ObjectStorageServerProcess(
            bind_address=AddressConfig.from_string(f"tcp://{bind_host}:{storage_port}"),
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        self._object_storage.start()
        self._object_storage.wait_until_ready()  # raises TimeoutError if the port was taken

        self._scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(f"tcp://{bind_host}:{scheduler_port}"),
            object_storage_address=AddressConfig.from_string(f"tcp://127.0.0.1:{storage_port}"),
            advertised_object_storage_address=(
                AddressConfig.from_string(f"tcp://{self._gateway}:{storage_port}") if self._gateway else None
            ),
            monitor_address=AddressConfig.from_string(f"tcp://{bind_host}:{monitor_port}"),
            policy=PolicyConfig(policy_engine_type=policy_engine_type, policy_content=policy_content),
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=self._client_timeout_seconds,
            worker_timeout_seconds=self._worker_timeout_seconds,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            event_loop="builtin",
            logging_paths=("/dev/stdout", self._scheduler_log_path),
            logging_config_file=None,
            logging_level="INFO",
        )
        self._scheduler.start()
        self._wait_scheduler_ready(scheduler_port)

    def _wait_scheduler_ready(self, port: int) -> None:
        """Only the object storage port is otherwise probed, so without this a scheduler that lost the
        port race slips through and surfaces much later as a client TimeoutError misattributed to a crash
        under churn. Raising TimeoutError here routes it back through the fresh-ports retry instead."""
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if self._scheduler is not None and not self._scheduler.is_alive():
                raise TimeoutError(f"scheduler exited during startup (exit {self._scheduler.exitcode}); port taken?")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(POLL_INTERVAL_SECONDS)
        raise TimeoutError(f"scheduler did not accept connections on 127.0.0.1:{port}")

    def _start_webgui(self) -> None:
        """Best-effort: the dashboard is a convenience for watching a local run, not the thing under test,
        so neither a missing [gui] extra nor a failed spawn may fail the e2e -- and it starts after the
        scheduler, so a raise here would leak it."""
        try:
            import scaler.ui.webgui  # noqa: F401

            port = get_available_tcp_port()
            self._webgui = get_context("spawn").Process(
                target=_run_webgui, args=(self._monitor_client_address, "0.0.0.0", port), daemon=True
            )
            self._webgui.start()
        except Exception as error:
            print(f"[harness] web GUI unavailable ({error}); continuing without it")
            return
        print(f"[harness] web GUI: http://localhost:{port}")

    def died(self) -> Optional[str]:
        """A description of whichever harness process exited mid-test, else None. A healthy run keeps both
        alive for the whole test, so either exiting means an unhandled crash."""
        for name, process in (("scheduler", self._scheduler), ("object storage", self._object_storage)):
            if process is not None and not process.is_alive():
                return f"{name} (exit {process.exitcode})"
        return None

    def unhandled_error(self, settle_seconds: float = 0.0) -> Optional[str]:
        """A one-line summary if the scheduler logged a traceback (a fault that wedges the pool without
        killing the process), else None. ``settle_seconds`` polls for a traceback that lands just after the
        client gives up. A silent wedge that logs nothing is out of reach -- only :meth:`died` guards that."""
        deadline = time.monotonic() + settle_seconds
        while True:
            try:
                with open(self._scheduler_log_path, "r", encoding="utf-8", errors="replace") as log_file:
                    lines = log_file.read().splitlines()
            except OSError:
                return None
            if any("Traceback (most recent call last)" in line for line in lines):
                # The router logs "<task>: exception happened, transition: ... path: ..." right before the
                # traceback; report that (it names the state path) if present, else the exception line.
                context = next((line.strip() for line in lines if "exception happened" in line), "")
                exception = next(
                    (line.strip() for line in reversed(lines) if line[:1].strip() and "Error" in line and ":" in line),
                    "",
                )
                return " | ".join(part for part in (context, exception) if part) or "unhandled scheduler exception"
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.25)

    def _kill_processes(self) -> None:
        for process in (self._scheduler, self._object_storage):
            if process is not None:
                terminate_process(process)
        self._scheduler = None
        self._object_storage = None

    def shutdown(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        if self._webgui is not None:
            terminate_process(self._webgui)
        # The scheduler drains on SIGINT; the object storage has no such handler.
        if self._scheduler is not None and self._scheduler.is_alive():
            try:
                os.kill(self._scheduler.pid, signal.SIGINT)
                self._scheduler.join(timeout=TERMINATION_TIMEOUT_SECONDS)
            except (ProcessLookupError, OSError):
                pass
        self._kill_processes()
        try:
            os.unlink(self._scheduler_log_path)
        except OSError:
            pass
