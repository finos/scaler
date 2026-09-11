import logging
import multiprocessing
import os
import signal
import sys
from typing import Optional

import psutil

from scaler.config.common.security import SecurityConfig
from scaler.config.defaults import DEFAULT_PROCESSOR_KILL_DELAY_SECONDS
from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import Task
from scaler.utility.identifiers import ProcessorID
from scaler.worker.agent.processor.processor import Processor

logger = logging.getLogger(__name__)

# how long to wait between two checks that a resuming processor is still alive
RESUMED_EVENT_POLL_SECONDS = 0.1


class ProcessorHolder:
    def __init__(
        self,
        event_loop: str,
        agent_address: AddressConfig,
        scheduler_address: AddressConfig,
        object_storage_address: AddressConfig,
        preload: Optional[str],
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_suspend: bool,
        logging_paths: tuple[str, ...],
        logging_level: str,
        security_config: Optional[SecurityConfig] = None,
    ):
        self._processor_id: Optional[ProcessorID] = None
        self._task: Optional[Task] = None
        self._suspended = False

        self._hard_suspend = hard_suspend
        self._suspend_trigger = None
        if hard_suspend:
            self._resume_event = None
            self._resumed_event = None
        else:
            context = multiprocessing.get_context("spawn")
            self._resume_event = context.Event()
            self._resumed_event = context.Event()
            if sys.platform == "win32":
                # Windows has no SIGUSR1; the processor uses Py_AddPendingCall driven by this event.
                self._suspend_trigger = context.Event()

        self._processor = Processor(
            event_loop=event_loop,
            agent_address=agent_address,
            scheduler_address=scheduler_address,
            object_storage_address=object_storage_address,
            preload=preload,
            resume_event=self._resume_event,
            resumed_event=self._resumed_event,
            suspend_trigger=self._suspend_trigger,
            garbage_collect_interval_seconds=garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=trim_memory_threshold_bytes,
            logging_paths=logging_paths,
            logging_level=logging_level,
            security_config=security_config,
        )
        self._processor.start()
        self._process = psutil.Process(self._processor.pid)

    def pid(self) -> int:
        assert self._processor.pid is not None
        return self._processor.pid

    def process(self) -> psutil.Process:
        return self._process

    def exitcode(self) -> Optional[int]:
        """The processor's exit code, or None if it has not exited yet: `Process.exitcode` reaps on read, so a
        processor that died on its own reports how it died even though `kill()` never joined it."""
        if self._processor.exitcode is None:
            self._processor.join(timeout=0.1)
        return self._processor.exitcode

    def processor_id(self) -> ProcessorID:
        assert self._processor_id is not None
        return self._processor_id

    def initialized(self) -> bool:
        return self._processor_id is not None

    def initialize(self, processor_id: ProcessorID):
        self._processor_id = processor_id

    def task(self) -> Optional[Task]:
        return self._task

    def set_task(self, task: Optional[Task]):
        self._task = task

    def suspended(self) -> bool:
        return self._suspended

    def suspend(self):
        assert self._processor is not None
        assert self._task is not None
        assert self._suspended is False
        assert self.initialized()

        if self._hard_suspend:
            self._process.suspend()
        else:
            # If we do not want to hardly suspend the processor's process (e.g. to keep network links alive), we request
            # the process to wait on a synchronization event. That will stop the main thread while allowing the helper
            # threads to continue running.
            #
            # See https://github.com/finos/opengris-scaler/issues/14

            assert self._resume_event is not None
            assert self._resumed_event is not None
            self._resume_event.clear()
            self._resumed_event.clear()

            if sys.platform == "win32":
                assert self._suspend_trigger is not None
                self._suspend_trigger.set()
            else:
                os.kill(self.pid(), signal.SIGUSR1)

        self._suspended = True

    def resume(self) -> bool:
        """Resumes the suspended processor, returning False if it died before acknowledging the resume."""

        assert self._task is not None
        assert self._suspended is True

        if self._hard_suspend:
            try:
                self._process.resume()
            except psutil.NoSuchProcess:
                return False
        else:
            assert self._resume_event is not None
            assert self._resumed_event is not None

            self._resume_event.set()

            if sys.platform != "win32":
                # POSIX uses a SIGUSR1 handler that runs synchronously at the next safe point. Waiting for the
                # processor to acknowledge resume avoids re-entering the signal handler while the previous
                # invocation is still in `_resume_event.wait()`. A processor that died while suspended never
                # acknowledges, so give up as soon as it is gone instead of blocking the agent forever.
                while not self._resumed_event.wait(RESUMED_EVENT_POLL_SECONDS):
                    if not self._processor.is_alive():
                        return False
            # On Windows the suspend handler is dispatched via Py_AddPendingCall, which is queued (not signal-
            # delivered) and therefore re-entrancy-safe. Waiting here would block the worker agent's asyncio loop
            # because the pending call cannot fire while the processor's main thread is inside a blocking C call
            # (e.g. an inner `future.result()` waiting on a sub-task) -- it only fires once the main thread next
            # reaches a bytecode boundary, which can be much later than the agent's heartbeat deadline.

        self._suspended = False

        return True

    def kill(self):
        # On POSIX this maps to SIGTERM and gives the processor a chance to run its __interrupt handler
        # (which destroys connectors to break blocking ZMQ reads). On Windows psutil.terminate() is
        # TerminateProcess and is unconditional; the SIGKILL fallback below is unnecessary but harmless.
        try:
            self._process.terminate()
        except psutil.NoSuchProcess:
            self.set_task(None)
            return

        self._processor.join(DEFAULT_PROCESSOR_KILL_DELAY_SECONDS)

        if self._processor.exitcode is None:
            # TODO: some processors fail to interrupt because of a blocking 0mq call. Ideally we should interrupt
            # these blocking calls instead of sending a SIGKILL signal.

            logger.warning(f"Processor[{self.pid()}] does not terminate in time, send SIGKILL.")
            try:
                self._process.kill()
            except psutil.NoSuchProcess:
                pass
            self._processor.join()

        self.set_task(None)
