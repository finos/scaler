import asyncio
import logging
from concurrent.futures import Future
from typing import Any, Callable, List, Tuple

import cloudpickle

from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import TaskID
from scaler.worker_manager.proxy.mixins import ExecutionBackend, TaskDeserializer, TaskInputLoader
from scaler.worker_manager.proxy.symphony.callback import create_session_callback_class
from scaler.worker_manager.proxy.symphony.message import create_soam_message_class
from scaler.worker_manager.proxy.symphony.response_router import TaskResponseRouter
from scaler.worker_manager.proxy.symphony.soamapi import load_soamapi

logger = logging.getLogger(__name__)


class SymphonyExecutionBackend(TaskInputLoader, ExecutionBackend):
    _loader: TaskDeserializer

    def __init__(self, service_name: str) -> None:
        self._service_name = service_name

        self._soamapi = load_soamapi()
        self._soamapi.initialize()

        self._message_class = create_soam_message_class()
        self._response_router = TaskResponseRouter(self._message_class)
        self._session_callback = create_session_callback_class()(self._response_router)

        self._ibm_soam_connection = self._soamapi.connect(
            self._service_name, self._soamapi.DefaultSecurityCallback("Guest", "Guest")
        )
        logger.info(f"established IBM Spectrum Symphony connection {self._ibm_soam_connection.get_id()}")

        ibm_soam_session_attr = self._soamapi.SessionCreationAttributes()
        ibm_soam_session_attr.set_session_type("RecoverableAllHistoricalData")
        ibm_soam_session_attr.set_session_name("ScalerSession")
        ibm_soam_session_attr.set_session_flags(self._soamapi.SessionFlags.PARTIAL_ASYNC)
        ibm_soam_session_attr.set_session_callback(self._session_callback)
        self._ibm_soam_session = self._ibm_soam_connection.create_session(ibm_soam_session_attr)
        logger.info(f"established IBM Spectrum Symphony session {self._ibm_soam_session.get_id()}")

    def register(self, load_task_inputs: TaskDeserializer) -> None:
        self._loader = load_task_inputs

    async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]:
        return await self._loader(task)

    def close(self) -> None:
        """Close the SOAM session and connection, then shut the API down.

        A worker that exits without this leaves Symphony to notice the broken connection and abort the
        session, which it records as an error against the application. Closing ends the session as
        closed instead, and stops the SOAM threads while the worker is still there to wait for them.

        The session is destroyed rather than detached because it belongs to this worker alone, and its
        tasks have already been resolved or cancelled by the time the worker gets here. Each step is
        attempted even where an earlier one failed, so a session that cannot be closed does not also
        leave the API initialized.

        This does not stop the ``malloc_consolidate(): invalid chunk size`` abort the worker dies of at
        exit. That corruption is already present by the time anything here runs, and closing in order,
        dropping every soamapi object before ``uninitialize``, and destroying the network backend were
        each measured against it and changed nothing.
        """
        self._close_step(
            "session", lambda: self._ibm_soam_session.close(self._soamapi.SessionCloseFlags.DESTROY_ON_CLOSE)
        )
        self._close_step("connection", self._ibm_soam_connection.close)
        self._close_step("api", self._soamapi.uninitialize)
        logger.info("closed the IBM Spectrum Symphony session and connection")

    @staticmethod
    def _close_step(what: str, close: Callable[[], None]) -> None:
        try:
            close()
        except Exception as error:
            # The worker is on its way out; a failed step is worth reporting but not worth raising over.
            logger.warning(f"failed to close the IBM Spectrum Symphony {what}: {error}")

    async def on_cancel(self, task_cancel: TaskCancel) -> None:
        pass

    def on_cleanup(self, task_id: TaskID) -> None:
        pass

    async def routine(self) -> None:
        pass

    async def execute(self, task: Task) -> asyncio.Future:
        function, arg_objects = await self.load_task_inputs(task)

        input_message = self._message_class()
        input_message.set_payload(cloudpickle.dumps((function, *arg_objects)))

        task_attr = self._soamapi.TaskSubmissionAttributes()
        task_attr.set_task_input(input_message)

        with self._response_router.get_callback_lock():
            symphony_task = self._ibm_soam_session.send_task_input(task_attr)

            future: Future = Future()
            future.set_running_or_notify_cancel()

            self._response_router.submit_task(symphony_task.get_id(), future)

        return asyncio.wrap_future(future)
