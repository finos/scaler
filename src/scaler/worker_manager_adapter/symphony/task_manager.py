import asyncio
import logging
from concurrent.futures import Future
from typing import Any

import cloudpickle

from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import TaskID
from scaler.worker_manager_adapter.mixins import ExecutionBackend, TaskDeserializer, TaskInputLoader
from scaler.worker_manager_adapter.symphony.callback import SessionCallback
from scaler.worker_manager_adapter.symphony.message import SoamMessage

logger = logging.getLogger(__name__)

try:
    import soamapi
except ImportError:
    raise ImportError("IBM Spectrum Symphony API not found, please install it with 'pip install soamapi'.")


class SymphonyExecutionBackend(TaskInputLoader, ExecutionBackend):
    _loader: TaskDeserializer

    def __init__(self, service_name: str):
        self._service_name = service_name

        soamapi.initialize()

        self._session_callback = SessionCallback()

        self._ibm_soam_connection = soamapi.connect(
            self._service_name, soamapi.DefaultSecurityCallback("Guest", "Guest")
        )
        logger.info(f"established IBM Spectrum Symphony connection {self._ibm_soam_connection.get_id()}")

        ibm_soam_session_attr = soamapi.SessionCreationAttributes()
        ibm_soam_session_attr.set_session_type("RecoverableAllHistoricalData")
        ibm_soam_session_attr.set_session_name("ScalerSession")
        ibm_soam_session_attr.set_session_flags(soamapi.SessionFlags.PARTIAL_ASYNC)
        ibm_soam_session_attr.set_session_callback(self._session_callback)
        self._ibm_soam_session = self._ibm_soam_connection.create_session(ibm_soam_session_attr)
        logger.info(f"established IBM Spectrum Symphony session {self._ibm_soam_session.get_id()}")

    def register(self, load_task_inputs: TaskDeserializer) -> None:
        self._loader = load_task_inputs

    async def load_task_inputs(self, task: Task) -> tuple[Any, list[Any]]:
        return await self._loader(task)

    async def on_cancel(self, task_cancel: TaskCancel) -> None:
        pass

    def on_cleanup(self, task_id: TaskID) -> None:
        pass

    async def routine(self) -> None:
        pass

    async def execute(self, task: Task) -> asyncio.Future:
        function, arg_objects = await self.load_task_inputs(task)

        input_message = SoamMessage()
        input_message.set_payload(cloudpickle.dumps((function, *arg_objects)))

        task_attr = soamapi.TaskSubmissionAttributes()
        task_attr.set_task_input(input_message)

        with self._session_callback.get_callback_lock():
            symphony_task = self._ibm_soam_session.send_task_input(task_attr)

            future: Future = Future()
            future.set_running_or_notify_cancel()

            self._session_callback.submit_task(symphony_task.get_id(), future)

        return asyncio.wrap_future(future)
