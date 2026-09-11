from typing import Optional

import psutil

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.protocol.capnp import InformationRequest, Resource, StateObject, StateScheduler
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import (
    ClientController,
    InformationController,
    ObjectController,
    TaskController,
    WorkerController,
)
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController
from scaler.utility.memory import get_memory_limit_and_available, get_process_memory
from scaler.utility.mixins import Looper

# An object's task ids that travel with it; a graph-wide object has one per node, so the rest is a count.
OBJECT_TASK_ID_LIMIT = 20


class VanillaInformationController(InformationController, Looper):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller

        self._process = psutil.Process()

        self._monitor_binder: Optional[AsyncPublisher] = None
        self._binder: Optional[AsyncBinder] = None
        self._client_controller: Optional[ClientController] = None
        self._object_controller: Optional[ObjectController] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None
        self._worker_manager_controller: Optional[WorkerManagerController] = None

    def register_managers(
        self,
        monitor_binder: AsyncPublisher,
        binder: AsyncBinder,
        client_controller: ClientController,
        object_controller: ObjectController,
        task_controller: TaskController,
        worker_controller: WorkerController,
        worker_manager_controller: WorkerManagerController,
    ):
        self._monitor_binder = monitor_binder
        self._binder = binder
        self._client_controller = client_controller
        self._object_controller = object_controller
        self._task_controller = task_controller
        self._worker_controller = worker_controller
        self._worker_manager_controller = worker_manager_controller

    async def on_request(self, request: InformationRequest):
        # TODO: implement commands
        pass

    async def routine(self):
        await self.__send_scheduler_state()
        await self.__send_object_state()

    async def __send_object_state(self) -> None:
        """The biggest objects, and the tasks holding each one, which is what a full store is made of."""
        details = self._object_controller.get_largest_objects(self._config_controller.get_config("object_report_limit"))
        task_ids_by_object = self._task_controller.get_task_ids_by_object({detail.object_id for detail in details})

        objects = []
        for detail in details:
            task_ids = task_ids_by_object[detail.object_id]
            objects.append(
                StateObject.ObjectDetail(
                    objectId=detail.object_id,
                    name=detail.name,
                    objectType=detail.content_type,
                    size=detail.size,
                    creator=detail.creator,
                    taskIds=task_ids[:OBJECT_TASK_ID_LIMIT],
                    taskCount=len(task_ids),
                )
            )

        await self._monitor_binder.send(
            StateObject(objects=objects, totalObjects=self._object_controller.object_count())
        )

    async def __send_scheduler_state(self) -> None:
        _, mem_available = get_memory_limit_and_available()
        await self._monitor_binder.send(
            StateScheduler(
                binder=self._binder.get_status(),
                scheduler=Resource(cpu=int(self._process.cpu_percent() * 10), rss=get_process_memory(self._process)),
                rssFree=mem_available,
                clientManager=self._client_controller.get_status(),
                objectManager=self._object_controller.get_status(),
                taskManager=self._task_controller.get_status(),
                workerManager=self._worker_controller.get_status(),
                scalingManager=self._worker_manager_controller.get_status(),
            )
        )
