import logging
import time
from typing import Optional

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.protocol.capnp import (
    ClientDisconnect,
    ClientHeartbeat,
    ClientHeartbeatEcho,
    ClientManagerStatus,
    ClientShutdownResponse,
    ObjectStorageAddress,
    TaskCancel,
)
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import ClientController, ObjectController, TaskController, WorkerController
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, TaskID
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.one_to_many_dict import OneToManyDict

logger = logging.getLogger(__name__)


class VanillaClientController(ClientController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller

        self._client_to_task_ids: OneToManyDict[ClientID, TaskID] = OneToManyDict()

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None
        self._object_controller: Optional[ObjectController] = None
        self._task_controller: Optional[TaskController] = None
        self._worker_controller: Optional[WorkerController] = None

        self._client_last_seen: dict[ClientID, tuple[float, ClientHeartbeat]] = dict()

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncPublisher,
        object_controller: ObjectController,
        task_controller: TaskController,
        worker_controller: WorkerController,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._object_controller = object_controller
        self._task_controller = task_controller
        self._worker_controller = worker_controller

    def get_client_task_ids(self, client_id: ClientID) -> set[TaskID]:
        return self._client_to_task_ids.get_values(client_id)

    def has_client_id(self, client_id: ClientID) -> bool:
        return client_id in self._client_last_seen

    def get_client_id(self, task_id: TaskID) -> Optional[ClientID]:
        if not self._client_to_task_ids.has_value(task_id):
            return None
        return self._client_to_task_ids.get_key(task_id)

    def on_task_begin(self, client_id: ClientID, task_id: TaskID):
        self._client_to_task_ids.add(client_id, task_id)

    def on_task_finish(self, task_id: TaskID) -> Optional[ClientID]:
        # Returns ``None`` when the task is not currently associated with any
        # tracked client. This happens when the client was disconnected (e.g.
        # client_timeout_seconds elapsed without a heartbeat) while the task
        # was still running on a worker: the in-flight task arrives back at
        # the scheduler after the client mapping has been torn down. Callers
        # must handle ``None`` by skipping the client-bound send while still
        # cleaning up scheduler-side state for the task.
        if not self._client_to_task_ids.has_value(task_id):
            return None
        return self._client_to_task_ids.remove_value(task_id)

    async def on_heartbeat(self, client_id: ClientID, info: ClientHeartbeat):
        object_storage_address = self._config_controller.get_config("advertised_object_storage_address")

        await self._binder.send(
            client_id,
            ClientHeartbeatEcho(
                objectStorageAddress=ObjectStorageAddress(
                    host=object_storage_address.host,
                    port=object_storage_address.port,
                    scheme=object_storage_address.type.value,
                )
            ),
            detached=True,
        )
        if client_id not in self._client_last_seen:
            logger.info(f"{client_id!r} connected")

        self._client_last_seen[client_id] = (time.time(), info)

    def notice_client_activity(self, client_id: ClientID):
        # Any inbound message from a tracked client counts as liveness for
        # the heartbeat-based timeout. We deliberately don't register new
        # clients here -- a client must complete the proper ClientHeartbeat
        # handshake before it shows up in ``_client_last_seen``.
        existing = self._client_last_seen.get(client_id)
        if existing is None:
            return
        self._client_last_seen[client_id] = (time.time(), existing[1])

    async def on_client_disconnect(self, client_id: ClientID, request: ClientDisconnect):
        if request.disconnectType == ClientDisconnect.DisconnectType.disconnect:
            await self.__on_client_disconnect(client_id)
            return

        if self._config_controller.get_config("protected"):
            logger.warning("cannot shutdown clusters as scheduler is running in protected mode")
            accepted = False
        else:
            logger.info(f"shutdown scheduler and all clusters as received signal from {client_id!r}")
            accepted = True

        await self._binder.send(client_id, ClientShutdownResponse(accepted=accepted), detached=True)

        if self._config_controller.get_config("protected"):
            return

        await self._worker_controller.on_client_shutdown(client_id)

        raise ClientShutdownException(f"received client shutdown from {client_id!r}, quitting")

    async def routine(self):
        await self.__routine_cleanup_clients()

    def get_status(self) -> ClientManagerStatus:
        return ClientManagerStatus(
            clientToNumOfTask=[
                ClientManagerStatus.Pair(
                    client=client,
                    numTask=(
                        len(self._client_to_task_ids.get_values(client))
                        if self._client_to_task_ids.has_key(client)
                        else 0
                    ),
                )
                for client in self._client_last_seen.keys()
            ]
        )

    async def __routine_cleanup_clients(self):
        now = time.time()
        dead_clients = {
            client
            for client, (last_seen, info) in self._client_last_seen.items()
            if now - last_seen > self._config_controller.get_config("client_timeout_seconds")
        }

        for client in dead_clients:
            await self.__on_client_disconnect(client)

    async def __on_client_disconnect(self, client_id: ClientID):
        logger.info(f"{client_id!r} disconnected")
        if client_id in self._client_last_seen:
            self._client_last_seen.pop(client_id)

        await self.__cancel_client_all_tasks(client_id)
        self._object_controller.clean_client(client_id)

    async def __cancel_client_all_tasks(self, client_id: ClientID):
        if client_id not in self._client_to_task_ids.keys():
            return

        tasks = self._client_to_task_ids.get_values(client_id).copy()
        for task in tasks:
            await self._task_controller.on_task_cancel(client_id, TaskCancel(taskId=task))
