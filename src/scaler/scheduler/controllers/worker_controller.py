import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.protocol.capnp import (
    ClientDisconnect,
    ObjectStorageAddress,
    ProcessorStatus,
    Resource,
    StateWorker,
    Task,
    TaskCancel,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
    WorkerManagerStatus,
    WorkerState,
    WorkerStatus,
)
from scaler.protocol.helpers import capabilities_to_dict, dict_to_capabilities
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import PolicyController, TaskController, WorkerController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter

logger = logging.getLogger(__name__)

UINT8_MAX = 2**8 - 1
UINT16_MAX = 2**16 - 1


class VanillaWorkerController(WorkerController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, policy_controller: PolicyController) -> None:
        self._config_controller = config_controller

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None
        self._task_controller: Optional[TaskController] = None

        self._worker_alive_since: Dict[WorkerID, Tuple[float, WorkerHeartbeat]] = dict()
        self._worker_to_manager: Dict[WorkerID, bytes] = dict()
        self._manager_to_workers: Dict[bytes, Set[WorkerID]] = dict()
        self._policy_controller = policy_controller

    def register(self, binder: AsyncBinder, binder_monitor: AsyncPublisher, task_controller: TaskController) -> None:
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._task_controller = task_controller

    def acquire_worker(self, task: Task) -> WorkerID:
        return self._policy_controller.assign_task(task)

    async def on_task_cancel(self, task_cancel: TaskCancel) -> WorkerID:
        worker = self._policy_controller.get_worker_by_task_id(task_cancel.taskId)
        if not worker.is_valid():
            logger.error(f"cannot find task_id={task_cancel.taskId.hex()} in task workers")

        return worker

    async def on_task_done(self, task_id: TaskID) -> WorkerID:
        worker = self._policy_controller.remove_task(task_id)
        if not worker.is_valid():
            logger.error(f"Cannot find task in worker queue: task_id={task_id.hex()}")

        return worker

    async def on_heartbeat(self, worker_id: WorkerID, info: WorkerHeartbeat) -> None:
        info.capabilities = capabilities_to_dict(info.capabilities)
        if self._policy_controller.add_worker(worker_id, info.capabilities, info.queueSize):
            logger.info(f"worker {worker_id!r} connected")
            await self._binder_monitor.send(
                StateWorker(
                    workerId=worker_id,
                    state=WorkerState.connected,
                    capabilities=dict_to_capabilities(info.capabilities),
                )
            )
            await self._task_controller.on_worker_connect(worker_id)

        if worker_id not in self._worker_to_manager:
            self._worker_to_manager[worker_id] = info.workerManagerID
            self._manager_to_workers.setdefault(info.workerManagerID, set()).add(worker_id)

        self._worker_alive_since[worker_id] = (time.time(), info)

        object_storage_address = self._config_controller.get_config("advertised_object_storage_address")
        await self._binder.send(
            worker_id,
            WorkerHeartbeatEcho(
                objectStorageAddress=ObjectStorageAddress(
                    host=object_storage_address.host,
                    port=object_storage_address.port,
                    scheme=object_storage_address.type.value,
                )
            ),
            detached=True,
        )

    async def on_client_shutdown(self, client_id: ClientID) -> None:
        for worker in self._policy_controller.get_worker_ids():
            await self.__shutdown_worker(worker)

    async def on_disconnect_notification(self, worker_id: WorkerID, notification: WorkerDisconnectNotification) -> None:
        # The notification always refers to its sender, whose identity comes from the binder and
        # cannot be spoofed by the payload.
        await self.__disconnect_worker(worker_id, reason="graceful notification")

    async def routine(self) -> None:
        await self.__clean_workers()

    def get_status(self) -> WorkerManagerStatus:
        worker_to_task_numbers = self._policy_controller.statistics()
        return WorkerManagerStatus(
            workers=[
                self.__worker_status_from_heartbeat(worker, worker_to_task_numbers[worker], last, info)
                for worker, (last, info) in self._worker_alive_since.items()
            ]
        )

    @staticmethod
    def __worker_status_from_heartbeat(
        worker_id: WorkerID, worker_task_numbers: Dict, last: float, info: WorkerHeartbeat
    ) -> WorkerStatus:
        current_processor = next((p for p in info.processors if not p.suspended), None)
        suspended = min(len([p for p in info.processors if p.suspended]), UINT8_MAX)
        last_s = min(int(time.time() - last), UINT16_MAX)

        if current_processor:
            debug_info = f"{int(current_processor.initialized)}{int(current_processor.hasTask)}{int(info.taskLock)}"
        else:
            debug_info = f"00{int(info.taskLock)}"

        return WorkerStatus(
            workerId=worker_id,
            agent=info.agent,
            rssFree=info.rssFree,
            memLimit=info.memLimit,
            free=worker_task_numbers["free"],
            sent=worker_task_numbers["sent"],
            queued=info.queuedTasks,
            suspended=suspended,
            lagUS=info.latencyUS,
            lastS=last_s,
            itl=debug_info,
            processorStatuses=[
                ProcessorStatus(
                    pid=p.pid,
                    initialized=p.initialized,
                    hasTask=p.hasTask,
                    suspended=p.suspended,
                    resource=Resource(cpu=p.resource.cpu, rss=p.resource.rss),
                    currentTaskId=bytes(p.currentTaskId),
                    taskAgeSeconds=p.taskAgeSeconds,
                )
                for p in info.processors
            ],
            hostname=info.hostname,
            netSentBytes=info.netSentBytes,
            netRecvBytes=info.netRecvBytes,
        )

    def has_available_worker(self) -> bool:
        return self._policy_controller.has_available_worker()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._policy_controller.get_worker_by_task_id(task_id)

    def get_worker_ids(self) -> Set[WorkerID]:
        return self._policy_controller.get_worker_ids()

    def get_workers_by_manager_id(self, manager_id: bytes) -> List[WorkerID]:
        return list(self._manager_to_workers.get(manager_id, set()))

    async def __clean_workers(self) -> None:
        now = time.time()
        timeout = self._config_controller.get_config("worker_timeout_seconds")
        dead_workers = [
            (dead_worker, now - alive_since)
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > timeout
        ]
        for dead_worker, elapsed in dead_workers:
            await self.__disconnect_worker(
                dead_worker, reason=f"no heartbeat for {elapsed:.0f}s, worker_timeout_seconds={timeout}"
            )

    def __remove_worker_from_manager(self, worker_id: WorkerID) -> None:
        manager_id = self._worker_to_manager.pop(worker_id, None)
        if manager_id is None:
            return

        workers_set = self._manager_to_workers.get(manager_id)
        if workers_set is None:
            return

        workers_set.discard(worker_id)
        if not workers_set:
            del self._manager_to_workers[manager_id]

    async def __disconnect_worker(self, worker_id: WorkerID, reason: str) -> None:
        if worker_id not in self._worker_alive_since:
            return

        # Drop the worker from local state before any await: on a backend whose monitor send yields (ZMQ),
        # a second disconnect of the same worker could otherwise pass the guard above and pop() a
        # now-missing id. Removing first keeps the guard-and-remove atomic.
        self._worker_alive_since.pop(worker_id)
        self.__remove_worker_from_manager(worker_id)

        await self._binder_monitor.send(
            StateWorker(workerId=worker_id, state=WorkerState.disconnected, capabilities=[])
        )

        task_ids = self._policy_controller.remove_worker(worker_id)
        if not task_ids:
            logger.info(f"{worker_id!r} disconnected ({reason})")
            return

        logger.warning(f"{worker_id!r} disconnected ({reason}): rerouting/failing {len(task_ids)} task(s)")
        for task_id in task_ids:
            await self._task_controller.on_worker_disconnect(task_id, worker_id)

    async def __shutdown_worker(self, worker_id: WorkerID) -> None:
        await self._binder.send(
            worker_id, ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown), detached=True
        )
        await self.__disconnect_worker(worker_id, reason="client shutdown")
