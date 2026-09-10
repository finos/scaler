import asyncio
import logging

from scaler.config.defaults import CLEANUP_INTERVAL_SECONDS
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.io.ymq import YMQException
from scaler.protocol.capnp import (
    BaseMessage,
    ClientDisconnect,
    ClientHeartbeat,
    GraphTask,
    InformationRequest,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskLog,
    TaskResult,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
    WorkerManagerHeartbeat,
)
from scaler.scheduler.controllers.balance_controller import VanillaBalanceController
from scaler.scheduler.controllers.client_controller import VanillaClientController
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.scheduler.controllers.information_controller import VanillaInformationController
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.vanilla_policy_controller import VanillaPolicyController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientShutdownException, ObjectStorageException
from scaler.utility.identifiers import ClientID, WorkerID

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self._config_controller = VanillaConfigController(config)

        self._identity = generate_identity_from_name("scheduler")
        self._backend = get_network_backend_from_env(io_threads=config.io_threads)

        self._address = config.bind_address
        self._security_config = config.security

        self._binder: AsyncBinder = self._backend.create_async_binder(
            identity=self._identity, callback=self.on_receive_message
        )
        self._connector_storage: AsyncObjectStorageConnector = self._backend.create_async_object_storage_connector(
            identity=self._identity
        )
        self._binder_monitor: AsyncPublisher = self._backend.create_async_publisher(identity=self._identity)

        self._policy_controller = VanillaPolicyController(
            config.policy.policy_engine_type, config.policy.policy_content
        )

        self._client_manager = VanillaClientController(config_controller=self._config_controller)
        self._object_controller = VanillaObjectController(config_controller=self._config_controller)
        self._graph_controller = VanillaGraphTaskController(config_controller=self._config_controller)
        self._task_controller = VanillaTaskController(config_controller=self._config_controller)
        self._worker_controller = VanillaWorkerController(
            config_controller=self._config_controller, policy_controller=self._policy_controller
        )
        self._balance_controller = VanillaBalanceController(
            config_controller=self._config_controller, policy_controller=self._policy_controller
        )
        self._information_controller = VanillaInformationController(config_controller=self._config_controller)
        self._worker_manager_controller = WorkerManagerController(
            config_controller=self._config_controller, policy_controller=self._policy_controller
        )

        self._client_manager.register(
            self._binder, self._binder_monitor, self._object_controller, self._task_controller, self._worker_controller
        )
        self._object_controller.register(
            self._binder, self._binder_monitor, self._connector_storage, self._client_manager, self._worker_controller
        )
        self._graph_controller.register(
            self._binder,
            self._binder_monitor,
            self._connector_storage,
            self._client_manager,
            self._task_controller,
            self._object_controller,
        )
        self._task_controller.register(
            self._binder,
            self._binder_monitor,
            self._connector_storage,
            self._client_manager,
            self._object_controller,
            self._worker_controller,
            self._graph_controller,
        )
        self._worker_controller.register(self._binder, self._binder_monitor, self._task_controller)
        self._balance_controller.register(self._binder, self._binder_monitor, self._task_controller)
        self._worker_manager_controller.register(self._binder, self._task_controller, self._worker_controller)

        self._information_controller.register_managers(
            self._binder_monitor,
            self._binder,
            self._client_manager,
            self._object_controller,
            self._task_controller,
            self._worker_controller,
            self._worker_manager_controller,
        )

    @property
    def address(self) -> AddressConfig:
        assert self._address is not None
        return self._address

    async def __initialize_network(self) -> None:
        # Scheduler's binder
        assert self._address is not None

        await self._binder.bind(self._address, security_config=self._security_config)

        self._address = self._binder.address
        assert self._address is not None
        self._config_controller.update_config("bind_address", self._address)

        logger.info(f"{self.__class__.__name__}: listen to scheduler address {self._address}")

        # Object storage

        await self._connector_storage.connect(
            self._config_controller.get_config("object_storage_address"), security_config=self._security_config
        )
        object_storage_address = self._connector_storage.address

        logger.info(f"{self.__class__.__name__}: connected to object storage server {object_storage_address!r}")

        advertised_object_storage_address = (
            self._config_controller.get_config("advertised_object_storage_address") or object_storage_address
        )

        logger.info(
            f"{self.__class__.__name__}: advertise object storage address {advertised_object_storage_address!r}"
        )

        self._config_controller.update_config("object_storage_address", object_storage_address)
        self._config_controller.update_config("advertised_object_storage_address", advertised_object_storage_address)

        # Monitor

        address = self._address
        assert address.port is not None, "scheduler bind address must have a port"

        default_monitor = AddressConfig(type=address.type, host=address.host, port=address.port + 2, path=address.path)

        monitor_address = self._config_controller.get_config("monitor_address") or default_monitor

        await self._binder_monitor.bind(monitor_address, security_config=self._security_config)

        monitor_address = self._binder_monitor.address
        assert monitor_address is not None
        self._config_controller.update_config("monitor_address", monitor_address)

        logger.info(f"{self.__class__.__name__}: listen to scheduler monitor address {monitor_address!r}")

    async def on_receive_message(self, source: bytes, message: BaseMessage):
        # Any inbound message from a tracked client refreshes its liveness
        # timestamp, not only ClientHeartbeat. This prevents heartbeat-based
        # eviction of clients that are actively talking to the scheduler but
        # whose user code monopolises the asyncio loop and starves the
        # heartbeat coroutine (e.g. browser/emscripten clients running heavy
        # synchronous Python like cloudpickle of large constants). The
        # underlying transport will report a real connection close promptly.
        if source.startswith(b"Client|"):
            self._client_manager.notice_client_activity(ClientID(source))

        # =====================================================================================
        # client manager
        if isinstance(message, ClientHeartbeat):
            await self._client_manager.on_heartbeat(ClientID(source), message)
            return

        # scheduler receives client shutdown request from upstream
        if isinstance(message, ClientDisconnect):
            await self._client_manager.on_client_disconnect(ClientID(source), message)
            return

        # =====================================================================================
        # graph manager
        if isinstance(message, GraphTask):
            await self._graph_controller.on_graph_task(ClientID(source), message)
            return

        # =====================================================================================
        # task manager
        if isinstance(message, Task):
            await self._task_controller.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            if self._graph_controller.is_graph_subtask(message.taskId):
                await self._graph_controller.on_graph_task_cancel(message)
            else:
                await self._task_controller.on_task_cancel(ClientID(source), message)
            return

        if isinstance(message, TaskCancelConfirm):
            await self._task_controller.on_task_cancel_confirm(WorkerID(source), message)
            return

        if isinstance(message, TaskResult):
            await self._task_controller.on_task_result(WorkerID(source), message)
            return

        if isinstance(message, TaskLog):
            client = self._client_manager.get_client_id(message.taskId)
            if client is not None:
                await self._binder.send(client, message, detached=True)
            return

        # =====================================================================================
        # worker manager
        if isinstance(message, WorkerHeartbeat):
            await self._worker_controller.on_heartbeat(WorkerID(source), message)
            return

        if isinstance(message, WorkerDisconnectNotification):
            await self._worker_controller.on_disconnect_notification(WorkerID(source), message)
            return

        # =====================================================================================
        # object manager
        if isinstance(message, ObjectInstruction):
            await self._object_controller.on_object_instruction(source, message)
            return

        # =====================================================================================
        # information manager
        if isinstance(message, InformationRequest):
            await self._information_controller.on_request(message)

        # =====================================================================================
        # worker manager controller
        if isinstance(message, WorkerManagerHeartbeat):
            await self._worker_manager_controller.on_heartbeat(source, message)
            return

        logger.error(f"{self.__class__.__name__}: unknown message from {source=}: {message}")

    async def get_loops(self):
        await self.__initialize_network()

        # swallow_routine_errors=True: a bug in any single routine must not escape to asyncio.gather and
        # tear the whole scheduler down, taking every connected client and worker with it.
        loops = [
            create_async_loop_routine(self._binder.routine, 0, swallow_routine_errors=True),
            create_async_loop_routine(self._connector_storage.routine, 0, swallow_routine_errors=True),
            create_async_loop_routine(self._graph_controller.routine, 0, swallow_routine_errors=True),
            create_async_loop_routine(
                self._balance_controller.routine,
                self._config_controller.get_config("load_balance_seconds"),
                swallow_routine_errors=True,
            ),
            create_async_loop_routine(
                self._client_manager.routine, CLEANUP_INTERVAL_SECONDS, swallow_routine_errors=True
            ),
            create_async_loop_routine(
                self._object_controller.routine, CLEANUP_INTERVAL_SECONDS, swallow_routine_errors=True
            ),
            create_async_loop_routine(
                self._worker_controller.routine, CLEANUP_INTERVAL_SECONDS, swallow_routine_errors=True
            ),
            create_async_loop_routine(
                self._worker_manager_controller.routine, CLEANUP_INTERVAL_SECONDS, swallow_routine_errors=True
            ),
            create_async_loop_routine(
                self._information_controller.routine,
                self._config_controller.get_config("status_report_interval_seconds"),
                swallow_routine_errors=True,
            ),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ClientShutdownException as e:
            logger.info(f"{self.__class__.__name__}: {e}")
        except (YMQException, ObjectStorageException):
            # The scheduler is tearing down below; never let this be silent -- a dead binder / object
            # storage is one of the ways it "just stops responding".
            logger.exception(f"{self.__class__.__name__}: main loop stopped on a transport/storage error")
        except Exception:
            logger.exception(f"{self.__class__.__name__}: main loop stopped on an unexpected error")

        self._binder.destroy()
        self._binder_monitor.destroy()
        self._connector_storage.destroy()
