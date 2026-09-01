import logging
from typing import Optional

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.protocol.capnp import StateBalanceAdvice
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import PolicyController, TaskController
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.mixins import Looper

logger = logging.getLogger(__name__)


class VanillaBalanceController(Looper):
    def __init__(self, config_controller: VanillaConfigController, policy_controller: PolicyController):
        self._config_controller = config_controller

        self._policy_controller = policy_controller

        self._last_balance_advice: dict[WorkerID, list[TaskID]] = dict()
        self._same_load_balance_advice_count = 0

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None

        self._task_controller: Optional[TaskController] = None

    def register(self, binder: AsyncBinder, binder_monitor: AsyncPublisher, task_controller: TaskController):
        self._binder = binder
        self._binder_monitor = binder_monitor

        self._task_controller = task_controller

    async def routine(self):
        current_advice = self._policy_controller.balance()
        if not self.__should_balance(current_advice):
            return

        worker_to_num_tasks = {worker: len(task_ids) for worker, task_ids in current_advice.items()}
        logger.info(f"balancing task: {worker_to_num_tasks}")
        for worker, task_ids in current_advice.items():
            await self._binder_monitor.send(StateBalanceAdvice(workerId=worker, taskIds=task_ids))

        self._last_balance_advice = current_advice
        for worker, task_ids in current_advice.items():
            for task_id in task_ids:
                await self._task_controller.on_task_balance_cancel(task_id)

    def __should_balance(self, current_advice: dict[WorkerID, list[TaskID]]) -> bool:
        # 1. if this is the same advise as last time, then we +1 on same advice count
        # 2. if there is another different advice come in, then we reset same advice count to 0
        if self._last_balance_advice == current_advice:
            self._same_load_balance_advice_count += 1
        else:
            self._last_balance_advice = current_advice
            self._same_load_balance_advice_count = 0

        # Act exactly once, when the advice has been stable for the trigger count, then wait for it to change.
        # Acting on every cycle beyond the trigger re-issues moves that are still in flight -- if a saturated
        # worker is slow to confirm a balance-cancel, the same task is re-advised forever.
        if self._same_load_balance_advice_count != self._config_controller.get_config("load_balance_trigger_times"):
            return False

        # if current advice is empty, then we skip
        if not current_advice:
            return False

        return True
