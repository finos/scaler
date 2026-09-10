"""The ``soamapi.SessionCallback`` subclass that forwards session events to a ``TaskResponseRouter``.

See this package's ``__init__`` for the import rule that keeps it reachable without Symphony.
"""

from __future__ import annotations

import soamapi

from scaler.worker_manager.proxy.symphony.response_router import TaskResponseRouter


class SoamSessionCallback(soamapi.SessionCallback):
    def __init__(self, response_router: TaskResponseRouter) -> None:
        self._response_router = response_router

    def on_response(self, task_output_handle: soamapi.TaskOutputHandle) -> None:
        self._response_router.on_response(task_output_handle)

    def on_exception(self, exception: soamapi.SoamException) -> None:
        self._response_router.on_exception(exception)
