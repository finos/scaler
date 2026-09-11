"""The GUI state is written by the batcher thread and read by each browser's own connection thread.

`http.server` gives every connection its own thread, so a browser reads this state as the batcher rewrites it.
Without one lock over both, a reader walks a collection being replaced and the request dies with a traceback.
"""

import threading
import time
import unittest
from typing import Any, Callable, Dict, List

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    Resource,
    ScalingManagerStatus,
    StateScheduler,
    StateTask,
    TaskManagerStatus,
    TaskState,
    WorkerManagerStatus,
    WorkerStatus,
)
from scaler.ui.app import BrowserView, WebGUIConfig, WebUIApp

RUN_SECONDS = 2.0
WORKERS_PER_FRAME = 40


def make_status(generation: int) -> StateScheduler:
    """A frame whose workers are all new, so applying it replaces every worker row."""
    return StateScheduler.from_bytes(
        StateScheduler(
            binder=BinderStatus(received=[], sent=[]),
            scheduler=Resource(cpu=0, rss=0),
            rssFree=0,
            clientManager=ClientManagerStatus(clients=[]),
            objectManager=ObjectManagerStatus(numberOfObjects=0),
            taskManager=TaskManagerStatus(stateToCount=[]),
            workerManager=WorkerManagerStatus(
                workers=[
                    WorkerStatus(
                        workerId=f"Worker|{generation}|{index}".encode(),
                        agent=Resource(cpu=0, rss=0),
                        rssFree=1,
                        memLimit=2,
                        free=0,
                        sent=0,
                        queued=0,
                        suspended=0,
                        lagUS=0,
                        lastS=0,
                        itl=" ",
                        processorStatuses=[],
                        hostname="box-1",
                        netSentBytes=0,
                        netRecvBytes=0,
                    )
                    for index in range(WORKERS_PER_FRAME)
                ]
            ),
            scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
        ).to_bytes()
    )


class TestStateLock(unittest.TestCase):
    def test_a_browser_can_read_while_the_batcher_writes(self) -> None:
        app = WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))
        failures: List[BaseException] = []
        stop = threading.Event()
        self.addCleanup(stop.set)  # a failure before the join below must not leave the threads running

        def batch() -> None:
            generation = 0
            while not stop.is_set():
                generation += 1
                app._on_monitor_message(make_status(generation))
                app._on_monitor_message(
                    StateTask.from_bytes(
                        StateTask(
                            taskId=generation.to_bytes(32, "big"), functionName=b"work", state=TaskState.success
                        ).to_bytes()
                    )
                )
                with app._state_lock:
                    app._batch_once()

        def read(build: Callable[[BrowserView], Dict[str, Any]]) -> None:
            while not stop.is_set():
                try:
                    build(BrowserView())
                except BaseException as error:  # noqa: B036, the point is that nothing escapes
                    failures.append(error)
                    return

        threads = [threading.Thread(target=batch)]
        threads += [threading.Thread(target=read, args=(app.get_full_state,)) for _ in range(2)]
        threads += [threading.Thread(target=read, args=(app.view_update,)) for _ in range(2)]

        for thread in threads:
            thread.start()
        time.sleep(RUN_SECONDS)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(failures, [], f"a browser thread failed while the batcher was writing: {failures[:3]}")


if __name__ == "__main__":
    unittest.main()
