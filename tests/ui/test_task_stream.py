"""The worker task stream: one bar per task a processor actually ran.

The scheduler reports a task running when it dispatches it, so a worker holds a queue of tasks that have not started.
Only the processors say what is on a core, and that is what the stream draws.
"""

import unittest
from typing import Any, Dict, List

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import StateTask, StateWorker, TaskState, WorkerState
from scaler.ui.app import TaskStreamState, WebGUIConfig, WebUIApp
from scaler.utility.metadata.profile_result import ProfileResult

WORKER = "Worker|one"


def make_app() -> WebUIApp:
    return WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))


def task_id(index: int) -> bytes:
    return index.to_bytes(16, "big")


def dispatch(stream: TaskStreamState, index: int, worker: str = WORKER) -> None:
    """The scheduler handing a task to a worker, which is where it joins that worker's queue."""
    stream.handle_task_state(
        StateTask.from_bytes(
            StateTask(
                taskId=task_id(index),
                functionName=b"work",
                state=TaskState.running,
                worker=worker.encode(),
                capabilities=[],
                metadata=b"",
            ).to_bytes()
        )
    )


def finish(stream: TaskStreamState, index: int, duration_s: float, worker: str = WORKER) -> None:
    stream.handle_task_state(
        StateTask.from_bytes(
            StateTask(
                taskId=task_id(index),
                functionName=b"work",
                state=TaskState.success,
                worker=worker.encode(),
                capabilities=[],
                metadata=ProfileResult(duration_s=duration_s, memory_peak=0).serialize(),
            ).to_bytes()
        )
    )


def running_bars(stream: TaskStreamState) -> List[Dict[str, Any]]:
    """Bars the stream draws as still running, which it outlines in yellow."""
    return [bar for bar in stream.get_render_data(5)["bars"] if bar["oc"] == "#eab308"]


class TestQueuedTasksAreNotDrawn(unittest.TestCase):
    def test_a_dispatched_task_draws_nothing_until_a_processor_holds_it(self) -> None:
        stream = TaskStreamState()
        for index in range(5):
            dispatch(stream, index)

        self.assertEqual(running_bars(stream), [], "a queue of five tasks is not five running bars")
        self.assertEqual(stream.get_render_data(5)["rows"], [], "an idle worker holding a queue has no row")

    def test_a_processor_starts_the_bar_where_the_worker_says_it_started(self) -> None:
        stream = TaskStreamState()
        for index in range(5):
            dispatch(stream, index)
        stream.handle_worker_processors(WORKER, [(task_id(0), 30)])

        bars = running_bars(stream)
        self.assertEqual(len(bars), 1, "only the task on a processor is running")
        self.assertAlmostEqual(bars[0]["x"], -30.0, delta=1.0, msg="the bar begins at the age the worker reports")
        self.assertIn("Running", bars[0]["h"])

    def test_a_second_processor_adds_a_second_bar(self) -> None:
        stream = TaskStreamState()
        for index in range(5):
            dispatch(stream, index)
        stream.handle_worker_processors(WORKER, [(task_id(0), 10), (task_id(1), 4)])

        self.assertEqual(len(running_bars(stream)), 2)

    def test_a_task_the_processors_let_go_of_stops_running(self) -> None:
        stream = TaskStreamState()
        dispatch(stream, 0)
        stream.handle_worker_processors(WORKER, [(task_id(0), 10)])
        stream.handle_worker_processors(WORKER, [])

        self.assertEqual(running_bars(stream), [])

    def test_a_finished_task_leaves_a_bar_of_its_own_length(self) -> None:
        stream = TaskStreamState()
        dispatch(stream, 0)
        stream.handle_worker_processors(WORKER, [(task_id(0), 8)])
        finish(stream, 0, duration_s=8.0)

        data = stream.get_render_data(5)
        self.assertEqual(running_bars(stream), [])
        completed = [bar for bar in data["bars"] if bar["oc"] != "#eab308"]
        self.assertEqual(len(completed), 1)
        self.assertAlmostEqual(completed[0]["w"], 8.0, delta=1.0)

    def test_a_departed_worker_leaves_nothing_running(self) -> None:
        stream = TaskStreamState()
        dispatch(stream, 0)
        stream.handle_worker_processors(WORKER, [(task_id(0), 3)])
        stream.handle_worker_state(
            StateWorker.from_bytes(
                StateWorker(workerId=WORKER.encode(), state=WorkerState.disconnected, capabilities=[]).to_bytes()
            )
        )

        self.assertEqual(running_bars(stream), [])


class TestStreamFromTheStatusFrame(unittest.TestCase):
    def test_the_app_feeds_the_stream_what_the_processors_hold(self) -> None:
        """The status frame is where the running set comes from, so the app must pass it on."""
        from tests.ui.test_worker_details import dispatch as dispatch_task
        from tests.ui.test_worker_details import report

        app = make_app()
        for index in range(4):
            dispatch_task(app, index)
        report(app, running=[0], queued=3)

        bars = running_bars(app._task_stream)
        self.assertEqual(len(bars), 1, "three queued tasks must not draw bars")


if __name__ == "__main__":
    unittest.main()
