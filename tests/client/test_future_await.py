"""Tests for ``ScalerFuture.__await__`` -- the async-await entrypoint used by
browser notebooks that prefer ``await client.submit(...)`` over the
synchronous ``.result()`` path.

These tests do not spin up a scheduler; they construct a ``ScalerFuture``
with mocked connectors and drive it via ``set_result`` / ``set_exception`` to
exercise only the await bridge.
"""

import asyncio
import unittest
from typing import Any
from unittest.mock import Mock

from scaler.client.future import ScalerFuture
from scaler.client.object_buffer import ObjectBuffer
from scaler.client.serializer.default import DefaultSerializer
from scaler.io.mixins import SyncConnector
from scaler.protocol.capnp import Task
from scaler.utility.identifiers import ClientID, ObjectID, TaskID


def _make_future(is_delayed: bool = False) -> ScalerFuture:
    client_id = ClientID.generate_client_id()
    task = Task(
        taskId=TaskID.generate_task_id(),
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities={},
    )
    fut = ScalerFuture(
        task=task,
        is_delayed=is_delayed,
        group_task_id=None,
        serializer=DefaultSerializer(),
        connector_agent=Mock(spec=SyncConnector),
        object_buffer=Mock(spec=ObjectBuffer),
    )
    fut.set_running_or_notify_cancel()
    return fut


class ScalerFutureAwaitTest(unittest.IsolatedAsyncioTestCase):
    async def test_await_returns_result_set_before_await(self) -> None:
        fut = _make_future()
        fut.set_result(42)
        got = await fut
        self.assertEqual(got, 42)

    async def test_await_returns_result_set_during_await(self) -> None:
        fut = _make_future()
        loop = asyncio.get_event_loop()
        # Schedule a late result set on the same loop.
        loop.call_later(0.01, fut.set_result, "hello")
        got = await fut
        self.assertEqual(got, "hello")

    async def test_await_propagates_exception(self) -> None:
        fut = _make_future()
        fut.set_exception(ValueError("boom"))
        with self.assertRaises(ValueError):
            await fut

    async def test_await_after_cancel_raises(self) -> None:
        fut = _make_future()
        fut.set_canceled()
        with self.assertRaises(asyncio.CancelledError):
            await fut


class ScalerFutureSyncPathUnaffectedTest(unittest.TestCase):
    """``__await__`` must not break the synchronous ``.result()`` path that
    native CPython clients rely on."""

    def test_result_still_works(self) -> None:
        fut = _make_future()
        fut.set_result(123)
        self.assertEqual(fut.result(timeout=1), 123)

    def test_exception_still_works(self) -> None:
        fut = _make_future()
        fut.set_exception(RuntimeError("x"))
        with self.assertRaises(RuntimeError):
            fut.result(timeout=1)


class ScalerFutureEmscriptenResultTest(unittest.TestCase):
    """Regression test for the wasm deadlock fix in ``_wait_result_ready``.

    Under Pyodide, ``ScalerFuture.result()`` cannot use
    ``threading.Condition.wait`` because the client agent task runs on the
    same single-threaded asyncio loop as the caller; blocking the thread
    would prevent the agent from ever signalling completion.

    The fix takes the ``sys.platform == "emscripten"`` branch and suspends
    via ``pyodide.ffi.run_sync(asyncio.wrap_future(self))``. These tests
    monkey-patch ``sys.platform`` and inject a fake ``pyodide.ffi`` module
    so the behaviour can be exercised on a regular CPython interpreter.
    """

    def setUp(self) -> None:
        import sys as _sys

        self._real_platform = _sys.platform
        # Force ``sys.platform`` to ``"emscripten"`` so ``_wait_result_ready``
        # takes the JSPI branch.
        _sys.platform = "emscripten"

        # Inject a fake ``pyodide.ffi.run_sync`` that drives the awaitable on
        # a fresh background-thread asyncio loop. ``asyncio.wrap_future`` is
        # thread-safe, so completing the future from any thread will resume
        # the coroutine running inside ``run_sync``.
        import asyncio as _asyncio
        import threading as _threading
        import types as _types

        def _fake_run_sync(awaitable: Any) -> Any:
            holder: dict = {}

            def _runner() -> None:
                try:
                    holder["result"] = _asyncio.new_event_loop().run_until_complete(awaitable)
                except BaseException as exc:  # noqa: BLE001
                    holder["error"] = exc

            t = _threading.Thread(target=_runner)
            t.start()
            t.join()
            if "error" in holder:
                raise holder["error"]
            return holder.get("result")

        fake_ffi = _types.ModuleType("pyodide.ffi")
        fake_ffi.run_sync = _fake_run_sync  # type: ignore[attr-defined]
        fake_pkg = _types.ModuleType("pyodide")
        fake_pkg.ffi = fake_ffi  # type: ignore[attr-defined]

        self._real_pyodide = _sys.modules.pop("pyodide", None)
        self._real_pyodide_ffi = _sys.modules.pop("pyodide.ffi", None)
        _sys.modules["pyodide"] = fake_pkg
        _sys.modules["pyodide.ffi"] = fake_ffi

    def tearDown(self) -> None:
        import sys as _sys

        _sys.platform = self._real_platform
        _sys.modules.pop("pyodide", None)
        _sys.modules.pop("pyodide.ffi", None)
        if self._real_pyodide is not None:
            _sys.modules["pyodide"] = self._real_pyodide
        if self._real_pyodide_ffi is not None:
            _sys.modules["pyodide.ffi"] = self._real_pyodide_ffi

    def test_result_already_set_returns_without_jspi(self) -> None:
        fut = _make_future()
        fut.set_result(7)
        # ``done()`` short-circuits before any JSPI call.
        self.assertEqual(fut.result(timeout=1), 7)

    def test_result_set_from_other_thread_unblocks(self) -> None:
        """Without the fix, ``result()`` would deadlock under emscripten."""
        import threading

        fut = _make_future()
        threading.Timer(0.05, lambda: fut.set_result("ok")).start()
        self.assertEqual(fut.result(timeout=2), "ok")

    def test_wait_result_ready_swallows_cancellation(self) -> None:
        """Regression: under emscripten, ``cancel()`` calls
        ``_wait_result_ready`` to wait for the cancel confirmation. Once the
        future transitions to cancelled, ``asyncio.wrap_future`` raises
        ``CancelledError`` -- but the native ``Condition.wait`` path returns
        silently in that case, so callers like ``Client.disconnect()`` ->
        ``cancel_all_futures()`` must not see the exception bubble up."""
        import threading

        fut = _make_future()
        threading.Timer(0.05, fut.set_canceled).start()
        # Must not raise CancelledError; cancel() returns once the future
        # has settled.
        self.assertTrue(fut.cancel(timeout=2))
        self.assertTrue(fut.cancelled())


if __name__ == "__main__":
    unittest.main()
