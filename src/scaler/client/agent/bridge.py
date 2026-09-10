"""Abstracts the connection between the user-facing ``Client`` and the background
``ClientAgent`` so that different execution environments can share the same
``Client`` code.

On native CPython, the ``Client`` runs in the user's thread and the
``ClientAgent`` runs its asyncio loop in a background thread. The two
communicate over an "internal" IPC socket pair (``ipc://`` or ``tcp://`` on
Windows) so the ``Client`` can submit tasks synchronously while the agent
handles network I/O and manager state on its own loop.

Browser / Pyodide environments cannot start threads and cannot create IPC
sockets, but they can run the same ``ClientAgent`` logic directly on the single
available asyncio loop (via JSPI's ``pyodide.ffi.run_sync`` to keep the
``Client``'s sync API). This module defines the interface both paths must
satisfy, and implements both the native (``IPCAgentBridge``) and browser
(``InProcessAgentBridge``) paths.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import struct
import sys
import threading
import time
import uuid
from typing import Any, Awaitable, Callable, Coroutine, Iterable, Iterator, Optional, TypeVar

from scaler.client.agent.client_agent import ClientAgent
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.agent.mixins import ClientAgentBridge
from scaler.client.serializer.mixins import Serializer
from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.mixins import (
    AsyncConnector,
    AsyncObjectStorageConnector,
    ConnectorRemoteType,
    NetworkBackend,
    SyncConnector,
)
from scaler.io.utility import serialize as _capnp_serialize
from scaler.protocol.capnp import BaseMessage, ClientHeartbeat, Resource
from scaler.utility.exceptions import ClientQuitException
from scaler.utility.identifiers import ClientID

T = TypeVar("T")


class IPCAgentBridge(ClientAgentBridge):
    """Native-CPython bridge that runs the ``ClientAgent`` on a background thread.

    Creates a dedicated "internal" connector address, instantiates the agent
    with that address on the bind side, and exposes a ``SyncConnector`` on the
    connect side so the ``Client`` can ``send()``/``receive()`` synchronously
    from the user's thread while the agent's loop runs concurrently.

    This is the historical behavior of ``Client.__initialize__``; the class
    only wraps it behind a common interface so the browser path can supply a
    drop-in replacement.
    """

    def __init__(
        self,
        *,
        identity: ClientID,
        scheduler_address: AddressConfig,
        network_backend: NetworkBackend,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
        object_storage_address: Optional[str] = None,
        security_config: Optional[SecurityConfig] = None,
    ) -> None:
        self._identity = identity
        self._backend = network_backend
        self._security_config = security_config

        self._client_agent_address = self._backend.create_internal_address(
            f"scaler_client_{uuid.uuid4().hex}", same_process=True
        )

        self._agent = ClientAgent(
            identity=identity,
            client_agent_address=self._client_agent_address,
            scheduler_address=scheduler_address,
            network_backend=network_backend,
            future_manager=future_manager,
            stop_event=stop_event,
            timeout_seconds=timeout_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            serializer=serializer,
            object_storage_address=object_storage_address,
            security_config=security_config,
        )

        self._connector: Optional[SyncConnector] = None

    def start(self) -> None:
        self._agent.start()

    @property
    def connector(self) -> SyncConnector:
        if self._connector is None:
            # Lazily created so we don't pay the cost until the client actually asks for it.
            self._connector = self._backend.create_sync_connector(
                identity=self._identity,
                connector_remote_type=ConnectorRemoteType.Connector,
                address=self._client_agent_address,
            )
        return self._connector

    def is_alive(self) -> bool:
        return self._agent.is_alive()

    def join(self) -> None:
        self._agent.join()

    def run_in_agent(self, coroutine: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        try:
            return self._agent.run_in_agent(coroutine)
        except RuntimeError as exc:
            # The agent terminated (e.g. lost contact with the scheduler) and closed its event loop. Closing the
            # coroutine avoids a "coroutine was never awaited" warning.
            coroutine.close()
            raise ClientQuitException("client agent is not running anymore.") from exc

    async def object_storage_connector(self) -> AsyncObjectStorageConnector:
        return await self._agent.get_object_storage_connector()


# ---------------------------------------------------------------------------
# In-process / browser bridge.
#
# Implements the same ``ClientAgentBridge`` interface without using threads or
# real IPC sockets. The ``ClientAgent`` coroutine runs on the user's asyncio
# loop (the only loop available under Pyodide) and the two connectors linking
# the agent to the client are in-memory queues.
#
# The sync half of the connector pair blocks via ``pyodide.ffi.run_sync``
# (JSPI), which suspends the current WebAssembly stack while the asyncio loop
# continues to drive the coroutine. JSPI is required; see ``Client`` for the
# preflight check. The queues exchange ``BaseMessage`` objects directly -- no
# serialization round-trip is performed between client and agent.


def _run_sync(coro: Awaitable[Any]) -> Any:
    """Drive an awaitable to completion from a synchronous stack via JSPI.

    Only valid on Pyodide with JSPI enabled; the import is guarded at call
    time so this module is safely importable in any Python environment for
    unit-testing.
    """
    from pyodide.ffi import run_sync

    return run_sync(coro)


# Browser-side heartbeat to the scheduler.
#
# Background. In the browser, the agent's heartbeat coroutine shares the
# single asyncio loop with the user's notebook code. Long synchronous pure-
# Python work (cloudpickle of large constants, pargraph graph construction,
# numpy result hand-off) blocks the loop entirely, the heartbeat task never
# runs, and the scheduler's ``client_timeout_seconds`` (default 60s) trips
# and disconnects the client mid-computation.
#
# A previous attempt drove the heartbeat from ``sys.setprofile``. That works
# for pure-Python heavy code (e.g. cloudpickle has many call/return events),
# but breaks down when the wasm stack is JSPI-suspended inside a single
# ``run_sync`` call (e.g. the Client's initial scheduler handshake):
# the user's Python stack has no frames executing while suspended, so the
# profile callback never fires.
#
# This implementation uses a JavaScript ``setInterval`` instead. The timer
# fires from the browser's event loop, which runs even while wasm is
# JSPI-suspended. The timer callback is pure JS (a closure that captures
# the WebSocket and a pre-built ``Uint8Array``) so it never re-enters wasm
# or Python -- it just calls ``ws.send(buf)``. The buf contains a YMQ-framed
# ``ClientHeartbeat`` capnp message with zeros for cpu/rss/latency: the
# scheduler only cares about the arrival, not the contents.
#
# The timer is installed via a class-level ``post_open_hook`` on
# ``ConnectorSocket`` that fires when the WebSocket completes its YMQ
# handshake. The hook is set in ``InProcessAgentBridge.start()`` and
# filters by socket-instance identity so only the agent's scheduler
# connector gets a heartbeat timer (not the object-storage connector or
# any other ConnectorSockets the user may create).

# YMQ wire framing: 8-byte little-endian length prefix. Mirror of
# ``_HEADER_FORMAT`` in scaler/io/ymq/_ymq_wasm.py; not imported from there
# to keep the wasm backend off the import path on native CPython.
_YMQ_HEADER_FORMAT: str = "<Q"

# Heartbeat tick interval, in milliseconds. Set well under the default 60s
# ``client_timeout_seconds`` so a few missed ticks don't trip eviction.
_HEARTBEAT_INTERVAL_MS: int = 5000

_js_heartbeat_state: dict = {
    # JS ``setInterval`` handle, returned by the install factory.
    "timer_id": None,
    # The ``ConnectorSocket`` instance the timer is attached to. Held so
    # we can release its WebSocket reference on teardown.
    "socket": None,
    # The agent whose scheduler connector we are protecting. Used by the
    # ``post_open_hook`` closure to filter sockets.
    "agent": None,
    # JsProxy of the JS-side telemetry object. Exposes ``fire_count``,
    # ``send_count``, ``send_error_count``, ``last_send_error``,
    # ``last_ws_state``, ``last_fire_ms``, ``last_send_ms`` so the diagnostic
    # cell can inspect what the timer actually did. Kept on the Python side
    # so its lifetime matches the timer.
    "js_state": None,
    # Diagnostics -- read from a notebook with
    # ``from scaler.client.agent import bridge; bridge.heartbeat_diagnostic()``
    # or by inspecting ``bridge._js_heartbeat_state`` directly.
    "hook_install_count": 0,
    "timer_install_count": 0,
    "last_install_error": None,
    "last_install_time": None,
    "framed_heartbeat_size": None,
}


def _build_framed_heartbeat() -> bytes:
    payload = _capnp_serialize(ClientHeartbeat(resource=Resource(cpu=0, rss=0), latencyUS=0))
    return struct.pack(_YMQ_HEADER_FORMAT, len(payload)) + payload


def _install_js_heartbeat_timer(socket: Any) -> None:
    """Attach a pure-JS ``setInterval`` to ``socket._ws`` that sends a
    framed ClientHeartbeat every ``_HEARTBEAT_INTERVAL_MS`` ms.

    Idempotent: a no-op if a timer is already installed on ``socket``.

    On failure the reason lands in ``_js_heartbeat_state['last_install_error']``
    instead of propagating, so a broken heartbeat never tears down the agent.
    """
    if _js_heartbeat_state.get("timer_id") is not None and _js_heartbeat_state.get("socket") is socket:
        return
    # Tear down any prior timer first (e.g. socket was rebuilt on reconnect).
    _uninstall_js_heartbeat_timer()

    try:
        import js
        from pyodide.code import run_js

        framed = _build_framed_heartbeat()
        _js_heartbeat_state["framed_heartbeat_size"] = len(framed)
        js_buf = js.Uint8Array.new(len(framed))
        js_buf.assign(memoryview(framed))

        # Build a JS factory ONCE that closes over:
        #   * the WebSocket
        #   * the pre-encoded heartbeat bytes (held as a JS Array of numbers
        #     so it cannot be transferred / detached by ws.send)
        #   * a state object whose mutable fields the JS timer can update so
        #     Python can read them later
        #
        # The timer callback is pure JS -- no Python or wasm re-entry -- so
        # it fires even when the wasm stack is JSPI-suspended inside a long-
        # running ``run_sync`` call.
        #
        # A fresh Uint8Array is built per tick: although WebSocket.send is
        # spec'd to copy binary payloads synchronously, some implementations
        # have detached typed-array buffers in the past, and a silent
        # detachment is the failure mode that motivated this hardening. The
        # cost is one tiny per-tick allocation.
        make_timer = run_js("""
            (ws, srcBytes, intervalMs, state) => {
                const srcArr = Array.from(srcBytes);
                const timerId = setInterval(() => {
                    state.fire_count = (state.fire_count || 0) + 1;
                    state.last_fire_ms = Date.now();
                    state.last_ws_state = ws ? ws.readyState : -1;
                    if (!ws || ws.readyState !== 1) {
                        return;
                    }
                    try {
                        const fresh = new Uint8Array(srcArr);
                        ws.send(fresh);
                        state.send_count = (state.send_count || 0) + 1;
                        state.last_send_ms = Date.now();
                    } catch (e) {
                        state.send_error_count = (state.send_error_count || 0) + 1;
                        state.last_send_error = String(e && e.message ? e.message : e);
                    }
                }, intervalMs);
                state.timer_id = timerId;
                state.install_ms = Date.now();
                return timerId;
            }
            """)

        # Plain JS object that the timer mutates. Created via ``js.Object.new``
        # so the JsProxy round-trips correctly (a Python dict here would be
        # converted to a Map, whose properties aren't accessible via
        # ``state.fire_count`` from JS).
        js_state = js.Object.new()
        # Pre-seed counter fields so a "never fired" timer is visibly distinct
        # from a "fired N times" timer when inspected from Python.
        js_state.fire_count = 0
        js_state.send_count = 0
        js_state.send_error_count = 0
        js_state.last_send_error = None
        js_state.last_ws_state = -1
        js_state.last_fire_ms = 0
        js_state.last_send_ms = 0
        js_state.install_ms = 0

        timer_id = make_timer(socket._ws, js_buf, _HEARTBEAT_INTERVAL_MS, js_state)  # noqa: SLF001

        _js_heartbeat_state["timer_id"] = timer_id
        _js_heartbeat_state["socket"] = socket
        _js_heartbeat_state["js_state"] = js_state
        _js_heartbeat_state["timer_install_count"] += 1
        _js_heartbeat_state["last_install_time"] = time.time()
        _js_heartbeat_state["last_install_error"] = None
    except BaseException as exc:
        _js_heartbeat_state["last_install_error"] = repr(exc)


def _uninstall_js_heartbeat_timer() -> None:
    timer_id = _js_heartbeat_state.get("timer_id")
    if timer_id is None:
        return
    try:
        import js

        js.clearInterval(timer_id)
    except BaseException as exc:
        _js_heartbeat_state["last_install_error"] = f"clearInterval: {exc!r}"
    _js_heartbeat_state["timer_id"] = None
    _js_heartbeat_state["socket"] = None
    _js_heartbeat_state["js_state"] = None


def heartbeat_diagnostic() -> dict:
    """Snapshot Python + JS heartbeat counters as a plain dict.

    Safe to call from any environment (returns just the Python state when
    no JS state is attached). Intended for notebook cells:

        from scaler.client.agent import bridge
        bridge.heartbeat_diagnostic()
    """
    snapshot = {k: v for k, v in _js_heartbeat_state.items() if k != "js_state"}
    js_state = _js_heartbeat_state.get("js_state")
    if js_state is None:
        snapshot["js"] = None
        return snapshot
    # Read every counter via attribute access. Any None or missing field
    # comes back as ``None`` rather than raising, so the snapshot is always
    # printable.
    js_view: dict = {}
    for name in (
        "fire_count",
        "send_count",
        "send_error_count",
        "last_send_error",
        "last_ws_state",
        "last_fire_ms",
        "last_send_ms",
        "install_ms",
        "timer_id",
    ):
        try:
            js_view[name] = getattr(js_state, name)
        except Exception as exc:  # noqa: BLE001
            js_view[name] = f"<read error: {exc!r}>"
    snapshot["js"] = js_view
    return snapshot


def _setup_browser_websocket_heartbeat(agent: Any) -> None:
    """Register a class-level ``post_open_hook`` on ``ConnectorSocket`` that
    installs a JS heartbeat timer when ``agent``'s scheduler connector's
    WebSocket finishes its YMQ handshake.

    Safe to call multiple times -- replaces the prior hook.
    """
    try:
        from scaler.io.ymq._ymq_wasm import ConnectorSocket
    except ImportError:
        # Not running on emscripten; nothing to do.
        return

    _js_heartbeat_state["agent"] = agent
    _js_heartbeat_state["hook_install_count"] += 1

    def post_open_hook(socket: Any) -> None:
        # Filter: only the agent's external (scheduler) connector should
        # get a heartbeat. Other ConnectorSockets (object storage, user-
        # created) must not, otherwise the remote peer will receive an
        # unexpected ClientHeartbeat frame.
        held_agent = _js_heartbeat_state.get("agent")
        if held_agent is None:
            return
        connector_external = getattr(held_agent, "_connector_external", None)
        if connector_external is None:
            return
        if getattr(connector_external, "_socket", None) is not socket:
            return
        _install_js_heartbeat_timer(socket)

    ConnectorSocket._post_open_hook = staticmethod(post_open_hook)

    # If the socket is already open by the time we install (e.g. very fast
    # local loopback), fire the hook synchronously so we don't miss it.
    connector_external = getattr(agent, "_connector_external", None)
    socket = getattr(connector_external, "_socket", None) if connector_external else None
    if socket is not None and getattr(socket, "_open", False) and not getattr(socket, "_closed", False):
        _install_js_heartbeat_timer(socket)


def _teardown_browser_websocket_heartbeat() -> None:
    _uninstall_js_heartbeat_timer()
    _js_heartbeat_state["agent"] = None
    try:
        from scaler.io.ymq._ymq_wasm import ConnectorSocket

        ConnectorSocket._post_open_hook = None
    except ImportError:
        pass


# Patch ``time.sleep`` so a synchronous sleep in user code (or in a library
# polling for results) hands control back to the asyncio loop instead of
# freezing the whole tab. Without this, ``time.sleep(N)`` blocks the only
# thread for N seconds with no heartbeats; with the patch, the wasm stack
# is suspended via JSPI and the loop continues to drive the agent.

_time_sleep_patched: bool = False
_original_time_sleep: Optional[Callable[[float], None]] = None


def _jspi_time_sleep(secs: float) -> None:
    try:
        if secs is None or secs <= 0:
            _run_sync(asyncio.sleep(0))
            return
        _run_sync(asyncio.sleep(float(secs)))
    except BaseException:
        # If JSPI fails for any reason fall back to the original blocking
        # sleep so callers always observe at least the requested delay.
        if _original_time_sleep is not None:
            _original_time_sleep(secs)


def _install_time_sleep_jspi_patch() -> None:
    global _time_sleep_patched, _original_time_sleep
    if _time_sleep_patched:
        return
    _original_time_sleep = time.sleep
    time.sleep = _jspi_time_sleep  # type: ignore[assignment]
    _time_sleep_patched = True


def _uninstall_time_sleep_jspi_patch() -> None:
    global _time_sleep_patched, _original_time_sleep
    if not _time_sleep_patched:
        return
    if _original_time_sleep is not None:
        time.sleep = _original_time_sleep  # type: ignore[assignment]
    _original_time_sleep = None
    _time_sleep_patched = False


# JSPI-aware patches for ``concurrent.futures.wait`` / ``as_completed``.
#
# The agent coroutine runs on the same single-threaded asyncio loop as the
# user's notebook code in the browser. ``ScalerFuture._wait_result_ready``
# already suspends the wasm stack via JSPI when ``.result()`` is called, so
# the loop keeps running and the agent keeps sending heartbeats. But code
# that blocks on multiple futures via the standard library -- most notably
# ``pargraph.GraphEngine.get`` which calls
# ``concurrent.futures.wait(..., return_when=FIRST_COMPLETED)`` -- uses
# ``threading.Event.wait`` internally, which blocks the only thread without
# letting the loop run. The agent never gets to send heartbeats, the
# scheduler trips ``client_timeout_seconds`` (60s default), and the client
# is disconnected mid-computation.
#
# When the browser bridge starts, monkey-patch ``concurrent.futures.wait``
# and ``concurrent.futures.as_completed`` to drive the asyncio loop via JSPI
# while waiting. The patch only activates on ``sys.platform == "emscripten"``
# and is idempotent.

_concurrent_futures_patched: bool = False
_original_wait: Optional[Callable[..., concurrent.futures._base.DoneAndNotDoneFutures]] = None
_original_as_completed: Optional[Callable[..., Iterator[concurrent.futures.Future]]] = None


def jspi_wait(
    fs: Iterable[concurrent.futures.Future],
    timeout: Optional[float] = None,
    return_when: str = concurrent.futures.ALL_COMPLETED,
) -> concurrent.futures._base.DoneAndNotDoneFutures:
    fs = list(fs)
    if not fs:
        return concurrent.futures._base.DoneAndNotDoneFutures(set(), set())

    asyncio_return_when = {
        concurrent.futures.FIRST_COMPLETED: asyncio.FIRST_COMPLETED,
        concurrent.futures.FIRST_EXCEPTION: asyncio.FIRST_EXCEPTION,
        concurrent.futures.ALL_COMPLETED: asyncio.ALL_COMPLETED,
    }[return_when]

    async def _await() -> None:
        wrapped = [asyncio.wrap_future(f) for f in fs]
        # ``asyncio.wait`` registers callbacks on each wrapped future and
        # returns once ``return_when`` is satisfied (or ``timeout`` elapses).
        # We deliberately do NOT cancel the wrappers afterwards: cancelling
        # an ``asyncio.wrap_future`` wrapper propagates ``cancel()`` to the
        # underlying ``concurrent.futures.Future``, which would silently kill
        # the user's in-flight tasks. The wrappers fall out of scope and are
        # GC'd; their done callbacks are no-ops once the original future
        # completes.
        await asyncio.wait(wrapped, timeout=timeout, return_when=asyncio_return_when)

    _run_sync(_await())

    done: set = set()
    not_done: set = set()
    for f in fs:
        if f.done():
            done.add(f)
        else:
            not_done.add(f)
    return concurrent.futures._base.DoneAndNotDoneFutures(done, not_done)


def jspi_as_completed(
    fs: Iterable[concurrent.futures.Future], timeout: Optional[float] = None
) -> Iterator[concurrent.futures.Future]:
    fs = list(fs)
    pending = set(fs)
    # Yield any already-completed futures up front, mirroring stdlib semantics.
    for f in list(pending):
        if f.done():
            pending.discard(f)
            yield f

    while pending:
        result = jspi_wait(pending, timeout=timeout, return_when=concurrent.futures.FIRST_COMPLETED)
        if not result.done:
            raise concurrent.futures.TimeoutError(f"{len(pending)} (of {len(fs)}) futures unfinished")
        for f in result.done:
            pending.discard(f)
            yield f


def _rebind_in_loaded_modules(old_wait: Any, old_as_completed: Any, new_wait: Any, new_as_completed: Any) -> None:
    # ``from concurrent.futures import wait`` captures a local reference at
    # the importing module's load time, so rebinding ``concurrent.futures.wait``
    # later doesn't affect callers that already imported it (pargraph does
    # exactly this). Walk every loaded module and replace any attribute that
    # still points at the original function. ``concurrent.futures._base``
    # holds the canonical definitions and re-export, so it's covered too.
    for module in list(sys.modules.values()):
        if module is None:
            continue
        try:
            module_dict = getattr(module, "__dict__", None)
            if module_dict is None:
                continue
            for name, value in list(module_dict.items()):
                if value is old_wait:
                    module_dict[name] = new_wait
                elif value is old_as_completed:
                    module_dict[name] = new_as_completed
        except Exception:
            # Some modules raise on __dict__ access (lazy importers, C
            # extensions with dynamic attribute lookup); skip them.
            continue


def _install_concurrent_futures_jspi_patch() -> None:
    global _concurrent_futures_patched, _original_wait, _original_as_completed
    if _concurrent_futures_patched:
        return
    _original_wait = concurrent.futures.wait
    _original_as_completed = concurrent.futures.as_completed
    _rebind_in_loaded_modules(_original_wait, _original_as_completed, jspi_wait, jspi_as_completed)
    _concurrent_futures_patched = True


def _uninstall_concurrent_futures_jspi_patch() -> None:
    global _concurrent_futures_patched, _original_wait, _original_as_completed
    if not _concurrent_futures_patched:
        return
    if _original_wait is not None and _original_as_completed is not None:
        _rebind_in_loaded_modules(jspi_wait, jspi_as_completed, _original_wait, _original_as_completed)
    _original_wait = None
    _original_as_completed = None
    _concurrent_futures_patched = False


_IN_PROCESS_ADDRESS: AddressConfig = AddressConfig(SocketType.inproc, host="scaler-client-agent")


class _InProcessAsyncConnector(AsyncConnector):
    """The agent-side half of the in-process connector pair.

    ``bind`` and ``connect`` are no-ops (there is no real socket to bind).
    ``routine`` pulls one message from the client->agent queue and dispatches
    it to the agent's callback, mirroring the contract of the ymq-backed
    async connector.
    """

    def __init__(
        self,
        identity: bytes,
        callback: Callable[[BaseMessage], Awaitable[None]],
        incoming: "asyncio.Queue[Optional[BaseMessage]]",
        outgoing: "asyncio.Queue[Optional[BaseMessage]]",
    ) -> None:
        self._identity = identity
        self._callback = callback
        self._incoming = incoming  # client -> agent
        self._outgoing = outgoing  # agent -> client
        self._address: Optional[AddressConfig] = None
        self._destroyed = False

    async def bind(self, address: AddressConfig, security_config: Optional[SecurityConfig] = None) -> None:
        self._address = address

    async def connect(
        self, address: AddressConfig, remote_type: ConnectorRemoteType, security_config: Optional[SecurityConfig] = None
    ) -> None:
        self._address = address

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        # Wake up any parked readers on either side with a sentinel.
        try:
            self._incoming.put_nowait(None)
        except asyncio.QueueFull:
            pass
        try:
            self._outgoing.put_nowait(None)
        except asyncio.QueueFull:
            pass

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def send(self, message: BaseMessage, *, detached: bool) -> None:
        if self._destroyed:
            return
        await self._outgoing.put(message)

    async def receive(self) -> Optional[BaseMessage]:
        if self._destroyed:
            return None
        return await self._incoming.get()

    async def routine(self) -> None:
        message = await self.receive()
        if message is None:
            return
        await self._callback(message)


class _InProcessSyncConnector(SyncConnector):
    """The client-side half of the in-process connector pair.

    Uses JSPI's ``run_sync`` to present a synchronous API backed by the same
    ``asyncio.Queue`` objects the agent reads from / writes to.
    """

    def __init__(
        self,
        identity: bytes,
        address: AddressConfig,
        incoming: "asyncio.Queue[Optional[BaseMessage]]",
        outgoing: "asyncio.Queue[Optional[BaseMessage]]",
    ) -> None:
        self._identity = identity
        self._address = address
        self._incoming = incoming  # client -> agent (we write here)
        self._outgoing = outgoing  # agent -> client (we read from here)
        self._destroyed = False

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> AddressConfig:
        return self._address

    def send(self, message: BaseMessage) -> None:
        if self._destroyed:
            return
        _run_sync(self._incoming.put(message))

    def receive(self) -> Optional[BaseMessage]:
        if self._destroyed:
            return None
        return _run_sync(self._outgoing.get())

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        try:
            self._incoming.put_nowait(None)
        except asyncio.QueueFull:
            pass


class _InProcessFuture(concurrent.futures.Future[T]):
    """A future for a coroutine that runs on the browser's single event loop.

    The client and the agent share that event loop, so blocking on a regular future would prevent the agent from ever
    completing it. ``result()`` instead suspends the WebAssembly stack with ``run_sync()``, which lets the event loop
    keep driving the agent.
    """

    def __init__(self, coroutine: Coroutine[Any, Any, T]) -> None:
        super().__init__()

        self._task = asyncio.ensure_future(coroutine)
        self._task.add_done_callback(self.__on_task_done)

    def result(self, timeout: Optional[float] = None) -> T:
        self.__wait(timeout)
        return super().result(timeout=0)

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]:
        self.__wait(timeout)
        return super().exception(timeout=0)

    def __wait(self, timeout: Optional[float]) -> None:
        if self.done():
            return

        # `asyncio.wait()` never propagates the task's exception nor cancels it on timeout, so the outcome is always
        # reported by the base class' methods below.
        _run_sync(asyncio.wait([self._task], timeout=timeout))

    def __on_task_done(self, task: "asyncio.Future") -> None:
        if task.cancelled():
            super().cancel()
            return

        exception = task.exception()
        if exception is not None:
            super().set_exception(exception)
        else:
            super().set_result(task.result())


class InProcessAgentBridge(ClientAgentBridge):
    """Browser / Pyodide bridge. Runs the ``ClientAgent`` coroutine on the
    current asyncio loop instead of on a background thread, and exchanges
    messages with the ``Client`` via in-memory queues.

    Requires JSPI (``pyodide.ffi.run_sync``) so the ``Client``'s synchronous
    API still works; callers must verify JSPI is available before
    instantiating this bridge (``Client`` does the preflight check).
    """

    def __init__(
        self,
        *,
        identity: ClientID,
        scheduler_address: AddressConfig,
        network_backend: NetworkBackend,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
        object_storage_address: Optional[str] = None,
        security_config: Optional[SecurityConfig] = None,
    ) -> None:
        self._identity = identity
        self._stop_event = stop_event
        self._security_config = security_config

        # Queues carry BaseMessage objects directly; the wire protocol between
        # Client and Agent is internal and need not be serialized.
        self._client_to_agent: asyncio.Queue[Optional[BaseMessage]] = asyncio.Queue()
        self._agent_to_client: asyncio.Queue[Optional[BaseMessage]] = asyncio.Queue()

        self._sync_connector = _InProcessSyncConnector(
            identity=identity,
            address=_IN_PROCESS_ADDRESS,
            incoming=self._client_to_agent,
            outgoing=self._agent_to_client,
        )

        def _internal_factory(identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]) -> AsyncConnector:
            return _InProcessAsyncConnector(
                identity=identity, callback=callback, incoming=self._client_to_agent, outgoing=self._agent_to_client
            )

        self._agent = ClientAgent(
            identity=identity,
            client_agent_address=_IN_PROCESS_ADDRESS,
            scheduler_address=scheduler_address,
            network_backend=network_backend,
            future_manager=future_manager,
            stop_event=stop_event,
            timeout_seconds=timeout_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            serializer=serializer,
            object_storage_address=object_storage_address,
            internal_connector_factory=_internal_factory,
            security_config=security_config,
        )

        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("InProcessAgentBridge.start() may only be called once")
        loop = asyncio.get_event_loop()
        # Schedule the agent's entry coroutine on the current loop. ClientAgent
        # normally has this wrapped by threading.Thread.run() -> run_task_forever,
        # but in-process we drive it as a plain asyncio task.
        self._task = loop.create_task(self._agent._run())  # noqa: SLF001
        self._running = True
        # Make ``concurrent.futures.wait`` / ``as_completed`` JSPI-aware so
        # libraries like pargraph (which block on multiple futures via the
        # standard library) keep the asyncio loop running and let the agent
        # send heartbeats. Only active on emscripten because run_sync
        # requires JSPI.
        if sys.platform == "emscripten":
            _install_concurrent_futures_jspi_patch()
            _install_time_sleep_jspi_patch()
            _setup_browser_websocket_heartbeat(self._agent)

    @property
    def connector(self) -> SyncConnector:
        return self._sync_connector

    def is_alive(self) -> bool:
        if self._task is None:
            return False
        return self._running and not self._task.done()

    def run_in_agent(self, coroutine: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        if not self.is_alive():
            # Closing the coroutine avoids a "coroutine was never awaited" warning.
            coroutine.close()
            raise ClientQuitException("client agent is not running anymore.")

        # The agent runs on this very event loop, so the coroutine is scheduled directly instead of going through
        # `asyncio.run_coroutine_threadsafe()`, which would deadlock.
        return _InProcessFuture(coroutine)

    async def object_storage_connector(self) -> AsyncObjectStorageConnector:
        return await self._agent.get_object_storage_connector()

    def join(self) -> None:
        if self._task is None:
            return
        self._running = False
        if sys.platform == "emscripten":
            _teardown_browser_websocket_heartbeat()
            _uninstall_time_sleep_jspi_patch()
            _uninstall_concurrent_futures_jspi_patch()

        async def _await_task() -> None:
            try:
                await self._task
            except asyncio.CancelledError:
                return
            except BaseException:
                # Match IPCAgentBridge: join() swallows terminal errors so
                # ``Client.__destroy`` can finish its cleanup even when the
                # agent failed. Errors already surfaced through futures.
                return

        _run_sync(_await_task())


def create_default_bridge(
    *,
    identity: ClientID,
    scheduler_address: AddressConfig,
    network_backend: NetworkBackend,
    future_manager: ClientFutureManager,
    stop_event: threading.Event,
    timeout_seconds: int,
    heartbeat_interval_seconds: int,
    serializer: Serializer,
    object_storage_address: Optional[str] = None,
    security_config: Optional[SecurityConfig] = None,
) -> ClientAgentBridge:
    """Pick the bridge implementation appropriate for the current platform.

    Native CPython -> ``IPCAgentBridge`` (threaded, IPC).
    Pyodide / Emscripten -> ``InProcessAgentBridge`` (single-loop, JSPI).
    """
    bridge_cls: type[ClientAgentBridge]
    if sys.platform == "emscripten":
        bridge_cls = InProcessAgentBridge
    else:
        bridge_cls = IPCAgentBridge

    return bridge_cls(
        identity=identity,
        scheduler_address=scheduler_address,
        network_backend=network_backend,
        future_manager=future_manager,
        stop_event=stop_event,
        timeout_seconds=timeout_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        serializer=serializer,
        object_storage_address=object_storage_address,
        security_config=security_config,
    )


def check_browser_runtime() -> None:
    """Raise ``RuntimeError`` if the current runtime cannot host a ``Client``.

    The browser bridge (``InProcessAgentBridge``) drives the agent coroutine
    on the same event loop as the user and uses JavaScript Promise
    Integration (``pyodide.ffi.run_sync``) to keep ``Client``'s synchronous
    public API working. When JSPI is not available the sync API would
    deadlock, so we fail fast with an actionable error instead.

    On non-emscripten platforms this is a no-op.
    """
    if sys.platform != "emscripten":
        return

    try:
        from pyodide.ffi import run_sync  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Scaler's browser client requires Pyodide's JavaScript Promise Integration (JSPI). "
            "pyodide.ffi.run_sync could not be imported. "
            "Please use a Pyodide build that exposes JSPI (Pyodide 0.27+ with a JSPI-capable browser, "
            "e.g. Chrome/Edge 137+). Alternatively, use 'await client.submit(...)' instead of the "
            "blocking sync API."
        ) from exc
