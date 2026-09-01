"""Subject process for ``tests/io/test_ymq_interpreter_shutdown.py``.

Each scenario leaves a ymq socket alive in a module global and returns. The socket is therefore still
alive when the interpreter starts finalizing, which is the situation that used to dead-lock (see
finos/opengris-scaler#945). Run as ``python -m tests.io.interpreter_shutdown_subject <scenario>``;
the test asserts the process exits cleanly rather than aborting or hanging.

This is not a test module itself, and is named so that test discovery skips it.
"""

import asyncio
import sys
from typing import Any, Callable, Coroutine

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import ConnectorRemoteType
from scaler.io.ymq import IOContext
from scaler.io.ymq_async_binder import YMQAsyncBinder
from scaler.io.ymq_async_connector import YMQAsyncConnector

LOOPBACK_ANY_PORT = "tcp://127.0.0.1:0"

# Long enough for the routine task to reach recv_message(), so the C++ socket really is holding a
# pending receive callback. Draining that callback is what makes the shutdown re-enter Python.
PENDING_RECEIVE_DELAY_SECONDS = 0.5

# Module globals are only dropped when the interpreter finalizes, which is exactly what these
# scenarios need. A local would be collected while the interpreter is still fully functional.
LEAKED: dict[str, object] = {}


async def binder_with_pending_receive() -> None:
    """A bound binder holding a pending receive callback, left alive at interpreter exit."""
    binder = YMQAsyncBinder(IOContext(), b"binder-under-test", lambda *_: None)
    await binder.bind(AddressConfig.from_string(LOOPBACK_ANY_PORT))

    LEAKED["routine"] = asyncio.create_task(binder.routine())
    await asyncio.sleep(PENDING_RECEIVE_DELAY_SECONDS)

    LEAKED["binder"] = binder


async def binder_without_python_del() -> None:
    """The same, with ``YMQAsyncBinder.__del__`` removed.

    Removing the Python-level finalizer is not a fix and must not be mistaken for one:
    ``PyBinderSocket_dealloc`` runs the same blocking shutdown that ``__del__`` does, so the process
    still dies. Patching the class like this is only reasonable because it is about to exit.
    """
    del YMQAsyncBinder.__del__

    await binder_with_pending_receive()


async def connector_with_pending_receive() -> None:
    """A connected connector holding a pending receive callback, left alive at interpreter exit.

    ``PyConnectorSocket_dealloc`` has the same blocking-shutdown shape as the binder, so the
    connector needs its own coverage rather than being assumed to follow.
    """
    binder = YMQAsyncBinder(IOContext(), b"binder-under-test", lambda *_: None)
    await binder.bind(AddressConfig.from_string(LOOPBACK_ANY_PORT))

    connector = YMQAsyncConnector(IOContext(), b"connector-under-test", lambda *_: None)
    await connector.connect(binder.address, ConnectorRemoteType.Binder)

    LEAKED["routine"] = asyncio.create_task(connector.routine())
    await asyncio.sleep(PENDING_RECEIVE_DELAY_SECONDS)

    LEAKED["binder"] = binder
    LEAKED["connector"] = connector


SCENARIOS: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {
    "binder_with_pending_receive": binder_with_pending_receive,
    "binder_without_python_del": binder_without_python_del,
    "connector_with_pending_receive": connector_with_pending_receive,
}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SCENARIOS:
        raise SystemExit(f"usage: {sys.argv[0]} {{{','.join(SCENARIOS)}}}")

    asyncio.run(SCENARIOS[sys.argv[1]]())


if __name__ == "__main__":
    main()
