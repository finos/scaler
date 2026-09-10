import asyncio
import concurrent.futures
import logging
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

try:
    from typing import Concatenate, ParamSpec
except ImportError:
    from typing_extensions import Concatenate, ParamSpec  # type: ignore[assignment]

from scaler.config.common.security import SecurityConfig
from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, SocketStopRequestedError, TLSConfig

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


async def call_async(
    func: Callable[Concatenate[Callable[[Union[T, BaseException]], None], P], None], *args: P.args, **kwargs: P.kwargs
) -> T:
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def callback(result: Union[T, BaseException]):
        if loop.is_closed():
            return

        if isinstance(result, BaseException):
            loop.call_soon_threadsafe(_safe_set_exception, future, result)
        else:
            loop.call_soon_threadsafe(_safe_set_result, future, result)

    func(callback, *args, **kwargs)
    return await future


# mypy rejects the keyword-only timeout argument that sits between P.args and P.kwargs, hence the ignore below
def call_sync(  # type: ignore[valid-type]
    func: Callable[Concatenate[Callable[[Union[T, BaseException]], None], P], None],
    *args: P.args,
    timeout: Optional[float] = None,
    **kwargs: P.kwargs,
) -> T:
    future: concurrent.futures.Future = concurrent.futures.Future()

    def callback(result: Union[T, BaseException]):
        if future.done():
            return

        if isinstance(result, BaseException):
            future.set_exception(result)
        else:
            future.set_result(result)

    func(callback, *args, **kwargs)
    return future.result(timeout)


def run_detached(
    awaitable: Awaitable[Any], description: str, on_done_callback: Optional[Callable[[asyncio.Task], None]] = None
) -> asyncio.Task:
    """Schedule an awaitable on the running event loop without waiting for its completion.

    This turns an otherwise blocking asyncio operation into a fire-and-forget one.

    Failures are logged instead of being propagated, as the caller does not wait for the result.

    If provided, `on_done_callback` is called with the completed task once it finishes, including
    when it is cancelled. It is typically used to remove the task from the caller's pending set.

    The returned task must be kept referenced by the caller until completion, as asyncio only keeps weak references to
    running tasks. Callers are also responsible for cancelling it on teardown.
    """

    task = asyncio.ensure_future(awaitable)

    def on_done(completed: asyncio.Task) -> None:
        if on_done_callback is not None:
            on_done_callback(completed)

        if completed.cancelled():
            return

        exception = completed.exception()
        if exception is None:
            return

        if isinstance(exception, (ConnectorSocketClosedByRemoteEndError, SocketStopRequestedError)):
            # The peer left, or our own socket closed during teardown. Routine, and the reason this send
            # is fire-and-forget in the first place.
            logger.debug(f"{description}: detached operation failed: {exception!r}")
        else:
            # Nobody is waiting on this send, so this callback is the only place the failure can surface.
            # Anything that is not a peer going away is a bug, and must not be invisible at default levels.
            logger.warning(f"{description}: detached operation failed: {exception!r}", exc_info=exception)

    task.add_done_callback(on_done)

    return task


def _safe_set_result(future: asyncio.Future, result: Any) -> None:
    if future.done():
        return
    future.set_result(result)


def _safe_set_exception(future: asyncio.Future, exc: BaseException) -> None:
    if future.done():
        return
    future.set_exception(exc)


def to_tls_config(security_config: Optional[SecurityConfig]) -> Optional[TLSConfig]:
    """Convert a Scaler ``SecurityConfig`` into a YMQ ``TLSConfig``."""

    if security_config is None or not security_config.has_credentials():
        return None

    return TLSConfig(cert_chain=security_config.tls_cert, private_key=security_config.tls_key)
