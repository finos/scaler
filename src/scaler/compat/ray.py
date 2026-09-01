"""
This module provides a compatibility layer for Scaler that mimics the Ray interface.
It allows users familiar with Ray's API to interact with Scaler in a similar fashion,
including remote function execution, object referencing, and waiting for task completion.
"""

import concurrent.futures
import inspect
import sys
from typing import Any, Callable, Generic, Iterator, Optional, TypeVar, Union, cast
from unittest.mock import Mock, patch

import psutil

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

from scaler.client.client import Client
from scaler.client.future import ScalerFuture
from scaler.client.object_reference import ObjectReference
from scaler.client.serializer.default import DefaultSerializer
from scaler.client.serializer.mixins import Serializer
from scaler.cluster.combo import SchedulerClusterCombo
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_PATHS,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.scheduler import PolicyConfig


def _not_implemented(*args, **kwargs) -> None:
    raise NotImplementedError


def _no_op(*_args, **_kwargs) -> None:
    pass


class NotImplementedMock(Mock):
    def __getattr__(self, _name):
        raise NotImplementedError("this module is not supported in Scaler compatibility layer.")


# We patch the underlying init and shutdown functions to prevent the real
# ray from being initialized or shut down, which can cause issues with our
# compatibility layer, especially during atexit.
patch("ray._private.worker.init", new=_no_op).start()
patch("ray._private.worker.shutdown", new=_no_op).start()

# Prevent the import of a module that causes issues during ray shutdown
# by replacing it with a mock object. This module contains a class decorated
# with @ray.remote, which is not yet supported by the Scaler compatibility layer.

try:
    patch("ray.experimental.channel.cpu_communicator", new=Mock()).start()
except AttributeError:
    pass  # this doesn't exist on old versions of Ray

patch("ray.dag.compiled_dag_node", new=Mock()).start()


# Add no-op, mock, or not-implemented patches for various ray functions and modules
patch("ray.init", new=_no_op).start()
patch("ray.method", new=_not_implemented).start()
patch("ray.actor", new=Mock()).start()
patch("ray.runtime_context", new=NotImplementedMock()).start()
patch("ray.cross_language", new=NotImplementedMock()).start()
patch("ray.get_actor", new=_no_op).start()
patch("ray.get_gpu_ids", new=_not_implemented).start()
patch("ray.get_runtime_context", new=_not_implemented).start()
patch("ray.kill", new=_not_implemented).start()


combo: Optional[SchedulerClusterCombo] = None
client: Optional[Client] = None


def scaler_init(
    address: Optional[str] = None,
    *,
    n_workers: Optional[int] = psutil.cpu_count(),
    object_storage_address: Optional[str] = None,
    monitor_address: Optional[str] = None,
    per_worker_capabilities: Optional[dict[str, int]] = None,
    worker_io_threads: int = DEFAULT_IO_THREADS,
    scheduler_io_threads: int = DEFAULT_IO_THREADS,
    max_number_of_tasks_waiting: int = DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    client_timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
    worker_timeout_seconds: int = DEFAULT_WORKER_TIMEOUT_SECONDS,
    object_retention_seconds: int = DEFAULT_OBJECT_RETENTION_SECONDS,
    task_timeout_seconds: int = DEFAULT_TASK_TIMEOUT_SECONDS,
    death_timeout_seconds: int = DEFAULT_WORKER_DEATH_TIMEOUT,
    load_balance_seconds: int = DEFAULT_LOAD_BALANCE_SECONDS,
    load_balance_trigger_times: int = DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    garbage_collect_interval_seconds: int = DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    trim_memory_threshold_bytes: int = DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    per_worker_task_queue_size: int = DEFAULT_PER_WORKER_QUEUE_SIZE,
    hard_processor_suspend: bool = DEFAULT_HARD_PROCESSOR_SUSPEND,
    protected: bool = True,
    scaler_policy: PolicyConfig = PolicyConfig(policy_content="allocate=even_load; scaling=no"),
    event_loop: str = "builtin",
    logging_paths: tuple[str, ...] = DEFAULT_LOGGING_PATHS,
    logging_level: str = DEFAULT_LOGGING_LEVEL,
    logging_config_file: Optional[str] = None,
    # client-specific options
    profiling: bool = False,
    timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
    serializer: Serializer = DefaultSerializer(),
    stream_output: bool = False,
) -> None:
    """
    Initializes Scaler's Ray compatibility layer.

    If `address` is provided, we connect to an existing Scaler cluster.
    Otherwise, it starts a new local cluster with the specified configuration.
    Several client-specific options can also be set, and shared options are passed to both client and cluster.

    Args:
        address: The address of the Scaler scheduler to connect to.
        n_workers: The number of workers to start in the local cluster.
            Defaults to the number of CPU cores.
        **kwargs: Other Scaler cluster configuration options.
    """
    global client, combo

    if client is not None:
        raise RuntimeError("Cannot initialize scaler twice")

    if address is None:
        combo = SchedulerClusterCombo(
            n_workers=n_workers,
            object_storage_address=object_storage_address,
            monitor_address=monitor_address,
            per_worker_capabilities=per_worker_capabilities,
            worker_io_threads=worker_io_threads,
            scheduler_io_threads=scheduler_io_threads,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            client_timeout_seconds=client_timeout_seconds,
            worker_timeout_seconds=worker_timeout_seconds,
            object_retention_seconds=object_retention_seconds,
            task_timeout_seconds=task_timeout_seconds,
            death_timeout_seconds=death_timeout_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
            garbage_collect_interval_seconds=garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=trim_memory_threshold_bytes,
            per_worker_task_queue_size=per_worker_task_queue_size,
            hard_processor_suspend=hard_processor_suspend,
            protected=protected,
            scaler_policy=scaler_policy,
            event_loop=event_loop,
            logging_paths=logging_paths,
            logging_level=logging_level,
            logging_config_file=logging_config_file,
        )

        address = combo.get_address()

    client = Client(
        address=address,
        profiling=profiling,
        timeout_seconds=timeout_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        serializer=serializer,
        stream_output=stream_output,
        object_storage_address=object_storage_address,
    )


def shutdown() -> None:
    """
    Disconnects the client and shuts down the local cluster if one was created.

    Mimics the behavior of `ray.shutdown()`.
    """
    global client, combo

    if client:
        client.disconnect()
    if combo:
        combo.shutdown()

    client = None
    combo = None


patch("ray.shutdown", new=shutdown).start()


def is_initialized() -> bool:
    """Checks if the Scaler client has been initialized."""
    return client is not None


patch("ray.is_initialized", new=is_initialized).start()


def ensure_init():
    """
    This is an internal function that ensures the Scaler client is initialized, calling `init()` with
    default parameters if it is not already initialized.

    Called lazily at `.remote()` invocation time so that callers who need a custom configuration (via
    `scaler_init()`) can apply decorators at module import time and still call `scaler_init()` before
    submitting any tasks.
    """
    if not is_initialized():
        scaler_init()


T = TypeVar("T")
P = ParamSpec("P")
V = TypeVar("V")


class RayObjectReference(Generic[T]):
    """
    A wrapper around a ScalerFuture to provide an API similar to a Ray ObjectRef.

    This class allows treating results of asynchronous Scaler tasks in a way
    that is compatible with the Ray API.
    """

    _future: ScalerFuture

    # the index into the return value for num_results > 1
    _index: Optional[int]

    def __init__(self, future: ScalerFuture, index: Optional[int] = None) -> None:
        """
        Initializes the RayObjectReference with a ScalerFuture.

        Args:
            future: The ScalerFuture instance to wrap.
        """
        self._future = future
        self._index = index

    def get(self) -> T:
        """
        Retrieves the result of the future, blocking until it's available.

        Returns:
            The result of the completed future.
        """
        obj = self._future.result()

        if self._index is None:
            return obj

        try:
            return obj[self._index]
        except TypeError as e:
            raise TypeError("num_returns can only be used on a function that returns an indexable object") from e

    def cancel(self) -> None:
        """Attempts to cancel the future."""
        self._future.cancel()


def unwrap_ray_object_reference(maybe_ref: Union[T, RayObjectReference[T]]) -> T:
    """
    Helper to get the result if the input is a RayObjectReference.

    If the input is a `RayObjectReference`, its result is returned.
    Otherwise, the input is returned as is. This is used to transparently
    handle passing of object references as arguments to remote functions.

    Args:
        maybe_ref: The object to unwrap.

    Returns:
        The result of the reference or the original object.
    """
    if isinstance(maybe_ref, RayObjectReference):
        return maybe_ref.get()
    return maybe_ref


def _wrap_remote_fn(fn: Callable[P, T], client: Client) -> Callable[P, T]:
    # this function forwards the implicit client to the worker and enables nesting
    def forward_client(*args: P.args, **kwargs: P.kwargs) -> T:
        import scaler.compat.ray

        scaler.compat.ray.client = client
        return fn(*args, **kwargs)

    forward_client.__signature__ = inspect.signature(fn)  # type: ignore[attr-defined]

    return forward_client


class RayRemote(Generic[P, T]):
    """
    A wrapper for a function to make it "remote," similar to a Ray remote function.

    This class is typically instantiated by the `@ray.remote` decorator.
    """

    _fn: Callable[P, T]

    _num_returns: int

    def __init__(self, fn: Callable[P, T], num_returns: int = 1, **kwargs) -> None:
        """
        Initializes the remote function wrapper.

        Args:
            fn: The Python function to be executed remotely.
            num_returns: The number of object refs returned by a call to this remote function.
            **kwargs: This is provided for callsite compatibility. All additional keyword arguments are ignored.
        """

        self._set_options(num_returns=num_returns)
        self._fn = fn

    def _set_options(self, **kwargs) -> None:
        if "num_returns" in kwargs:
            self._set_num_returns(kwargs["num_returns"])

    def _set_num_returns(self, num_returns: Any) -> None:
        if not isinstance(num_returns, int):
            raise ValueError("num_returns must be an integer")

        if num_returns <= 0:
            raise ValueError("num_returns must be > 0")

        self._num_returns = num_returns

    def remote(self, *args: P.args, **kwargs: P.kwargs) -> Union[RayObjectReference, list[RayObjectReference]]:
        """
        Executes the wrapped function remotely.

        Args:
            *args: Positional arguments for the remote function.
            **kwargs: Keyword arguments for the remote function.

        Returns:
            A RayObjectReference that can be used to retrieve the result,
            or a list of RayObjectReferences if num_returns > 1.
        """
        ensure_init()

        # Ray supports passing object references into other remote functions
        # so we must take special care to get their values
        processed_args = [unwrap_ray_object_reference(arg) for arg in args]
        processed_kwargs = {k: unwrap_ray_object_reference(v) for k, v in kwargs.items()}

        future = client.submit(_wrap_remote_fn(self._fn, client), *processed_args, **processed_kwargs)

        if self._num_returns == 1:
            return RayObjectReference(future)

        return [RayObjectReference(future, index=i) for i in range(self._num_returns)]

    def options(self, *args, **kwargs) -> "RayRemote[P, T]":
        return RayRemote(self._fn, *args, **kwargs)


def get(ref: Union[RayObjectReference[T], list[RayObjectReference[Any]]]) -> Union[T, list[Any]]:
    """
    Retrieves the result from one or more RayObjectReferences.

    This function blocks until the results are available. Mimics `ray.get()`.

    Args:
        ref: A single RayObjectReference or a list of them.

    Returns:
        The result of the reference or a list of results.
    """
    if isinstance(ref, list):
        return [get(x) for x in ref]
    if isinstance(ref, RayObjectReference):
        return ref.get()

    raise RuntimeError(f"Unknown type [{type(ref)}] passed to ray.get()")


patch("ray.get", new=get).start()


def put(obj: Any) -> ObjectReference:
    """
    Stores an object in the Scaler object store. Mimics `ray.put()`.

    Args:
        obj: The Python object to be stored.

    Returns:
        An ObjectReference that can be used to retrieve the object.
    """
    ensure_init()
    return client.send_object(obj)


patch("ray.put", new=put).start()


def remote(*args, **kwargs) -> Union[RayRemote, Callable]:
    """
    A decorator that creates a `RayRemote` instance from a regular function.

    Mimics the behavior of `@ray.remote`. This decorator can be used with or without arguments,
    e.g., `@ray.remote` or `@ray.remote(num_cpus=1)`.

    All arguments passed to the decorator are ignored.

    Returns:
        A RayRemote instance that can be called with `.remote()`, or a decorator
        that produces a RayRemote instance.
    """

    def _decorator(fn: Callable) -> RayRemote:
        if isinstance(fn, type):  # Check if 'fn' is a class
            raise NotImplementedError(
                "Decorating classes with @ray.remote is not yet supported in Scaler compatibility layer."
            )
        return RayRemote(fn, **kwargs)

    if len(args) == 1 and callable(args[0]) and not kwargs:
        # This is the case: @ray.remote
        return _decorator(args[0])
    else:
        # This is the case: @ray.remote(...)
        return _decorator


patch("ray.remote", new=remote).start()


def cancel(ref: RayObjectReference) -> None:
    """
    Attempts to cancel the execution of a task. Mimics `ray.cancel()`.

    Args:
        ref: The RayObjectReference corresponding to the task to be canceled.
    """
    ref.cancel()


patch("ray.cancel", new=cancel).start()


class _RayUtil:
    def as_completed(self, refs: list[RayObjectReference[T]]) -> Iterator[RayObjectReference[T]]:
        """
        Returns an iterator that yields object references as they are completed.
        Mimics `ray.util.as_completed()`.
        """
        future_to_ref = {ref._future: ref for ref in refs}
        for future in concurrent.futures.as_completed(future_to_ref.keys()):
            yield future_to_ref[cast(ScalerFuture, future)]

    # python3.8 cannot handle giving real type hints to `fn`
    def map_unordered(self, fn: RayRemote, values: list[V]) -> Iterator[T]:
        """
        Applies a remote function to each value in a list and yields the results
        as they become available. Mimics `ray.util.map_unordered()`.

        The function `fn` must be a @ray.remote decorated function, with `num_returns=1`.
        """
        if not hasattr(fn, "remote") or not callable(fn.remote):
            raise TypeError("The function passed to map_unordered must be a @ray.remote function.")

        if fn._num_returns > 1:
            raise TypeError("map_unordered only supports remote functions with num_returns=1")

        refs = [cast(RayObjectReference, fn.remote(v)) for v in values]
        for ref in self.as_completed(refs):
            yield ref.get()


patch("ray.util", new=_RayUtil()).start()


def wait(
    refs: list[RayObjectReference[T]], *, num_returns: Optional[int] = 1, timeout: Optional[float] = None
) -> tuple[list[RayObjectReference[T]], list[RayObjectReference[T]]]:
    """
    Waits for a number of object references to be ready. Mimics `ray.wait()`.

    Args:
        refs: A list of RayObjectReferences to wait on.
        num_returns: The number of references to wait for. If None, waits for all.
        timeout: The maximum time in seconds to wait.

    Returns:
        A tuple containing two lists: the list of ready references and the
        list of remaining, not-ready references.
    """

    if num_returns is not None and num_returns > len(refs):
        raise ValueError("num_returns cannot be greater than the number of provided object references")

    if num_returns is not None and num_returns <= 0:
        return [], list(refs)

    future_to_ref = {ref._future: ref for ref in refs}
    done = set()

    try:
        for future in concurrent.futures.as_completed((ref._future for ref in refs), timeout=timeout):
            done.add(future_to_ref[cast(ScalerFuture, future)])

            if num_returns is not None and len(done) == num_returns:
                break
    except concurrent.futures.TimeoutError:
        pass

    return list(done), [ref for ref in refs if ref not in done]


patch("ray.wait", new=wait).start()
