import dataclasses
import functools
import logging
import sys
import threading
from collections import Counter
from inspect import signature
from typing import Any, Callable, Iterable, Optional, TypeVar, Union

from scaler.client.agent.bridge import ClientAgentBridge, check_browser_runtime, create_default_bridge
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.future import ScalerFuture
from scaler.client.object_buffer import ObjectBuffer
from scaler.client.object_reference import ObjectReference
from scaler.client.serializer.default import DefaultSerializer
from scaler.client.serializer.mixins import Serializer
from scaler.config.common.security import SecurityConfig
from scaler.config.defaults import DEFAULT_CLIENT_TIMEOUT_SECONDS, DEFAULT_HEARTBEAT_INTERVAL_SECONDS
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import NetworkBackend, SyncConnector
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.ymq import YMQException
from scaler.protocol.capnp import ClientDisconnect, ClientShutdownResponse, GraphTask, Task
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.exceptions import ClientQuitException, MissingObjects
from scaler.utility.graph.optimization import cull_graph
from scaler.utility.graph.topological_sorter import TopologicalSorter
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.metadata.task_flags import TaskFlags, retrieve_task_flags_from_task

if sys.platform != "emscripten":
    from scaler.worker.agent.processor.processor import Processor
else:
    Processor = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


@dataclasses.dataclass
class _CallNode:
    func: Callable
    args: tuple[str, ...]

    def __post_init__(self):
        if not callable(self.func):
            raise TypeError(f"the first item of the tuple must be function, get {self.func}")

        if not isinstance(self.args, tuple):
            raise TypeError(f"arguments must be tuple, get {self.args}")

        for arg in self.args:
            if not isinstance(arg, str):
                raise TypeError(f"argument `{arg}` must be a string and the string has to be in the graph")


class Client:
    def __init__(
        self,
        address: Optional[str] = None,
        profiling: bool = False,
        timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
        heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        serializer: Serializer = DefaultSerializer(),
        stream_output: bool = False,
        object_storage_address: Optional[str] = None,
        security_config: Optional[SecurityConfig] = None,
    ):
        """
        The Scaler Client used to send tasks to a scheduler.

        :param address: Address of Scheduler to submit work to. If None, will attempt to auto-detect
                       when running inside a worker context.
        :type address: Optional[str]
        :param profiling: If True, the returned futures will have the `task_duration()` property enabled.
        :type profiling: bool
        :param timeout_seconds: Seconds until heartbeat times out
        :type timeout_seconds: int
        :param heartbeat_interval_seconds: Frequency of heartbeat to scheduler in seconds
        :type heartbeat_interval_seconds: int
        :param stream_output: If True, stdout/stderr will be streamed to client during task execution
        :type stream_output: bool
        :param object_storage_address: Override object storage address (e.g., for Docker/Kubernetes port mapping).
                                       If None, will use address received from scheduler.
        :type object_storage_address: Optional[str]
        """
        self.__initialize__(
            address,
            profiling,
            timeout_seconds,
            heartbeat_interval_seconds,
            serializer,
            stream_output,
            object_storage_address,
            security_config,
        )

    def __initialize__(
        self,
        address: Optional[str],
        profiling: bool,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer = DefaultSerializer(),
        stream_output: bool = False,
        object_storage_address: Optional[str] = None,
        security_config: Optional[SecurityConfig] = None,
    ):
        check_browser_runtime()

        self._future_manager: Optional[ClientFutureManager] = None
        self._bridge: Optional[ClientAgentBridge] = None
        self._connector_agent: Optional[SyncConnector] = None

        self._serializer = serializer

        self._profiling = profiling
        self._stream_output = stream_output
        self._identity = ClientID.generate_client_id()
        self._security_config = security_config

        self._backend: NetworkBackend = get_network_backend_from_env()

        self._scheduler_address = self.__resolve_scheduler_address(address)
        self._timeout_seconds = timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

        self._stop_event = threading.Event()

        self._future_manager = ClientFutureManager(self._serializer)
        self._bridge = create_default_bridge(
            identity=self._identity,
            scheduler_address=self._scheduler_address,
            network_backend=self._backend,
            future_manager=self._future_manager,
            stop_event=self._stop_event,
            timeout_seconds=self._timeout_seconds,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            serializer=self._serializer,
            object_storage_address=object_storage_address,
            security_config=self._security_config,
        )
        self._bridge.start()

        self._connector_agent = self._bridge.connector

        # The object storage server is accessed by the client agent, on its event loop.
        self._object_buffer = ObjectBuffer(self._identity, self._serializer, self._connector_agent, self._bridge)
        self._future_factory = functools.partial(
            ScalerFuture,
            serializer=self._serializer,
            connector_agent=self._connector_agent,
            object_buffer=self._object_buffer,
        )

    @property
    def identity(self) -> ClientID:
        return self._identity

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __getstate__(self) -> dict:
        """
        Serializes the client object's state.

        Client serialization is useful when a client reference is used within a remote task:


        .. code:: python

            client = Client(...)

            def fibonacci(client: Client, n: int):
                if n == 0:
                    return 0
                elif n == 1:
                    return 1
                else:
                    a = client.submit(fibonacci, n - 1)
                    b = client.submit(fibonacci, n - 2)
                    return a.result() + b.result()

            print(client.submit(fibonacci, client, 7).result())


        When serializing the client, only saves the address parameters. When deserialized, a new client object
        connecting to the same scheduler and remote logger will be instantiated.
        """

        return {
            "address": repr(self._scheduler_address),
            "profiling": self._profiling,
            "stream_output": self._stream_output,
            "timeout_seconds": self._timeout_seconds,
            "heartbeat_interval_seconds": self._heartbeat_interval_seconds,
        }

    def __setstate__(self, state: dict) -> None:
        # TODO: fix copy the serializer
        self.__initialize__(
            address=state["address"],
            profiling=state["profiling"],
            stream_output=state["stream_output"],
            timeout_seconds=state["timeout_seconds"],
            heartbeat_interval_seconds=state["heartbeat_interval_seconds"],
        )

    def submit(self, fn: Callable, *args, **kwargs) -> ScalerFuture:
        """
        Submit a single task (function with arguments) to the scheduler, and return a future.

        See `submit_verbose()` for additional parameters.

        :param fn: function to be executed remotely
        :type fn: Callable
        :param args: positional arguments will be passed to function
        :param kwargs: keyword arguments will be passed to function
        :return: future of the submitted task
        :rtype: ScalerFuture
        """

        return self.submit_verbose(fn, args, kwargs)

    def submit_verbose(
        self,
        fn: Callable,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        capabilities: Optional[dict[str, int]] = None,
        reserialize: bool = False,
    ) -> ScalerFuture:
        """
        Submit a single task (function with arguments) to the scheduler, and return a future. Possibly route the task to
        specific workers.

        :param fn: function to be executed remotely
        :type fn: Callable
        :param args: positional arguments will be passed to function
        :param kwargs: keyword arguments will be passed to function
        :param capabilities: capabilities used for routing the tasks, e.g. `{"gpu": 2, "memory": 1_000_000_000}`.
        :type capabilities: Optional[dict[str, int]]
        :param reserialize: If True, re-serialize this task's argument objects instead of reusing a cached snapshot,
                            and refresh the cache. Because identical content uploads once, the re-serialized bytes are
                            re-uploaded only if the content actually changed, so this is cheap when nothing changed. Use
                            this after mutating an argument in place. Defaults to False, where a reused object is
                            serialized and uploaded once.
        :type reserialize: bool
        :return: future of the submitted task
        :rtype: ScalerFuture
        """

        self.__assert_client_not_stopped()

        function_object_id = self._object_buffer.buffer_send_function(fn).object_id
        all_args = Client.__convert_kwargs_to_args(fn, args, kwargs)

        task, future = self.__submit(
            function_object_id, all_args, delayed=True, capabilities=capabilities, reserialize=reserialize
        )

        self._object_buffer.commit_send_objects()
        self._connector_agent.send(task)
        return future

    def map(
        self,
        fn: Callable[..., _T],
        *iterables: Iterable[Any],
        capabilities: Optional[dict[str, int]] = None,
        reserialize: bool = False,
    ) -> list[_T]:
        """
        Apply function to every item of iterables, collecting the results in a list.

        This works like Python's built-in map(), where each iterable provides one argument to the function.

        Example:
            >>> def add(x, y):
            ...     return x + y
            >>> client.map(add, [1, 2, 3], [4, 5, 6])
            [5, 7, 9]

        :param fn: function to be executed remotely
        :type fn: Callable[..., _T]
        :param iterables: one or more iterables, each providing one argument to the function
        :type iterables: Iterable[Any]
        :param capabilities: capabilities used for routing the tasks, e.g. `{"gpu": 2, "memory": 1_000_000_000}`.
        :type capabilities: Optional[dict[str, int]]
        :param reserialize: If True, re-serialize the argument objects instead of reusing a cached snapshot, and refresh
                            the cache. Because identical content uploads once, the re-serialized bytes are re-uploaded
                            only if the content actually changed, so this is cheap when nothing changed. Use this after
                            mutating an argument in place. Defaults to False, where a reused object is serialized and
                            uploaded once.
        :type reserialize: bool
        :return: list of results, where each result is the return value of fn
        :rtype: list[_T]
        """
        if len(iterables) == 0:
            raise TypeError("map() requires at least one iterable")

        return self.starmap(fn, zip(*iterables), capabilities=capabilities, reserialize=reserialize)

    def starmap(
        self,
        fn: Callable[..., _T],
        iterable: Iterable[Iterable[Any]],
        capabilities: Optional[dict[str, int]] = None,
        reserialize: bool = False,
    ) -> list[_T]:
        """
        Apply function to every item of iterable, where each item is an iterable of arguments to unpack.

        This works like Python's itertools.starmap() and multiprocessing.Pool.starmap().

        Example:
            >>> def add(x, y):
            ...     return x + y
            >>> client.starmap(add, [(1, 4), (2, 5), (3, 6)])
            [5, 7, 9]

        :param fn: function to be executed remotely
        :type fn: Callable[..., _T]
        :param iterable: iterable of argument iterables to unpack and pass to the function
        :type iterable: Iterable[Iterable[Any]]
        :param capabilities: capabilities used for routing the tasks, e.g. `{"gpu": 2, "memory": 1_000_000_000}`.
        :type capabilities: Optional[dict[str, int]]
        :param reserialize: If True, re-serialize the argument objects instead of reusing a cached snapshot, and refresh
                            the cache. Because identical content uploads once, the re-serialized bytes are re-uploaded
                            only if the content actually changed, so this is cheap when nothing changed. Use this after
                            mutating an argument in place. Defaults to False, where a reused object is serialized and
                            uploaded once.
        :type reserialize: bool
        :return: list of results, where each result is the return value of fn
        :rtype: list[_T]
        """
        iterable_list = [tuple(args) for args in iterable]

        self.__assert_client_not_stopped()

        function_object_id = self._object_buffer.buffer_send_function(fn).object_id
        tasks, futures = zip(
            *[
                self.__submit(
                    function_object_id, args, delayed=False, capabilities=capabilities, reserialize=reserialize
                )
                for args in iterable_list
            ]
        )

        self._object_buffer.commit_send_objects()
        for task in tasks:
            self._connector_agent.send(task)

        try:
            results = [fut.result() for fut in futures]
        except Exception as e:
            logger.exception(f"Error occured during scaler client.starmap:\n{e}")
            self.disconnect()
            raise e

        return results

    def get(
        self,
        graph: dict[str, Union[Any, tuple[Union[Callable, str], ...]]],
        keys: list[str],
        block: bool = True,
        capabilities: Optional[dict[str, int]] = None,
        reserialize: bool = False,
    ) -> dict[str, Union[Any, ScalerFuture]]:
        """
        .. code-block:: python
           :linenos:
            graph = {
                "a": 1,
                "b": 2,
                "c": (inc, "a"),
                "d": (inc, "b"),
                "e": (add, "c", "d")
            }

        :param graph: dictionary presentation of task graphs
        :type graph: dict[str, Union[Any, tuple[Union[Callable, Any]]
        :param keys: list of keys want to get results from computed graph
        :type keys: list[str]
        :param block: if True, it will directly return a dictionary that maps from keys to results
        :return: dictionary of mapping keys to futures, or map to results if block=True is specified
        :param capabilities: capabilities used for routing the tasks, e.g. `{"gpu": 2, "memory": 1_000_000_000}`.
        :type capabilities: Optional[dict[str, int]]
        :param reserialize: If True, re-serialize the graph's data nodes instead of reusing a cached snapshot, and
                            refresh the cache. Because identical content uploads once, the re-serialized bytes are
                            re-uploaded only if the content actually changed, so this is cheap when nothing changed. Use
                            this after mutating a data node in place. Defaults to False, where a reused object is
                            serialized and uploaded once.
        :type reserialize: bool
        :rtype: dict[ScalerFuture]
        """

        self.__assert_client_not_stopped()

        capabilities = capabilities or {}

        graph = cull_graph(graph, keys)

        node_name_to_argument, call_graph = self.__split_data_and_graph(graph, reserialize=reserialize)
        self.__check_graph(node_name_to_argument, call_graph, keys)

        graph_task, compute_futures, finished_futures = self.__construct_graph(
            node_name_to_argument, call_graph, keys, block, capabilities
        )

        # future needs to be added first to avoid having task completes too early
        self._future_manager.add_future(
            self._future_factory(
                task=Task(
                    taskId=graph_task.taskId,
                    source=self._identity,
                    metadata=b"",
                    funcObjectId=b"",
                    functionArgs=[],
                    capabilities=dict_to_capabilities(capabilities),
                ),
                is_delayed=not block,
                group_task_id=graph_task.taskId,
            )
        )
        for future in compute_futures.values():
            self._future_manager.add_future(future)

        self._object_buffer.commit_send_objects()
        self._connector_agent.send(graph_task)

        # preserve the future insertion order based on inputted keys
        futures = {}
        for key in keys:
            if key in compute_futures:
                futures[key] = compute_futures[key]
            else:
                futures[key] = finished_futures[key]

        if not block:
            # just return futures
            return futures

        try:
            results = {k: v.result() for k, v in futures.items()}
        except Exception as e:
            logger.exception(f"error happened when do scaler client.get:\n{e}")
            self.disconnect()
            raise e

        return results

    def send_object(self, obj: Any, name: Optional[str] = None) -> ObjectReference:
        """
        send object to scheduler, this can be used to cache very large data to scheduler, and reuse it in multiple
        tasks

        :param obj: object to send, it will be serialized and send to scheduler
        :type obj: Any
        :param name: give a name to the cached argument
        :type name: Optional[str]
        :return: object reference
        :rtype ObjectReference
        """

        self.__assert_client_not_stopped()

        # dedup=False: send_object() returns to the user before the next commit,
        # so a kept dedup entry could serve a later submit() a stale snapshot.
        cache = self._object_buffer.buffer_send_object(obj, name, reserialize=False, dedup=False)
        return ObjectReference(cache.object_name, len(cache.object_payload), cache.object_id)

    def clear(self):
        """
        clear all resources used by the client, this will cancel all running futures and invalidate all existing object
        references
        """

        # It's important to be ensure that all running futures are cancelled/finished before clearing object, or else we
        # might end up with tasks indefinitely waiting on no longer existing objects.
        self._future_manager.cancel_all_futures()

        self._object_buffer.clear()

    def disconnect(self):
        """
        disconnect from connected scheduler, this will not shut down the scheduler
        """

        # Handle case where the object exists but __initialize__ never ran (e.g. built by __new__
        # during a failed unpickling), so none of the attributes below exist yet.
        if not hasattr(self, "_stop_event"):
            return

        # Initialization was interrupted before the connector was created (e.g. Ctrl-C while waiting
        # for an unreachable scheduler); there is nothing to disconnect. The agent, if it was
        # started, is a daemon thread that stops on its own.
        if self._connector_agent is None:
            return

        if self._stop_event.is_set():
            self.__destroy()
            return

        logger.info(f"ScalerClient: disconnect from {self._scheduler_address!r}")

        self._future_manager.cancel_all_futures()

        try:
            self._connector_agent.send(ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.disconnect))
        except YMQException:
            # Race: between the _stop_event.is_set() check above and this send, the agent
            # thread can observe the scheduler going away, exit __get_loops, and destroy its
            # internal binder. The send then fails with ConnectorSocketClosedByRemoteEnd. The
            # agent has already torn itself down, so just fall through to __destroy().
            pass

        self.__destroy()

    def __receive_shutdown_response(self):
        message: Optional[ClientShutdownResponse] = None
        try:
            while not isinstance(message, ClientShutdownResponse):
                message = self._connector_agent.receive()
        except YMQException:
            # The scheduler may close its end of the socket immediately after raising
            # ClientShutdownException, before YMQ has flushed the ClientShutdownResponse on the wire.
            # The client agent then sees the remote-end close and tears down its connectors, which
            # surfaces here as a YMQException. Treat this as a successful shutdown -- the scheduler
            # acknowledged the request before quitting.
            return

        if not message.accepted:
            raise ValueError("Scheduler is in protected mode. Can't shutdown")

    def shutdown(self):
        """
        shutdown all workers that connected to the scheduler this client connects to, it will cancel all other
        clients' ongoing tasks, please be aware shutdown might not success if scheduler is configured as protected mode,
        then it cannot shut down scheduler and the workers
        """

        if not self._bridge.is_alive():
            self.__destroy()
            return

        logger.info(f"ScalerClient: request shutdown for {self._scheduler_address!r}")

        self._future_manager.cancel_all_futures()

        self._connector_agent.send(ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown))
        try:
            self.__receive_shutdown_response()
        finally:
            self.__destroy()

    def __submit(
        self,
        function_object_id: ObjectID,
        args: tuple[Any, ...],
        delayed: bool,
        capabilities: Optional[dict[str, int]],
        reserialize: bool,
    ) -> tuple[Task, ScalerFuture]:
        task_id = TaskID.generate_task_id()

        capabilities = capabilities or {}

        function_args: list[Union[ObjectID, TaskID]] = []
        for arg in args:
            if isinstance(arg, ObjectReference):
                if not self._object_buffer.is_valid_object_id(arg.object_id):
                    raise MissingObjects(f"unknown object: {arg.object_id!r}.")

                function_args.append(arg.object_id)
            else:
                function_args.append(
                    self._object_buffer.buffer_send_object(arg, None, reserialize=reserialize, dedup=True).object_id
                )

        task_flags_bytes = self.__get_task_flags().serialize()

        task = Task(
            taskId=task_id,
            source=self._identity,
            metadata=task_flags_bytes,
            funcObjectId=function_object_id,
            functionArgs=[
                Task.Argument(type=Task.Argument.ArgumentType.objectID, data=argument) for argument in function_args
            ],
            capabilities=dict_to_capabilities(capabilities),
        )

        future = self._future_factory(task=task, is_delayed=delayed, group_task_id=None)
        self._future_manager.add_future(future)
        return task, future

    @staticmethod
    def __convert_kwargs_to_args(fn: Callable, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
        all_params = [p for p in signature(fn).parameters.values()]

        params = [p for p in all_params if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}]

        if len(args) >= len(params):
            return args

        number_of_required = len([p for p in params if p.default is p.empty])

        args_list = list(args)
        kwargs = kwargs.copy()
        kwargs.update({p.name: p.default for p in all_params if p.kind == p.KEYWORD_ONLY if p.default != p.empty})

        for p in params[len(args_list) : number_of_required]:
            try:
                args_list.append(kwargs.pop(p.name))
            except KeyError:
                missing = tuple(p.name for p in params[len(args_list) : number_of_required])
                raise TypeError(f"{fn} missing {len(missing)} arguments: {missing}")

        for p in params[len(args_list) :]:
            args_list.append(kwargs.pop(p.name, p.default))

        return tuple(args_list)

    def __split_data_and_graph(
        self, graph: dict[str, Union[Any, tuple[Union[Callable, str], ...]]], reserialize: bool
    ) -> tuple[dict[str, tuple[ObjectID, Any]], dict[str, _CallNode]]:
        call_graph = {}
        node_name_to_argument: dict[str, tuple[ObjectID, Union[Any, tuple[Union[Callable, Any], ...]]]] = dict()

        for node_name, node in graph.items():
            if isinstance(node, tuple) and len(node) > 0 and callable(node[0]):
                call_graph[node_name] = _CallNode(func=node[0], args=node[1:])  # type: ignore[arg-type]
                continue

            if isinstance(node, ObjectReference):
                object_id = node.object_id
            else:
                object_id = self._object_buffer.buffer_send_object(
                    node, name=node_name, reserialize=reserialize, dedup=True
                ).object_id

            node_name_to_argument[node_name] = (object_id, node)

        return node_name_to_argument, call_graph

    @staticmethod
    def __check_graph(
        node_to_argument: dict[str, tuple[ObjectID, Any]], call_graph: dict[str, _CallNode], keys: list[str]
    ):
        duplicate_keys = [key for key, count in dict(Counter(keys)).items() if count > 1]
        if duplicate_keys:
            raise KeyError(f"duplicate key detected in argument keys: {duplicate_keys}")

        # sanity check graph
        for key in keys:
            if key not in call_graph and key not in node_to_argument:
                raise KeyError(f"key {key} has to be in graph")

        sorter: TopologicalSorter[str] = TopologicalSorter()
        for node_name, node in call_graph.items():
            for arg in node.args:
                if arg not in node_to_argument and arg not in call_graph:
                    raise KeyError(f"argument {arg} in node '{node_name}': {node} is not defined in graph")

            sorter.add(node_name, *node.args)

        # check cyclic dependencies
        sorter.prepare()

    def __construct_graph(
        self,
        node_name_to_arguments: dict[str, tuple[ObjectID, Any]],
        call_graph: dict[str, _CallNode],
        keys: list[str],
        block: bool,
        capabilities: dict[str, int],
    ) -> tuple[GraphTask, dict[str, ScalerFuture], dict[str, ScalerFuture]]:
        graph_task_id = TaskID.generate_task_id()

        node_name_to_task_id = {node_name: TaskID.generate_task_id() for node_name in call_graph.keys()}

        task_flags_bytes = self.__get_task_flags().serialize()

        task_id_to_tasks = dict()

        for node_name, node in call_graph.items():
            task_id = node_name_to_task_id[node_name]
            function_cache = self._object_buffer.buffer_send_function(node.func)

            arguments: list[Union[TaskID, ObjectID]] = []
            for arg in node.args:
                assert arg in call_graph or arg in node_name_to_arguments

                if arg in call_graph:
                    arguments.append(TaskID(node_name_to_task_id[arg]))
                elif arg in node_name_to_arguments:
                    argument, _ = node_name_to_arguments[arg]
                    arguments.append(argument)
                else:
                    raise ValueError("Not possible")

            task_id_to_tasks[task_id] = Task(
                taskId=task_id,
                source=self._identity,
                metadata=task_flags_bytes,
                funcObjectId=function_cache.object_id,
                functionArgs=[
                    Task.Argument(
                        type=(
                            Task.Argument.ArgumentType.task
                            if isinstance(argument, TaskID)
                            else Task.Argument.ArgumentType.objectID
                        ),
                        data=argument,
                    )
                    for argument in arguments
                ],
                capabilities=dict_to_capabilities(capabilities),
            )

        result_task_ids = [node_name_to_task_id[key] for key in keys if key in call_graph]
        graph_task = GraphTask(
            taskId=graph_task_id, source=self._identity, targets=result_task_ids, graph=list(task_id_to_tasks.values())
        )

        compute_futures = {}
        ready_futures = {}
        for key in keys:
            if key in call_graph:
                compute_futures[key] = self._future_factory(
                    task=task_id_to_tasks[node_name_to_task_id[key]], is_delayed=not block, group_task_id=graph_task_id
                )

            elif key in node_name_to_arguments:
                argument, data = node_name_to_arguments[key]
                future: ScalerFuture = self._future_factory(
                    task=Task(
                        taskId=TaskID.generate_task_id(),
                        source=self._identity,
                        metadata=b"",
                        funcObjectId=b"",
                        functionArgs=[],
                        capabilities=[],
                    ),
                    is_delayed=False,
                    group_task_id=graph_task_id,
                )
                future.set_result(data, ProfileResult())
                ready_futures[key] = future

            else:
                raise ValueError(f"cannot find {key=} in graph")

        return graph_task, compute_futures, ready_futures

    def __get_task_flags(self) -> TaskFlags:
        parent_task_priority = self.__get_parent_task_priority()

        if parent_task_priority is not None:
            task_priority = parent_task_priority + 1
        else:
            task_priority = 0

        return TaskFlags(profiling=self._profiling, priority=task_priority, stream_output=self._stream_output)

    def __assert_client_not_stopped(self):
        if self._stop_event.is_set():
            raise ClientQuitException("client is already stopped.")

    def __destroy(self):
        if self._bridge is not None and not sys.is_finalizing():
            self._bridge.join()

        if self._connector_agent is not None:
            self._connector_agent.destroy()

    @staticmethod
    def __get_parent_task_priority() -> Optional[int]:
        """If the client is running inside a Scaler processor, returns the priority of the associated task."""
        if Processor is None:
            return None

        current_processor = Processor.get_current_processor()

        if current_processor is None:
            return None

        current_task = current_processor.current_task()
        assert current_task is not None

        return retrieve_task_flags_from_task(current_task).priority

    def __resolve_scheduler_address(self, address: Optional[str]) -> AddressConfig:
        """Resolve the scheduler address based on the provided address and worker context."""
        # Provided address always takes precedence
        if address is not None:
            return AddressConfig.from_string(address)

        # No address provided, check if we're running inside a worker context
        current_processor = Processor.get_current_processor() if Processor is not None else None
        if current_processor is None:
            raise ValueError(
                "No scheduler address provided and not running inside a worker context. "
                "Please provide a scheduler address when creating the Client outside of a worker."
            )

        # Return the scheduler address from the current processor
        return current_processor.scheduler_address()
