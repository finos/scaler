import asyncio
import base64
import datetime
import functools
import gzip
import logging
import re
from concurrent.futures import Future
from typing import Any, Dict, List, Optional, Set, Tuple

import cloudpickle
import oci

from scaler.config.types.oci_auth_type import OCIAuthType
from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import TaskID
from scaler.worker_manager.proxy.mixins import ExecutionBackend, TaskDeserializer, TaskInputLoader
from scaler.worker_manager.proxy.oci.container_instance_lifecycle_state import ContainerInstanceLifecycleState

logger = logging.getLogger(__name__)

_INSTANCE_STATE_RUNNING = {ContainerInstanceLifecycleState.CREATING, ContainerInstanceLifecycleState.ACTIVE}

_KEY_INPUTS = "inputs"
_KEY_RESULTS = "results"

_MAX_INLINE_PAYLOAD_BYTES = 28 * 1024
_POLL_INTERVAL_SECONDS = 2.0
_MAX_UNEXPECTED_STATE_COUNT = 5


class OCIExecutionBackend(TaskInputLoader, ExecutionBackend):
    _loader: TaskDeserializer

    def __init__(
        self,
        compartment_id: str,
        availability_domain: str,
        subnet_id: str,
        container_image: str,
        oci_region: str,
        object_storage_namespace: str,
        object_storage_bucket: str,
        object_storage_prefix: str = "scaler-tasks",
        instance_shape: str = "CI.Standard.E4.Flex",
        instance_ocpus: float = 1.0,
        instance_memory_gb: float = 6.0,
        job_timeout_seconds: int = 3600,
        oci_profile: str = "DEFAULT",
        auth_type: OCIAuthType = OCIAuthType.config_file,
    ) -> None:
        self._compartment_id = compartment_id
        self._availability_domain = availability_domain
        self._subnet_id = subnet_id
        self._container_image = container_image
        self._oci_region = oci_region
        self._object_storage_namespace = object_storage_namespace
        self._object_storage_bucket = object_storage_bucket
        self._object_storage_prefix = object_storage_prefix
        self._instance_shape = instance_shape
        self._instance_ocpus = instance_ocpus
        self._instance_memory_gb = instance_memory_gb
        self._job_timeout_seconds = job_timeout_seconds
        self._oci_profile = oci_profile
        self._auth_type = auth_type

        self._task_id_to_instance_id: Dict[TaskID, str] = {}
        self._task_id_to_input_key: Dict[TaskID, str] = {}
        self._monitor_tasks: Set[asyncio.Task] = set()

        self._container_instances_client: Any = None
        self._object_storage_client: Any = None
        self._log_search_client: Any = None

    def register(self, load_task_inputs: TaskDeserializer) -> None:
        self._loader = load_task_inputs
        self._initialize_oci_clients()

    async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]:
        return await self._loader(task)

    def _build_oci_signer(self) -> Tuple[Dict[str, Any], Any]:
        if self._auth_type == OCIAuthType.instance_principal:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return {"region": self._oci_region}, signer

        config = oci.config.from_file(profile_name=self._oci_profile)
        config["region"] = self._oci_region
        return config, None

    def _initialize_oci_clients(self) -> None:
        config, signer = self._build_oci_signer()
        kwargs: Dict[str, Any] = {"config": config}
        if signer is not None:
            kwargs["signer"] = signer

        self._container_instances_client = oci.container_instances.ContainerInstanceClient(**kwargs)
        self._object_storage_client = oci.object_storage.ObjectStorageClient(**kwargs)
        self._log_search_client = oci.loggingsearch.LogSearchClient(**kwargs)

        logger.info(
            f"OCI execution backend initialized: auth={self._auth_type}, "
            f"region={self._oci_region}, "
            f"compartment={self._compartment_id[:20]}..., "
            f"bucket={self._object_storage_bucket}"
        )

    async def execute(self, task: Task) -> asyncio.Future:
        function, arg_objects = await self.load_task_inputs(task)

        future: Future = Future()
        future.set_running_or_notify_cancel()

        try:
            instance_id, input_key = await self._create_container_instance(task, function, arg_objects)
            self._task_id_to_instance_id[task.taskId] = instance_id
            if input_key:
                self._task_id_to_input_key[task.taskId] = input_key
            logger.info(f"Task {task.taskId.hex()[:8]} submitted as Container Instance {instance_id[-20:]}")

            monitor_task = asyncio.create_task(self._monitor_container_instance(instance_id, future, task.taskId))
            self._monitor_tasks.add(monitor_task)
            monitor_task.add_done_callback(self._monitor_tasks.discard)
        except Exception as exc:
            logger.exception(f"Failed to submit task {task.taskId.hex()[:8]}: {exc}")
            future.set_exception(exc)

        return asyncio.wrap_future(future)

    async def on_cancel(self, task_cancel: TaskCancel) -> None:
        instance_id = self._task_id_to_instance_id.pop(task_cancel.taskId, None)
        if instance_id is not None:
            await self._delete_container_instance(instance_id)
        input_key = self._task_id_to_input_key.pop(task_cancel.taskId, None)
        if input_key:
            await self._delete_object_storage_object(input_key)

    def on_cleanup(self, task_id: TaskID) -> None:
        self._task_id_to_instance_id.pop(task_id, None)
        self._task_id_to_input_key.pop(task_id, None)

    async def routine(self) -> None:
        pass

    async def _create_container_instance(
        self, task: Task, function: Any, arguments: List[Any]
    ) -> Tuple[str, Optional[str]]:
        task_id_hex = task.taskId.hex()
        func_name = getattr(function, "__name__", "unknown")

        task_data = {"task_id": task_id_hex, "function": function, "arguments": arguments}
        payload = cloudpickle.dumps(task_data)
        payload_size = len(payload)

        compressed = False
        if payload_size > 4 * 1024:
            payload = gzip.compress(payload)
            compressed = True
            logger.debug(f"Compressed payload: {payload_size} -> {len(payload)} bytes")

        safe_func_name = re.sub(r"[^a-zA-Z0-9_-]", "_", func_name)[:50]
        display_name = f"scaler-{safe_func_name}-{task_id_hex[:12]}"

        input_key: Optional[str] = None
        if len(payload) <= _MAX_INLINE_PAYLOAD_BYTES:
            encoded_payload = base64.b64encode(payload).decode("ascii")
            object_key = "none"
        else:
            suffix = ".pkl.gz" if compressed else ".pkl"
            object_key = f"{self._object_storage_prefix}/{_KEY_INPUTS}/{task_id_hex}{suffix}"
            input_key = object_key
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._object_storage_client.put_object,
                    namespace_name=self._object_storage_namespace,
                    bucket_name=self._object_storage_bucket,
                    object_name=object_key,
                    put_object_body=payload,
                ),
            )
            encoded_payload = ""

        env_vars = {
            "TASK_ID": task_id_hex,
            "OCI_NAMESPACE": self._object_storage_namespace,
            "OCI_BUCKET": self._object_storage_bucket,
            "OCI_PREFIX": self._object_storage_prefix,
            "OCI_OBJECT_KEY": object_key,
            "PAYLOAD_B64": encoded_payload,
            "COMPRESSED": "1" if compressed else "0",
        }

        create_details = oci.container_instances.models.CreateContainerInstanceDetails(
            compartment_id=self._compartment_id,
            availability_domain=self._availability_domain,
            shape=self._instance_shape,
            shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                ocpus=self._instance_ocpus, memory_in_gbs=self._instance_memory_gb
            ),
            containers=[
                oci.container_instances.models.CreateContainerDetails(
                    image_url=self._container_image, display_name=display_name, environment_variables=env_vars
                )
            ],
            vnics=[oci.container_instances.models.CreateContainerVnicDetails(subnet_id=self._subnet_id)],
            display_name=display_name,
            container_restart_policy="NEVER",
        )

        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            functools.partial(
                self._container_instances_client.create_container_instance,
                create_container_instance_details=create_details,
            ),
        )
        return response.data.id, input_key

    async def _monitor_container_instance(self, instance_id: str, future: Future, task_id: TaskID) -> None:
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        unexpected_state_count = 0

        while True:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

            elapsed = loop.time() - start_time
            if elapsed > self._job_timeout_seconds:
                future.set_exception(
                    TimeoutError(f"Container Instance {instance_id[-20:]} timed out after {self._job_timeout_seconds}s")
                )
                await self._delete_container_instance(instance_id)
                return

            try:
                response = await loop.run_in_executor(
                    None,
                    functools.partial(
                        self._container_instances_client.get_container_instance, container_instance_id=instance_id
                    ),
                )
                state = response.data.lifecycle_state

                if state == ContainerInstanceLifecycleState.INACTIVE:
                    task_id_hex = task_id.hex()
                    result_key = f"{self._object_storage_prefix}/{_KEY_RESULTS}/{task_id_hex}.pkl"

                    try:
                        obj_response = await loop.run_in_executor(
                            None,
                            functools.partial(
                                self._object_storage_client.get_object,
                                namespace_name=self._object_storage_namespace,
                                bucket_name=self._object_storage_bucket,
                                object_name=result_key,
                            ),
                        )
                        result_bytes = obj_response.data.content

                        if len(result_bytes) >= 2 and result_bytes[:2] == b"\x1f\x8b":
                            result_bytes = gzip.decompress(result_bytes)

                        result = cloudpickle.loads(result_bytes)
                        if isinstance(result, dict) and result.get("_scaler_container_error"):
                            future.set_exception(
                                RuntimeError(
                                    f"Task failed in container: {result.get('error', 'unknown')}\n"
                                    f"{result.get('traceback', '')}"
                                )
                            )
                        else:
                            future.set_result(result)

                        try:
                            await loop.run_in_executor(
                                None,
                                functools.partial(
                                    self._object_storage_client.delete_object,
                                    namespace_name=self._object_storage_namespace,
                                    bucket_name=self._object_storage_bucket,
                                    object_name=result_key,
                                ),
                            )
                        except Exception as cleanup_exc:
                            logger.warning(f"Failed to clean up result object {result_key}: {cleanup_exc}")

                    except Exception as fetch_exc:
                        future.set_exception(RuntimeError(f"Failed to fetch result from Object Storage: {fetch_exc}"))

                    await self._delete_container_instance(instance_id)
                    return

                elif state == ContainerInstanceLifecycleState.FAILED:
                    reason = getattr(response.data, "lifecycle_details", "unknown failure")
                    logs = await self._fetch_instance_logs(instance_id)
                    error_msg = f"Container Instance failed: {reason}"
                    if logs:
                        error_msg += f"\n\n{logs}"
                    future.set_exception(RuntimeError(error_msg))
                    await self._delete_container_instance(instance_id)
                    return

                elif state in _INSTANCE_STATE_RUNNING:
                    continue
                else:
                    unexpected_state_count += 1
                    logger.warning(
                        f"Unexpected Container Instance state: {state} for {instance_id[-20:]} "
                        f"({unexpected_state_count}/{_MAX_UNEXPECTED_STATE_COUNT})"
                    )
                    if unexpected_state_count >= _MAX_UNEXPECTED_STATE_COUNT:
                        future.set_exception(
                            RuntimeError(f"Container Instance {instance_id[-20:]} stuck in unexpected state: {state}")
                        )
                        await self._delete_container_instance(instance_id)
                        return

            except oci.exceptions.ServiceError as svc_exc:
                if svc_exc.status == 404:
                    future.set_exception(
                        RuntimeError(f"Container Instance {instance_id[-20:]} not found (deleted externally?)")
                    )
                    return
                logger.exception(f"OCI service error polling Container Instance {instance_id[-20:]}: {svc_exc}")

            except Exception as poll_exc:
                logger.exception(f"Error polling Container Instance {instance_id[-20:]}: {poll_exc}")

    async def _delete_container_instance(self, instance_id: str) -> None:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._container_instances_client.delete_container_instance, container_instance_id=instance_id
                ),
            )
            logger.info(f"Deleted Container Instance {instance_id[-20:]}")
        except Exception as exc:
            logger.warning(f"Failed to delete Container Instance {instance_id[-20:]}: {exc}")

    async def _delete_object_storage_object(self, object_key: str) -> None:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                functools.partial(
                    self._object_storage_client.delete_object,
                    namespace_name=self._object_storage_namespace,
                    bucket_name=self._object_storage_bucket,
                    object_name=object_key,
                ),
            )
            logger.info(f"Deleted input object {object_key}")
        except Exception as exc:
            logger.warning(f"Failed to delete input object {object_key}: {exc}")

    async def _fetch_instance_logs(self, instance_id: str) -> str:
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            time_start = now - datetime.timedelta(hours=2)
            search_details = oci.loggingsearch.models.SearchLogsDetails(
                time_start=time_start,
                time_end=now,
                search_query=(
                    f'search "{self._compartment_id}" | '
                    f'where subject = "{instance_id}" | '
                    f"sort by datetime desc | limit 100"
                ),
                is_return_field_info=False,
            )

            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None, functools.partial(self._log_search_client.search_logs, search_logs_details=search_details)
            )
            results = response.data.results or []

            if not results:
                return "(No log records found)"

            lines = [str(r.data) for r in results if r.data]
            return "Container Instance logs:\n" + "\n".join(lines)

        except Exception as exc:
            logger.warning(f"Failed to fetch logs for instance {instance_id[-20:]}: {exc}")
            return f"(Failed to fetch logs: {exc})"
