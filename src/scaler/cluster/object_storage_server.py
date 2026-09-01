import logging
import multiprocessing
import time
from typing import Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import YMQNetworkBackend
from scaler.io.ymq import YMQException
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID
from scaler.utility.logging.utility import get_logger_info
from scaler.utility.process_bootstrap import bootstrap_process

logger = logging.getLogger(__name__)


_READINESS_TIMEOUT_SECONDS = 30
_READINESS_RETRY_DELAY_SECONDS = 0.1


class ObjectStorageServerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        bind_address: AddressConfig,
        identity: str,
        logging_paths: tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
        security_config: Optional[SecurityConfig] = None,
    ):
        super().__init__(name="ObjectStorageServer")

        self._ident = identity

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        self._bind_address = bind_address
        self._security_config = security_config

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to serve requests."""
        backend = YMQNetworkBackend(num_threads=1)
        identity = ClientID.generate_client_id("ObjectStorageServerReadinessProbe")
        random_object_id = ObjectID.generate_object_id(identity)

        try:
            start_time = time.time()
            while time.time() - start_time < _READINESS_TIMEOUT_SECONDS:
                connector = None
                try:
                    connector = backend.create_sync_object_storage_connector(
                        identity=identity, address=self._bind_address
                    )

                    # Delete on a missing object returns immediately (delNotExists) and confirms the OSS is fully ready.
                    connector.delete_object(random_object_id)
                    return
                except (ObjectStorageException, YMQException, OSError):
                    time.sleep(_READINESS_RETRY_DELAY_SECONDS)
                finally:
                    if connector is not None:
                        connector.destroy()
        finally:
            backend.destroy()

        raise TimeoutError(f"ObjectStorageServer at {self._bind_address!r} failed to start within 30 seconds")

    def run(self) -> None:
        bootstrap_process(
            self._logging_paths, self._logging_config_file, self._logging_level, process_name="object_storage_server"
        )
        logger.info(f"ObjectStorageServer: start and listen to {self._bind_address!r}")

        log_format_str, log_level_str, logging_paths = get_logger_info(logging.getLogger("scaler"))

        tls_cert = self._security_config.tls_cert if self._security_config is not None else None
        tls_key = self._security_config.tls_key if self._security_config is not None else None

        self._server = ObjectStorageServer()
        try:
            self._server.run(
                repr(self._bind_address), self._ident, log_level_str, log_format_str, logging_paths, tls_cert, tls_key
            )
        except KeyboardInterrupt:
            logger.info("ObjectStorageServer: received KeyboardInterrupt, shutting down")
