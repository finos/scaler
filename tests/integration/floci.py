"""A managed floci AWS emulator: it launches ECS ``RunTask`` and EC2 ``RunInstances`` containers as
siblings on the host Docker daemon (via the mounted socket), so it drives the shipped AWS managers through
a real scale curve with real workers and no AWS account. Speaks the AWS API over plain HTTP; a manager
reaches it via ``AWS_ENDPOINT_URL``. Spawned containers land on the bridge, reaching the host scheduler
over the gateway like the container backend's machines.

https://github.com/floci-io/floci
"""

from __future__ import annotations

import os
import subprocess
import time
import urllib.error
import urllib.request

from scaler.utility.network_util import get_available_tcp_port
from tests.integration import container_cli

_IMAGE = os.environ.get("SCALER_IT_FLOCI_IMAGE", "floci/floci:latest")
_INTERNAL_PORT = 4566
_DOCKER_SOCKET = "/var/run/docker.sock"

# floci names each launched container after the service that started it; the pools count and reap by these.
ECS_CONTAINER_PREFIX = "floci-ecs"
EC2_CONTAINER_PREFIX = "floci-ec2"

_READY_TIMEOUT_SECONDS = 60.0
_STOP_GRACE_SECONDS = 5


class FlociEmulator:
    """floci in a container with the host docker socket mounted. Register :meth:`stop` with ``addCleanup``
    before calling :meth:`start`, then read :attr:`endpoint_url`."""

    def __init__(self) -> None:
        self._port = get_available_tcp_port()
        # Suffixed with the pid so two runs on one host do not reap each other's emulator on start-up.
        # (The containers floci spawns keep its fixed floci-ecs/floci-ec2 prefixes, so genuinely
        # concurrent floci runs on one host remain unsupported.)
        self._name = f"scaler-it-floci-{os.getpid()}"
        self._started = False

    @property
    def endpoint_url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def start(self) -> None:
        subprocess.run([*container_cli(), "rm", "-f", self._name], capture_output=True)
        subprocess.run(
            [
                *container_cli(), "run", "-d", "--rm",
                "--name", self._name,
                "-p", f"{self._port}:{_INTERNAL_PORT}",
                "-v", f"{_DOCKER_SOCKET}:{_DOCKER_SOCKET}",
                _IMAGE,
            ],
            check=True,
            capture_output=True,
            text=True,
        )  # fmt: skip
        self._started = True
        self._wait_until_ready()

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(self.endpoint_url, timeout=2) as response:
                    response.read(1)
                return
            except urllib.error.HTTPError:
                return  # any HTTP status means the server is up and routing
            except OSError as error:
                last_error = repr(error)
                time.sleep(0.5)
        logs = subprocess.run([*container_cli(), "logs", self._name], capture_output=True, text=True).stdout
        raise TimeoutError(
            f"floci not ready at {self.endpoint_url} within {_READY_TIMEOUT_SECONDS:.0f}s "
            f"(last error: {last_error}); logs:\n{logs}"
        )

    def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        subprocess.run([*container_cli(), "stop", "-t", str(_STOP_GRACE_SECONDS), self._name], capture_output=True)
