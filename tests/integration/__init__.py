"""End-to-end scaling tests: a real client and scheduler drive a real worker manager that provisions real
workers in real Docker containers. See README.md.

Opt-in, one topology per run, because each needs a Docker daemon and takes minutes::

    SCALER_E2E=container python -m unittest tests.integration.test_scaling -v
"""

import os
from typing import List

# Names the one topology to run (see test_scaling.py). Unset skips every case, so the default
# `unittest discover` build never pays for a Docker daemon or an image build.
E2E_TOPOLOGY = os.environ.get("SCALER_E2E", "")

# The region the floci-backed managers use. floci does not validate it; it only has to be consistent.
AWS_REGION = "us-east-1"


def container_cli() -> List[str]:
    """The container CLI every backend shells out to. Override with ``SCALER_IT_CONTAINER_CLI`` where the
    docker socket is root-only (``sudo docker``) or to point at a podman shim."""
    return os.environ.get("SCALER_IT_CONTAINER_CLI", "docker").split()


def point_boto3_at_floci(endpoint_url: str) -> None:
    """Aim this process's boto3 clients at a floci emulator. Call from a manager entry point (a spawned
    child) only: it mutates the child's os.environ so the shipped provisioner's boto3 hits floci with dummy
    credentials floci never validates."""
    os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", AWS_REGION)
