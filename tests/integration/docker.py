"""The container CLI every backend provisions through, and the images they run.

The worker image bakes in the freshly built wheel plus the custom capnp/kj libs the C++ extensions link
against, so a container matches the host scheduler's build and wire protocol with no bind-mount. Portable
across the dev host (libs in /usr/local/lib) and CI ($SCALER_THIRDPARTY_PREFIX/lib); the wheel and
interpreter come from the current environment.
"""

from __future__ import annotations

import asyncio
import glob
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Dict, List, Optional, Sequence

from tests.integration import container_cli

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))

WORKER_IMAGE = "scaler-it-worker:local"
# The worker image plus an entrypoint that execs the ECS provisioner's COMMAND env var.
ECS_IMAGE = "scaler-it-ecs-worker:local"
# floci launches EC2 RunInstances on this exact image name; ec2.Dockerfile re-tags an AMI-baseline
# superset over it so the shipped UserData runs unmodified.
EC2_BASE_IMAGE = "public.ecr.aws/amazonlinux/amazonlinux:2023"
_WORKER_BASE_IMAGE = "ubuntu:26.04"

# Rebuild even when the tag exists, so a wheel change is never silently run against a stale image. The
# run script sets it; CI always builds fresh.
_REBUILD = os.environ.get("SCALER_IT_REBUILD") == "1"

# The extensions dynamically link the custom capnp/kj libs; ymq statically links libuv, so capnp/kj is all
# the worker needs. The rest are copied when present but not required.
_REQUIRED_LIB_GLOBS = ("libcapnp*.so*", "libkj*.so*")
_OPTIONAL_LIB_GLOBS = ("libuv*.so*", "libssl*.so*", "libcrypto*.so*")

# Max seconds docker waits for a container to exit on stop before SIGKILL. Headroom for the worker's
# graceful task-finishing shutdown, not a fixed delay (docker returns as soon as the container exits).
_STOP_GRACE_SECONDS = 15


def _cli(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*container_cli(), *args], check=check, capture_output=True, text=True, stdin=subprocess.DEVNULL
    )


def is_available() -> bool:
    try:
        return _cli("info", check=False).returncode == 0
    except OSError:
        return False


def host_gateway() -> str:
    """The bridge address a container uses to reach the scheduler on the host."""
    return _cli("network", "inspect", "bridge", "-f", "{{(index .IPAM.Config 0).Gateway}}").stdout.strip()


class DockerRuntime:
    """Run and stop containers from a worker manager's event loop.

    Async because a blocking subprocess on that loop both stalls heartbeats and can deadlock waitpid
    against the loop's own child reaping.
    """

    async def _cli_async(self, *args: str, check: bool = True) -> str:
        # stdin=DEVNULL: a detached launch must never read stdin (no sudo/tty stall).
        process = await asyncio.create_subprocess_exec(
            *container_cli(), *args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if check and process.returncode != 0:
            raise RuntimeError(f"`docker {args[0]}` failed ({process.returncode}): {stderr.decode().strip()}")
        return stdout.decode()

    async def run(self, image: str, name: str, command: Sequence[str], env: Optional[Dict[str, str]] = None) -> str:
        args: List[str] = []
        for key, value in (env or {}).items():
            args += ["-e", f"{key}={value}"]
        # --rm so an exited machine leaves nothing behind; detached so start_units does not block.
        return (await self._cli_async("run", "-d", "--rm", "--name", name, *args, image, *command)).strip()

    async def stop(self, container: str) -> None:
        # docker stop sends SIGTERM; the native manager forwards SIGINT so each worker finishes its
        # in-flight task first. This only caps a genuinely stuck shutdown -- docker returns as soon as
        # the container exits, so it is not a fixed wait.
        await self._cli_async("stop", "-t", str(_STOP_GRACE_SECONDS), container, check=False)


def _image_exists(tag: str) -> bool:
    return _cli("image", "inspect", tag, check=False).returncode == 0


def _libs_dir() -> str:
    prefix = os.environ.get("SCALER_THIRDPARTY_PREFIX")
    return os.path.join(prefix, "lib") if prefix else "/usr/local/lib"


def _stage_libs(libs_dir: str, patterns: tuple, destination: str) -> int:
    """Copy matching libs into the build context, preserving the libX.so -> libX-1.1.0.so symlinks."""
    copied = 0
    for pattern in patterns:
        for lib in glob.glob(os.path.join(libs_dir, pattern)):
            shutil.copy2(lib, os.path.join(destination, os.path.basename(lib)), follow_symlinks=False)
            copied += 1
    return copied


def _build(dockerfile: str, tag: str, context: str, **build_args: str) -> None:
    args = [arg for key, value in build_args.items() for arg in ("--build-arg", f"{key}={value}")]
    _cli("build", "-f", os.path.join(_HERE, dockerfile), *args, "-t", tag, context)


def ensure_worker_image() -> str:
    """The image the container machines and ECS tasks run: the current wheel plus its runtime libs."""
    if not _REBUILD and _image_exists(WORKER_IMAGE):
        return WORKER_IMAGE

    wheels = sorted(glob.glob(os.path.join(_REPO_ROOT, "dist", "*.whl")))
    if not wheels:
        raise RuntimeError("no wheel under dist/; build one with `python -m build --wheel`")

    with tempfile.TemporaryDirectory(prefix="scaler-it-image-") as context:
        wheel_dir, libs_stage = os.path.join(context, "wheel"), os.path.join(context, "libs")
        os.mkdir(wheel_dir)
        os.mkdir(libs_stage)
        shutil.copy(wheels[-1], wheel_dir)
        if not _stage_libs(_libs_dir(), _REQUIRED_LIB_GLOBS, libs_stage):
            raise RuntimeError(f"no capnp/kj runtime libs (e.g. libcapnp*.so) under {_libs_dir()}")
        _stage_libs(_libs_dir(), _OPTIONAL_LIB_GLOBS, libs_stage)
        _build(
            "worker.Dockerfile",
            WORKER_IMAGE,
            context,
            BASE_IMAGE=_WORKER_BASE_IMAGE,
            PYTHON_VERSION=f"{sys.version_info.major}.{sys.version_info.minor}",
        )
    return WORKER_IMAGE


def ensure_ecs_image() -> str:
    ensure_worker_image()
    if _REBUILD or not _image_exists(ECS_IMAGE):
        # Stage just the entrypoint so the build context stays tiny.
        with tempfile.TemporaryDirectory(prefix="scaler-it-ecs-image-") as context:
            shutil.copy(os.path.join(_HERE, "ecs_entrypoint.sh"), context)
            _build("ecs.Dockerfile", ECS_IMAGE, context, BASE_IMAGE=WORKER_IMAGE)
    return ECS_IMAGE


def ensure_ec2_base_image() -> str:
    """Tag the AMI-baseline AL2023 image floci launches for RunInstances. Always rebuilt: it is two dnf
    packages over a cached base."""
    with tempfile.TemporaryDirectory(prefix="scaler-it-ec2-base-") as context:
        _build("ec2.Dockerfile", EC2_BASE_IMAGE, context)
    return EC2_BASE_IMAGE
