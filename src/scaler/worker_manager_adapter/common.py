from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand


class CapacityExceededError(Exception):
    pass


class WorkerNotFoundError(Exception):
    pass


def extract_desired_count(
    requests: list[WorkerManagerCommand.DesiredTaskConcurrencyRequest], own_capabilities: dict[str, int]
) -> int:
    """Return the desired worker count for this provisioner from a declarative scaling command.

    Sums taskConcurrency across all requests whose capability set is a subset of own_capabilities.
    An empty capability set in a request acts as a wildcard that matches any provisioner.
    Returns 0 if no request matches.
    """
    total = 0
    for request in requests:
        request_capabilities = {entry.name: entry.value for entry in request.capabilities}
        if request_capabilities.items() <= own_capabilities.items():
            total += request.taskConcurrency
    return total


def load_requirements_content(requirements_txt: str) -> str:
    """Return requirements file content, reading from disk if requirements_txt is a file path."""
    if os.path.isfile(requirements_txt):
        with open(requirements_txt) as f:
            return f.read()
    return requirements_txt


def format_capabilities(capabilities: dict[str, int]) -> str:
    """
    Reverse of `parse_capabilities`: convert a capabilities dict into a
    comma-separated capability string (e.g. "linux,cpu=4").
    Values equal to -1 are emitted as flag-style entries (no `=value`).
    """
    parts = []
    for name, value in capabilities.items():
        if value == -1:
            parts.append(name)
        else:
            parts.append(f"{name}={value}")
    return ",".join(parts)
