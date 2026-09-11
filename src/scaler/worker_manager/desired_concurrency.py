from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand


def extract_desired_count(
    requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest], own_capabilities: Dict[str, int]
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
