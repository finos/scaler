"""How the Symphony service tags the outcome it sends back, shared by both sides of that contract.

This module is deliberately a leaf: it imports nothing, from scaler or anywhere else. The service half
of the contract, ``scripts/symphony/scaler_service.py``, runs under Symphony on a compute host from a
copy that ``setup_application.py`` deploys, and that copy has no scaler to import. Packaging this file
alongside it is what lets both halves read the tags from one definition rather than two that can drift.
"""

import enum


class TaskOutputTag(str, enum.Enum):
    """What the second element of a task's output payload means.

    The payload is ``cloudpickle.dumps((tag, value))``. The tag is written as its string value rather
    than as this enum, so that the service and the worker manager need not agree on anything but the
    strings.
    """

    RESULT = "result"
    EXCEPTION = "exception"
    UNSERIALIZABLE_EXCEPTION = "unserializable-exception"
