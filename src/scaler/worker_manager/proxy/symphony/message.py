from __future__ import annotations

from typing import TYPE_CHECKING, Type

from scaler.worker_manager.proxy.symphony.soamapi import soamapi_import_error

if TYPE_CHECKING:
    from scaler.worker_manager.proxy.symphony._soam.message import SoamMessage


def create_soam_message_class() -> Type[SoamMessage]:
    """Return the ``soamapi.Message`` subclass that carries task payloads.

    The import is deferred to call time because ``_soam.message`` needs ``soamapi`` at its own import.
    """
    try:
        from scaler.worker_manager.proxy.symphony._soam.message import SoamMessage
    except ImportError as error:
        raise soamapi_import_error(error) from error

    return SoamMessage
