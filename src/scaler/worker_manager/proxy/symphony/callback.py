from __future__ import annotations

from typing import TYPE_CHECKING, Type

from scaler.worker_manager.proxy.symphony.soamapi import soamapi_import_error

if TYPE_CHECKING:
    from scaler.worker_manager.proxy.symphony._soam.session_callback import SoamSessionCallback


def create_session_callback_class() -> Type[SoamSessionCallback]:
    """Return the ``soamapi.SessionCallback`` subclass that forwards events to a ``TaskResponseRouter``.

    The import is deferred to call time because ``_soam.session_callback`` needs ``soamapi`` at its own import.
    """
    try:
        from scaler.worker_manager.proxy.symphony._soam.session_callback import SoamSessionCallback
    except ImportError as error:
        raise soamapi_import_error(error) from error

    return SoamSessionCallback
