import logging
import signal

from scaler.config.section.webgui import WebGUIConfig
from scaler.ui.app import create_app
from scaler.ui.server import WebGUIServer
from scaler.utility.process_bootstrap import bootstrap_process

logger = logging.getLogger(__name__)


def start_webgui(config: WebGUIConfig) -> None:
    bootstrap_process(
        config.logging_config.paths, config.logging_config.config_file, config.logging_config.level, process_name="gui"
    )

    def _raise_keyboard_interrupt(*_args: object) -> None:
        raise KeyboardInterrupt

    # The `scaler` launcher stops this process with SIGTERM; on Windows it cannot be delivered, harmlessly.
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    app = create_app(config)
    server = WebGUIServer((config.gui_address.host, config.gui_address.port), app, config.security)

    scheme = "https" if config.security.has_credentials() else "http"
    logger.info(f"Web GUI is now listening on: {scheme}://{config.gui_address}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Web GUI: stopped by user")
    finally:
        server.server_close()
        app.stop()
