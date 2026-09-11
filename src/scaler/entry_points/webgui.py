# PYTHON_ARGCOMPLETE_OK
from typing import Optional

from scaler.config.section.webgui import WebGUIConfig
from scaler.ui.webgui import start_webgui


def main(config: Optional[WebGUIConfig] = None) -> None:
    if config is None:
        config = WebGUIConfig.parse("Web GUI for Scaler Monitoring", "gui")

    start_webgui(config)
