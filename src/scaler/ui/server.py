"""The web GUI's HTTP server: static files, a server-sent event stream, and one view endpoint.

The GUI pushes far more than it receives, so the browser holds a `text/event-stream` and posts view changes back.
That needs nothing beyond `http.server`.
"""

import json
import logging
import mimetypes
import ssl
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from scaler.config.common.security import SecurityConfig
from scaler.ui.app import STATIC_DIR, BrowserView, WebUIApp

logger = logging.getLogger(__name__)

# How long a stream waits for a payload before writing a comment line, so a proxy or a NAT leaves it open.
STREAM_KEEPALIVE_SECONDS = 15.0

MAX_VIEW_REQUEST_BYTES = 64 * 1024


class WebGUIRequestHandler(BaseHTTPRequestHandler):
    """Serves one browser. `ThreadingHTTPServer` gives each connection its own thread."""

    protocol_version = "HTTP/1.1"
    server_version = "ScalerWebGUI"

    @property
    def app(self) -> WebUIApp:
        return self.server.app  # type: ignore[attr-defined]

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path == "/":
            self.__send_file(STATIC_DIR / "index.html")
        elif path == "/events":
            self.__stream_events()
        elif path.startswith("/static/"):
            self.__send_static(path[len("/static/") :])
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != "/view":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        request = self.__read_json_body()
        if request is None:
            return

        try:
            stream = self.app.get_browser(int(request.get("browser_id", 0)))
        except (TypeError, ValueError):
            self.send_error(HTTPStatus.BAD_REQUEST, "malformed browser id")
            return

        if stream is None:
            self.send_error(HTTPStatus.GONE, "unknown browser")
            return

        try:
            stream.view.apply_view(request.get("view", {}))
            stream.view.apply_settings(request.get("settings", {}))
        except (TypeError, ValueError):
            # A page or window value that is not a number is answered, not raised: raising is a 500.
            self.send_error(HTTPStatus.BAD_REQUEST, "malformed view")
            return

        update = self.app.view_update(stream.view)
        update["type"] = "view_update"
        self.__send_bytes(json.dumps(update).encode(), "application/json")

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002, the base class names it
        logger.debug("%s %s", self.address_string(), format % args)

    def __read_json_body(self) -> Optional[Dict[str, Any]]:
        """The posted object, or None when the request was answered with an error."""
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0

        if length <= 0 or length > MAX_VIEW_REQUEST_BYTES:
            self.send_error(HTTPStatus.BAD_REQUEST, "bad content length")
            return None

        try:
            request = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            self.send_error(HTTPStatus.BAD_REQUEST, "malformed request")
            return None

        if not isinstance(request, dict):
            self.send_error(HTTPStatus.BAD_REQUEST, "malformed request")
            return None
        return request

    def __opening_view(self) -> BrowserView:
        """The view a stream opens on, which the browser sends so a reload or a dropped stream shows what it did.

        A saved view the server can no longer apply opens on the default view rather than failing the stream.
        """
        view = BrowserView()
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        try:
            state = json.loads(query.get("state", ["{}"])[0])
            view.apply_view(state.get("view", {}))
            view.apply_settings(state.get("settings", {}))
        except (AttributeError, TypeError, ValueError):
            return BrowserView()
        return view

    def __stream_events(self) -> None:
        """Hold one browser's event stream open, writing each payload the batcher queues for it."""
        stream = self.app.add_browser(self.__opening_view())
        try:
            # The stream has no length and never ends on its own, so the connection is what delimits it.
            self.close_connection = True
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()

            full_state = self.app.get_full_state(stream.view)
            full_state["type"] = "full_state"
            full_state["browser_id"] = stream.browser_id
            self.__write_event(json.dumps(full_state))

            while not stream.is_closed():
                payload = stream.take(timeout=STREAM_KEEPALIVE_SECONDS)
                if payload is None:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                self.__write_event(payload)
        except OSError:  # the browser went away mid-write
            pass
        finally:
            self.app.remove_browser(stream.browser_id)

    def __write_event(self, payload: str) -> None:
        """One SSE event. A payload never contains a newline, so it is always one `data:` line."""
        self.wfile.write(b"data: " + payload.encode() + b"\n\n")
        self.wfile.flush()

    def __send_static(self, name: str) -> None:
        """Serve one of the files the static directory holds, matched by name.

        The request never joins a path.
        It picks from what the directory lists, so a `..` or an absolute path matches nothing and gets a 404.
        """
        for asset in STATIC_DIR.iterdir():
            if asset.name == name and asset.is_file():
                self.__send_file(asset)
                return

        self.send_error(HTTPStatus.NOT_FOUND)

    def __send_file(self, path: Path) -> None:
        content_type, _ = mimetypes.guess_type(path.name)
        self.__send_bytes(path.read_bytes(), content_type or "application/octet-stream")

    def __send_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        # Assets come from the running package, so a cached copy only lets a browser disagree with it.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.end_headers()
        self.wfile.write(body)


class WebGUIServer(ThreadingHTTPServer):
    """A `ThreadingHTTPServer` that holds the GUI state its handlers read."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: Tuple[str, int], app: WebUIApp, security: SecurityConfig) -> None:
        super().__init__(address, WebGUIRequestHandler)
        self.app = app
        if security.has_credentials():
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            # TLS 1.0 and 1.1 are deprecated by RFC 8996 and the default context still offers them.
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(certfile=security.tls_cert, keyfile=security.tls_key)
            self.socket = context.wrap_socket(self.socket, server_side=True)
