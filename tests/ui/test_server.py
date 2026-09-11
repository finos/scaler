"""The web GUI's HTTP surface: the page, its static files, the event stream and the view endpoint.

The GUI serves these from `http.server`, so each is exercised over a real socket, not a test client.
"""

import json
import socket
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

from scaler.config.section.webgui import WebGUIConfig
from scaler.config.types.address import AddressConfig
from scaler.ui.app import BROWSER_QUEUE_MAX_PAYLOADS, WebUIApp
from scaler.ui.server import WebGUIServer
from scaler.utility.network_util import get_available_tcp_port


class TestWebGUIServer(unittest.TestCase):
    def setUp(self) -> None:
        config = WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380"))
        self.app = WebUIApp(config)
        self.port = get_available_tcp_port()
        self.server = WebGUIServer(("127.0.0.1", self.port), self.app, config.security)
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def test_the_page_and_its_assets_are_served(self) -> None:
        page = urllib.request.urlopen(f"{self.base}/", timeout=10)
        self.assertEqual(page.status, 200)
        self.assertIn(b"Scaler Monitor", page.read())

        script = urllib.request.urlopen(f"{self.base}/static/app.js", timeout=10)
        self.assertEqual(script.status, 200)

    def test_a_path_outside_the_static_directory_is_refused(self) -> None:
        # Sent raw: urllib resolves the `..` away before the request leaves, which never reaches the guard.
        self.assertIn(b"404", self.__raw_get("/static/../pyproject.toml"))
        self.assertIn(b"404", self.__raw_get("/static/../../pyproject.toml"))

    def test_a_stream_opens_with_a_full_state_and_then_carries_what_is_queued(self) -> None:
        with urllib.request.urlopen(f"{self.base}/events", timeout=10) as stream:
            first = self.__next_event(stream)
            self.assertEqual(first["type"], "full_state")

            browser = self.app.get_browser(first["browser_id"])
            self.assertIsNotNone(browser)
            browser.offer(json.dumps({"type": "view_update", "marker": 7}))
            self.assertEqual(self.__next_event(stream)["marker"], 7)

    def test_a_view_request_moves_that_browser_and_is_answered_with_the_update(self) -> None:
        with urllib.request.urlopen(f"{self.base}/events", timeout=10) as stream:
            browser_id = self.__next_event(stream)["browser_id"]
            answer = self.__post_view({"browser_id": browser_id, "settings": {"stream_window": 30}})

            self.assertEqual(answer["type"], "view_update")
            self.assertEqual(answer["settings"]["stream_window"], 30)
            self.assertEqual(self.app.get_browser(browser_id).view.stream_window_minutes, 30)

    def test_a_stream_opens_on_the_view_the_browser_saved(self) -> None:
        """A reload or a dropped stream reopens on what the browser was showing, not on the default view."""
        state = json.dumps({"view": {"task_events_task": "abc"}, "settings": {"stream_window": 30}})
        with urllib.request.urlopen(self.__events_url(state), timeout=10) as stream:
            first = self.__next_event(stream)

        self.assertEqual(first["settings"]["stream_window"], 30)
        self.assertEqual(first["task_stream"]["window"], 30 * 60)
        self.assertEqual(first["task_events_task"], "abc")

    def test_a_saved_view_that_cannot_be_applied_opens_on_the_default_view(self) -> None:
        malformed_page = json.dumps({"view": {"workers_page": "x"}, "settings": {"stream_window": 30}})
        for state in ("not json", json.dumps([30]), malformed_page):
            with self.subTest(state=state):
                with urllib.request.urlopen(self.__events_url(state), timeout=10) as stream:
                    first = self.__next_event(stream)

                self.assertEqual(first["type"], "full_state")
                self.assertEqual(first["settings"]["stream_window"], 5)

    def test_a_browser_that_falls_too_far_behind_is_dropped(self) -> None:
        """A browser that stops reading must lose its stream, so it reconnects to a fresh full state."""
        with urllib.request.urlopen(f"{self.base}/events", timeout=10) as stream:
            browser_id = self.__next_event(stream)["browser_id"]
            browser = self.app.get_browser(browser_id)

            for _ in range(BROWSER_QUEUE_MAX_PAYLOADS + 1):
                browser.offer(json.dumps({"type": "view_update"}))
            self.assertTrue(browser.is_closed())

            deadline = time.time() + 20
            while time.time() < deadline and self.app.get_browser(browser_id) is not None:
                stream.read(1)

        self.assertIsNone(self.app.get_browser(browser_id))

    def test_a_malformed_view_is_answered_rather_than_raised(self) -> None:
        """A bad page value must come back as a 400, not a 500 with a traceback in the log."""
        with urllib.request.urlopen(f"{self.base}/events", timeout=10) as stream:
            browser_id = self.__next_event(stream)["browser_id"]

            for body in (
                {"browser_id": browser_id, "view": {"workers_page": "not-a-number"}},
                {"browser_id": browser_id, "settings": {"stream_window": []}},
                {"browser_id": "not-a-number"},
            ):
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    self.__post_view(body)
                self.assertEqual(raised.exception.code, 400)

            self.assertEqual(self.app.get_browser(browser_id).view.workers_page, 0)

    def test_a_request_for_an_unknown_browser_is_refused(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.__post_view({"browser_id": 4321, "view": {"workers_page": 1}})
        self.assertEqual(raised.exception.code, 410)

    def __events_url(self, state: str) -> str:
        return f"{self.base}/events?{urllib.parse.urlencode({'state': state})}"

    def __raw_get(self, path: str) -> bytes:
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            connection.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
            return connection.recv(4096)

    def __next_event(self, stream: Any) -> Dict[str, Any]:
        for line in stream:
            if line.startswith(b"data: "):
                return json.loads(line[len(b"data: ") :])
        raise AssertionError("the stream ended without an event")

    def __post_view(self, body: Dict[str, Any]) -> Dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base}/view", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
        )
        return json.loads(urllib.request.urlopen(request, timeout=10).read())


if __name__ == "__main__":
    unittest.main()
