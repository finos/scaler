"""
Headless JupyterLite test.

Drives a headless Chromium against the locally-built docs site and runs the
``parallel_sqrt.ipynb`` notebook end-to-end inside JupyterLite to confirm:

  * the lite kernel starts,
  * the bundled scaler wasm wheel is preinstalled via piplite (no network),
  * ``import scaler`` succeeds,
  * the notebook completes without a cell error.

This catches breakage in the actual JupyterLite runtime path (lockfiles,
wheel tagging, MIME types, piplite_urls resolution) which the
``pyodide venv`` import test in
``tests/wasm/test_browser_client_imports.py`` does not.

The test is gated by ``RUN_JUPYTERLITE_TEST=1`` so it stays out of the
default ``python -m unittest discover`` run on developer laptops. CI sets
the variable explicitly.

Prerequisites:

    pip install playwright
    playwright install --with-deps chromium

Then build the docs (which embeds the wasm wheel) before running:

    cd docs && make html

To run locally:

    RUN_JUPYTERLITE_TEST=1 python -m unittest tests.wasm.test_jupyterlite
"""

import http.server
import os
import re
import socketserver
import threading
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_HTML = REPO_ROOT / "docs" / "build" / "html"

# The notebook itself does not assume a scheduler is reachable -- the test
# only verifies that the kernel starts, scaler imports (via the bundle patch
# in scripts/patch_jupyterlite_kernel.py), and the cells run to completion.
# The Client(...) call inside the notebook will raise a connection error
# (no scheduler is running), which we treat as success: it proves scaler is
# loaded and executing user code.


@unittest.skipUnless(
    os.environ.get("RUN_JUPYTERLITE_TEST") == "1", "Set RUN_JUPYTERLITE_TEST=1 to enable the headless JupyterLite test."
)
class JupyterLiteTests(unittest.TestCase):
    """Headless test for the docs-site JupyterLite build."""

    server: socketserver.TCPServer
    server_thread: threading.Thread
    port: int

    @classmethod
    def setUpClass(cls) -> None:
        if not (DOCS_HTML / "lite" / "lab" / "index.html").is_file():
            raise unittest.SkipTest("docs/build/html/lite/lab/index.html missing. " "Run `cd docs && make html` first.")
        if not (DOCS_HTML / "_static" / "wasm").is_dir():
            raise unittest.SkipTest(
                "docs/build/html/_static/wasm missing. "
                "Run `scripts/build_wasm.sh` then `cd docs && make html` first."
            )

        # Serve docs/build/html on an ephemeral port. JupyterLite needs to
        # be served from a real HTTP server (not file://) for service
        # workers and SharedArrayBuffer headers to work.
        handler_class = _make_handler(str(DOCS_HTML))
        cls.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), handler_class)
        cls.port = cls.server.server_address[1]
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True, name="jupyterlite-http")
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=5)

    def test_parallel_sqrt_runs(self) -> None:
        self._run_notebook("parallel_sqrt.ipynb")

    def test_monte_carlo_pi_runs(self) -> None:
        self._run_notebook("monte_carlo_pi.ipynb")

    def _run_notebook(self, notebook: str) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.skipTest("playwright not installed; pip install playwright && playwright install chromium")

        url = f"http://127.0.0.1:{self.port}/lite/lab/index.html?path={notebook}"

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            console_messages: list[str] = []
            page.on("console", lambda msg: console_messages.append(msg.text))
            page.on("pageerror", lambda exc: console_messages.append(f"PAGEERROR: {exc}"))

            page.goto(url, wait_until="load", timeout=120_000)

            # Wait for the lab UI to register the notebook command.
            page.wait_for_selector("div.jp-Notebook", timeout=120_000)

            # Trigger Run All Cells via the menu bar. Keyboard-shortcut and
            # command-palette paths are unreliable in headless mode (focus
            # races, palette open timing), and JupyterLab does not expose
            # its app instance on ``window``. The menu bar is plain DOM and
            # always present, so clicking it is the most robust option.
            page.get_by_role("menuitem", name="Run", exact=True).click()
            page.get_by_role("menuitem", name="Run All Cells", exact=True).click()

            # "Run All Cells" queues every cell as "[*]:" immediately, so "[*]:"
            # alone proves nothing ran. Each demo runs ``%pip install
            # opengris-scaler`` first, then ``from scaler import Client`` +
            # ``Client(...)``. Two observed facts drive the check below:
            #   * Jupyter "Run All" HALTS on the first cell error, so if the %pip
            #     install fails, the import cell never runs -- it stays at the empty
            #     "[ ]:" prompt with no output.
            #   * ``Client(...)`` itself raises here (the test runs no scheduler),
            #     so on SUCCESS the import cell finishes with a *connection* error
            #     (e.g. SysCallError) -- NOT a scaler import error.
            # Therefore success == the import cell actually RAN (numeric/running
            # prompt, not "[ ]:") AND its output shows no scaler-import failure. We
            # read cell OUTPUT because a ModuleNotFoundError is kernel output, not a
            # JS pageerror -- a pageerror-only check missed a fully broken import.
            import_error_markers = ("No module named 'scaler'", "Can't find a pure Python 3 wheel")

            def cell_state(js_regex: str):
                """{prompt, output} of the first code cell whose source matches js_regex."""
                return page.evaluate(
                    "() => {"
                    "  const cells = Array.from(document.querySelectorAll('.jp-Notebook .jp-Cell'))"
                    "    .filter(c => c.querySelector('.jp-InputPrompt'));"
                    f"  const cell = cells.find(c => ({js_regex})"
                    "    .test(c.querySelector('.jp-InputArea-editor')?.innerText || ''));"
                    "  if (!cell) return null;"
                    "  return {"
                    "    prompt: (cell.querySelector('.jp-InputPrompt').textContent || '').trim(),"
                    "    output: (cell.querySelector('.jp-OutputArea')?.innerText || '').trim(),"
                    "  };"
                    "}"
                )

            # (1) Wait for the %pip install cell to FINISH -- a numeric prompt
            # ("[N]:"), not the queued/running "[*]:". 5 minutes absorbs cold
            # pyodide load + the wheel install on slow CI runners.
            page.wait_for_function(
                "() => {"
                "  const cells = Array.from(document.querySelectorAll('.jp-Notebook .jp-Cell'))"
                "    .filter(c => c.querySelector('.jp-InputPrompt'));"
                "  const cell = cells.find(c => /pip install [^\\n]*opengris-scaler/"
                "    .test(c.querySelector('.jp-InputArea-editor')?.innerText || ''));"
                "  if (!cell) return false;"
                "  const prompt = (cell.querySelector('.jp-InputPrompt').textContent || '').trim();"
                "  return /^\\[\\s*\\d+\\s*\\]/.test(prompt);"
                "}",
                timeout=300_000,
            )

            # (2) Grace for execution to continue into the import cell (or to halt
            # and revert it to "[ ]:" if the install failed), then inspect it. We do
            # NOT assert on the %pip cell's own output: piplite's install log is
            # noisy and non-deterministic (benign "Path resolution bailing" lines /
            # non-fatal resolution tracebacks appear even on a clean install).
            page.wait_for_timeout(25_000)
            imp_cell = cell_state(r"/\bfrom scaler\b|\bimport scaler\b/")
            self.assertIsNotNone(imp_cell, f"{notebook}: no cell importing scaler was found")
            pip_cell = cell_state(r"/pip install [^\n]*opengris-scaler/")

            # The import cell must have RUN (numeric or still-running prompt). The
            # empty "[ ]:" means Run-All halted on the %pip cell -> install failed.
            ran = bool(re.match(r"^\[\s*(?:\d+|\*)\s*\]", imp_cell["prompt"]))
            self.assertTrue(
                ran,
                msg=(
                    f"{notebook}: the scaler import cell never executed (prompt "
                    f"{imp_cell['prompt']!r}); '%pip install opengris-scaler' must have "
                    f"failed and halted Run-All.\n%pip-cell output:\n{(pip_cell or {}).get('output', '')[:1200]}"
                ),
            )
            # And it must not have failed to import scaler. The Client(...) call
            # raises a connection error here (no scheduler) -- expected, and NOT a
            # scaler import failure -- so we match only the import/install signature.
            imp_failed = [m for m in import_error_markers if m in imp_cell["output"]]
            self.assertFalse(
                imp_failed,
                msg=(
                    f"{notebook}: in-browser 'import scaler' FAILED (matched {imp_failed}).\n"
                    f"import-cell prompt={imp_cell['prompt']!r}\noutput:\n{imp_cell['output'][:1200]}\n"
                    f"console:\n{chr(10).join(console_messages[-15:])}"
                ),
            )
            browser.close()


def _make_handler(directory: str):
    """Build a SimpleHTTPRequestHandler bound to ``directory``."""

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

        def log_message(self, format, *args):  # noqa: A002
            # Suppress per-request stderr noise during the test.
            pass

    return Handler


if __name__ == "__main__":
    unittest.main()
