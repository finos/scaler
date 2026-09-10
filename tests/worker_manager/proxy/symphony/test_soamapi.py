"""Covers the diagnosis that turns a failed ``soamapi`` import into an actionable message.

The three ways the import fails need three different fixes, so reporting one as another sends the reader to
the wrong place. These tests pin which cause produces which message.
"""

import builtins
import os
import tempfile
import unittest
from typing import Any, List, Optional
from unittest.mock import patch

from scaler.worker_manager.proxy.symphony.soamapi import (
    installed_python_api_versions,
    load_soamapi,
    soamapi_import_error,
)


class SoamapiImportErrorTest(unittest.TestCase):
    def test_missing_module_names_pythonpath_and_pypi(self) -> None:
        message = str(soamapi_import_error(ModuleNotFoundError("No module named 'soamapi'")))

        self.assertIn("not installable from PyPI", message)
        self.assertIn("PYTHONPATH", message)

    def test_unreachable_shared_library_names_ld_library_path(self) -> None:
        error = ImportError("libcom_platform_log4cxx_097_4.so.9: cannot open shared object file")

        message = str(soamapi_import_error(error))

        self.assertIn("LD_LIBRARY_PATH", message)
        self.assertIn("shared libraries", message)
        self.assertNotIn("not installable from PyPI", message)

    def test_interpreter_mismatch_names_the_running_version(self) -> None:
        message = str(soamapi_import_error(ImportError("bad magic number in 'soamapi': b'\\xee\\x0c\\r\\n'")))

        self.assertIn("different Python version", message)
        self.assertNotIn("not installable from PyPI", message)
        self.assertNotIn("LD_LIBRARY_PATH", message)


class LoadSoamapiTest(unittest.TestCase):
    def test_asks_symphony_to_select_bytecode_before_importing_soamapi(self) -> None:
        """``soamapi`` sits below the ``lib64`` on PYTHONPATH, so the selector has to be tried first."""
        imported = []
        real_import = builtins.__import__

        def recording_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name in ("soamapiversion", "soamapi"):
                imported.append(name)
                raise ModuleNotFoundError(f"No module named {name!r}")

            return real_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", recording_import):
            with self.assertRaises(ImportError):
                load_soamapi()

        self.assertEqual(imported, ["soamapiversion", "soamapi"])

    def test_a_missing_selector_does_not_hide_the_soamapi_diagnosis(self) -> None:
        """A PYTHONPATH naming the bytecode directory outright has no selector, which is not an error."""
        real_import = builtins.__import__

        def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name in ("soamapiversion", "soamapi"):
                raise ModuleNotFoundError(f"No module named {name!r}")

            return real_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", guarded_import):
            with self.assertRaises(ImportError) as raised:
                load_soamapi()

        self.assertIn("soamapi", str(raised.exception))
        self.assertNotIn("soamapiversion", str(raised.exception))


class InstalledPythonAPIVersionsTest(unittest.TestCase):
    def test_reads_versions_from_disk_lowest_first(self) -> None:
        with TemporaryLibraryDirectory(["pythonapi_3.12.0", "pythonapi_2.7.2", "pythonapi_3.9.0"]) as library:
            with patch("sys.path", [library]):
                self.assertEqual(installed_python_api_versions(), ["2.7.2", "3.9.0", "3.12.0"])

    def test_finds_versions_from_a_path_entry_inside_the_library_directory(self) -> None:
        with TemporaryLibraryDirectory(["pythonapi_3.10.0", "pythonapi_3.12.0"]) as library:
            with patch("sys.path", [os.path.join(library, "pythonapi_3.12.0")]):
                self.assertEqual(installed_python_api_versions(), ["3.10.0", "3.12.0"])

    def test_finds_versions_through_ld_library_path_alone(self) -> None:
        """LD_LIBRARY_PATH names the same lib64, so a host with only it set is still recognised."""
        with TemporaryLibraryDirectory(["pythonapi_3.10.0", "pythonapi_3.12.0"]) as library:
            with patch("sys.path", []), patch.dict(os.environ, {"LD_LIBRARY_PATH": library}, clear=True):
                self.assertEqual(installed_python_api_versions(), ["3.10.0", "3.12.0"])

    def test_ignores_pythonpath_that_the_interpreter_itself_ignores(self) -> None:
        """Under -E or -I a set PYTHONPATH is not on sys.path, and reporting it would be a false positive."""
        with TemporaryLibraryDirectory(["pythonapi_3.12.0"]) as library:
            with patch("sys.path", []), patch.dict(os.environ, {"PYTHONPATH": library}, clear=True):
                self.assertEqual(installed_python_api_versions(), [])

    def test_no_symphony_installation_yields_no_versions(self) -> None:
        with TemporaryLibraryDirectory([]) as library:
            with patch("sys.path", [library]), patch.dict(os.environ, {}, clear=True):
                self.assertEqual(installed_python_api_versions(), [])


class TemporaryLibraryDirectory:
    """A throwaway ``lib64`` holding the given ``pythonapi_<python-version>`` directories."""

    def __init__(self, names: List[str]) -> None:
        self._names = names
        self._directory: Optional[tempfile.TemporaryDirectory] = None

    def __enter__(self) -> str:
        directory = tempfile.TemporaryDirectory()
        self._directory = directory

        for name in self._names:
            os.mkdir(os.path.join(directory.name, name))

        return directory.name

    def __exit__(self, *exception_info: object) -> None:
        assert self._directory is not None
        self._directory.cleanup()


if __name__ == "__main__":
    unittest.main()
