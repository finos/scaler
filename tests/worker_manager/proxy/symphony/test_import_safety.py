"""Pins the boundary between modules that need IBM Spectrum Symphony and modules that do not.

``soamapi`` ships with the Symphony product and is absent from development and CI hosts, so most of the
symphony package has to stay importable, and unit testable, without it. Only the ``_soam`` subpackage may
import it at module level. A module that drifts across that line breaks every host without Symphony, and
does so at import time, far from whatever change caused it.
"""

import builtins
import importlib
import pkgutil
import sys
import unittest
from typing import Any, List
from unittest.mock import patch

import scaler.worker_manager.proxy.symphony as symphony_package
import scaler.worker_manager.proxy.symphony._soam as soam_package

SOAMAPI_MODULE = "soamapi"


def _module_names(package: Any) -> List[str]:
    return sorted(f"{package.__name__}.{info.name}" for info in pkgutil.iter_modules(package.__path__))


def _without_soamapi() -> Any:
    """Make ``import soamapi`` fail even on a host that does have Symphony installed."""
    real_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == SOAMAPI_MODULE:
            raise ModuleNotFoundError(f"No module named {SOAMAPI_MODULE!r}")
        return real_import(name, *args, **kwargs)

    return patch.object(builtins, "__import__", guarded_import)


class ImportSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._original_modules = dict(sys.modules)

    def tearDown(self) -> None:
        for name in set(sys.modules) - set(self._original_modules):
            del sys.modules[name]
        sys.modules.update(self._original_modules)

    def _reimport(self, module_name: str) -> None:
        sys.modules.pop(module_name, None)
        importlib.import_module(module_name)

    def test_symphony_modules_import_without_soamapi(self) -> None:
        for module_name in _module_names(symphony_package):
            with self.subTest(module=module_name), _without_soamapi():
                self._reimport(module_name)

    def test_soam_modules_require_soamapi(self) -> None:
        module_names = _module_names(soam_package)
        self.assertNotEqual(module_names, [], "expected the soam subpackage to hold modules")

        for module_name in module_names:
            with self.subTest(module=module_name), _without_soamapi():
                with self.assertRaises(ImportError):
                    self._reimport(module_name)


if __name__ == "__main__":
    unittest.main()
