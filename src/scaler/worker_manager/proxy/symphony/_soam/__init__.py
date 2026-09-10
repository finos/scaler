"""Subclasses of ``soamapi`` types, kept apart because importing them requires IBM Spectrum Symphony.

Every module in this package imports ``soamapi`` at module level, so importing any of them raises
``ImportError`` on a host without a Symphony installation. Nothing outside this package may import it at
module level. Reach it through the factories in ``message`` and ``callback``, which defer the import to
call time and turn the failure into a descriptive error.

``tests/worker_manager/proxy/symphony/test_import_safety.py`` enforces that boundary.
"""
