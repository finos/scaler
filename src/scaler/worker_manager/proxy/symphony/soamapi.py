"""Deferred access to the IBM Spectrum Symphony ``soamapi`` module.

This module is not ``soamapi`` itself, it is how the rest of the package reaches it. The stub that
describes the real API is ``stubs/soamapi.pyi``, which is unrelated to this file.

``soamapi`` is not installable from PyPI. It ships with the Symphony product as bytecode, one
``pythonapi_<python-version>`` directory per supported interpreter, under
``$SOAM_HOME/$SOAM_VERSION/$BINARY_TYPE/lib64``. Sourcing ``$SOAM_HOME/conf/profile.soam`` puts that
``lib64`` on both ``PYTHONPATH`` and ``LD_LIBRARY_PATH``. Both are needed: the API is backed by shared
libraries in the same directory, so setting ``PYTHONPATH`` alone fails at import. ``lib64`` holds the
per-interpreter directories rather than ``soamapi`` itself, so ``load_soamapi`` first asks Symphony to select
the one matching the running interpreter.

The import is deferred to call time so that this package stays importable, and its
Symphony-independent logic stays unit testable, on hosts without a Symphony installation.
"""

import os
import sys
import types
from typing import Dict, List, Set, Tuple

PYTHON_API_PREFIX = "pythonapi_"

_SETUP_HINT = (
    "Source $SOAM_HOME/conf/profile.soam, which puts $SOAM_HOME/$SOAM_VERSION/$BINARY_TYPE/lib64 on both "
    "PYTHONPATH and LD_LIBRARY_PATH."
)


def load_soamapi() -> types.ModuleType:
    """Return the ``soamapi`` module, raising a descriptive ``ImportError`` when it is unavailable."""
    try:
        _select_bytecode_for_running_interpreter()
        import soamapi
    except ImportError as error:
        raise soamapi_import_error(error) from error

    return soamapi


def _select_bytecode_for_running_interpreter() -> None:
    """Let Symphony put its bytecode directory for the running interpreter on ``sys.path``.

    ``soamapi`` sits one directory below the ``lib64`` that the Symphony environment puts on ``PYTHONPATH``,
    so sourcing that environment is not enough to import it. ``soamapiversion``, which ships in ``lib64``
    itself, appends the ``pythonapi_<python-version>`` directory matching the running interpreter. A
    ``PYTHONPATH`` naming that directory outright does not need it, so its absence is not an error here: the
    ``soamapi`` import that follows is what decides.
    """
    try:
        import soamapiversion  # noqa: F401
    except ImportError:
        pass


def soamapi_import_error(error: ImportError) -> ImportError:
    """Return an ``ImportError`` naming the actual cause of a failed ``soamapi`` import.

    A missing installation, an interpreter Symphony has no bytecode for, and unreachable shared libraries
    each need a different fix, and the message for one misdirects for the other two. The cause is read off
    ``error`` rather than assumed.
    """
    text = str(error)

    if "bad magic number" in text:
        return ImportError(
            f"IBM Spectrum Symphony API (soamapi) was found, but it is bytecode for a different Python version "
            f"than the running interpreter ({_running_python_version()}). {_installed_versions_clause()} Run the "
            f"worker manager on one of those versions."
        )

    if "cannot open shared object file" in text or "undefined symbol" in text:
        return ImportError(
            f"IBM Spectrum Symphony API (soamapi) was found, but the shared libraries backing it were not "
            f"({text}). {_SETUP_HINT}"
        )

    return ImportError(
        f"IBM Spectrum Symphony API (soamapi) not found ({text}). It is not installable from PyPI, it ships "
        f"with the Symphony product. {_SETUP_HINT} {_installed_versions_clause()}"
    )


def installed_python_api_versions() -> List[str]:
    """Return the Python versions the installed Symphony ships ``soamapi`` bytecode for, lowest first.

    The directories on disk are the supported set for whatever build is installed, so they are read rather
    than pinned to a list that a later Symphony build would make wrong.
    """
    versions: Set[str] = set()

    for directory in _candidate_library_directories():
        try:
            entries = os.listdir(directory)
        except OSError:
            continue

        versions.update(entry[len(PYTHON_API_PREFIX) :] for entry in entries if entry.startswith(PYTHON_API_PREFIX))

    return sorted(versions, key=_version_sort_key)


def _candidate_library_directories() -> List[str]:
    """Return the directories that may hold ``pythonapi_<python-version>`` directories, without repeats.

    Three sources, because a half-configured host has only some of them. ``SOAM_HOME`` and its companions
    name the ``lib64`` directly. ``sys.path`` is read rather than ``PYTHONPATH`` because it is the superset:
    it carries the ``PYTHONPATH`` entries, the directory ``soamapiversion`` appends for the running
    interpreter, and anything a caller inserted, and under ``-E`` or ``-I`` it correctly omits a
    ``PYTHONPATH`` the interpreter is ignoring. ``LD_LIBRARY_PATH`` names the same ``lib64``, so it still
    finds the installation when only it was set.

    An entry may name the ``lib64`` or one ``pythonapi_<python-version>`` directory inside it, so each entry
    and its parent are searched.
    """
    roots = []

    soam_home = os.environ.get("SOAM_HOME")
    soam_version = os.environ.get("SOAM_VERSION")
    binary_type = os.environ.get("BINARY_TYPE")
    if soam_home and soam_version and binary_type:
        roots.append(os.path.join(soam_home, soam_version, binary_type, "lib64"))

    roots.extend(sys.path)
    roots.extend(os.environ.get("LD_LIBRARY_PATH", "").split(os.pathsep))

    directories: Dict[str, None] = {}
    for root in roots:
        if root:
            directories[root] = None
            directories[os.path.dirname(root)] = None

    return list(directories)


def _installed_versions_clause() -> str:
    versions = installed_python_api_versions()
    if not versions:
        return f"No {PYTHON_API_PREFIX}<python-version> directory was found."

    return f"This installation ships soamapi for Python {', '.join(versions)}."


def _running_python_version() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def _version_sort_key(version: str) -> Tuple[int, ...]:
    return tuple(int(part) if part.isdigit() else -1 for part in version.split("."))
