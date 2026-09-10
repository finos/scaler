#!/usr/bin/env python3
"""Deploy the Scaler service and register a Symphony application for the Scaler worker manager.

Run this on a host with an IBM Spectrum Symphony installation, once, before starting
``scaler_worker_manager symphony``. It packages ``scaler_service.py``, deploys it with ``soamdeploy``,
fills in ``application_profile.xml`` with every path resolved, and registers it with ``soamreg``. Both of
those files ship next to this script.

Paths are resolved and written into the profile rather than left as ``${VERSION_NUM}`` and
``${EGO_MACHINE_TYPE}``: Developer Edition leaves both of those empty, which silently produces broken
directories. The interpreter is checked by running it, not by matching version numbers against a table,
because Symphony's own ``soamapiversion`` decides which bytecode a given interpreter gets.

    python3 setup_application.py --python /opt/python3.12/bin/python3.12
    python3 setup_application.py --application MyApp --service MyService --dry-run
"""

import argparse
import dataclasses
import importlib.util
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

SERVICE_MODULE = "scaler_service.py"
# The service half of the output contract, deployed next to SERVICE_MODULE so that both halves read the
# tags from one definition. It lives in the scaler package because the worker manager imports it too.
TASK_OUTPUT_MODULE = "task_output.py"
TASK_OUTPUT_PACKAGE = "scaler.worker_manager.proxy.symphony"

DEFAULT_APPLICATION_NAME = "Scaler"
DEFAULT_SERVICE_NAME = "ScalerService"

APPLICATION_PROFILE_TEMPLATE = "application_profile.xml"


class SetupError(Exception):
    """A condition the operator has to fix, reported without a traceback."""


@dataclasses.dataclass(frozen=True)
class Installation:
    """The resolved locations inside one Symphony installation."""

    home: Path
    version: str
    binary_type: str

    @property
    def library_directory(self) -> Path:
        return self.home / self.version / self.binary_type / "lib64"

    @property
    def binary_directory(self) -> Path:
        return self.home / self.version / self.binary_type / "bin"


def main() -> int:
    arguments = _parse_arguments()

    try:
        installation = _resolve_installation(arguments.soam_home)
        _report("symphony", f"{installation.home} version {installation.version} ({installation.binary_type})")

        interpreter = _resolve_interpreter(arguments.python)
        bytecode_directory = _verify_interpreter(interpreter, installation)
        _report("interpreter", f"{interpreter} using {bytecode_directory.name}")

        profile = _render_application_profile(arguments, installation, interpreter, bytecode_directory)

        if arguments.dry_run:
            _report("dry run", "nothing was deployed or registered, the profile follows")
            print(profile)
            return 0

        _deploy_service(arguments, installation)
        _register_application(arguments, installation, profile)
        _report_registered_applications(installation)
    except SetupError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    # soamapi.connect() takes the application name, and the worker manager passes its --service-name
    # straight to it, so the flag has to be given the application rather than the service.
    _report(
        "done",
        f"start the worker manager with: scaler_worker_manager symphony <scheduler-address> "
        f"--service-name {arguments.application}",
    )
    return 0


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--application", default=DEFAULT_APPLICATION_NAME, help="Symphony application name to register"
    )
    parser.add_argument("--service", default=DEFAULT_SERVICE_NAME, help="Symphony service name to deploy")
    parser.add_argument(
        "--consumer", default=None, help="consumer path for the application (default: /<application>)"
    )
    parser.add_argument(
        "--python",
        default=None,
        help="interpreter that runs the service on the compute hosts (default: the interpreter running this script)",
    )
    parser.add_argument("--soam-home", default=None, help="Symphony installation root (default: $SOAM_HOME)")
    parser.add_argument(
        "--dry-run", action="store_true", help="print the application profile without deploying or registering"
    )

    arguments = parser.parse_args()
    if arguments.consumer is None:
        arguments.consumer = f"/{arguments.application}"

    return arguments


def _resolve_installation(soam_home_argument: Optional[str]) -> Installation:
    """Locate the installation, preferring what the Symphony profile exports over what has to be guessed."""
    soam_home = soam_home_argument or os.environ.get("SOAM_HOME")
    if not soam_home:
        raise SetupError("SOAM_HOME is not set, source $SOAM_HOME/conf/profile.soam or pass --soam-home")

    home = Path(soam_home)
    if not home.is_dir():
        raise SetupError(f"{home} is not a directory")

    # Developer Edition exports SOAM_VERSION and BINARY_TYPE, and leaves VERSION_NUM and EGO_MACHINE_TYPE
    # empty, so both spellings are tried before falling back to what is on disk.
    version = os.environ.get("SOAM_VERSION") or os.environ.get("VERSION_NUM")
    binary_type = os.environ.get("BINARY_TYPE") or os.environ.get("EGO_MACHINE_TYPE")
    if version and binary_type:
        installation = Installation(home, version, binary_type)
        if installation.library_directory.is_dir():
            return installation

    return _discover_installation(home)


def _discover_installation(home: Path) -> Installation:
    """Find the one ``<version>/<binary type>/lib64`` under ``home``, when the environment does not name it."""
    candidates = sorted(path for path in home.glob("*/*/lib64") if path.is_dir())
    if not candidates:
        raise SetupError(f"found no <version>/<binary-type>/lib64 directory under {home}")

    if len(candidates) > 1:
        listed = ", ".join(str(path) for path in candidates)
        raise SetupError(f"found more than one installation under {home} ({listed}), set SOAM_VERSION and BINARY_TYPE")

    library_directory = candidates[0]
    return Installation(home, library_directory.parent.parent.name, library_directory.parent.name)


def _resolve_interpreter(python_argument: Optional[str]) -> Path:
    if python_argument is None:
        return Path(sys.executable)

    resolved = shutil.which(python_argument) or python_argument
    interpreter = Path(resolved)
    if not interpreter.is_file():
        raise SetupError(f"{python_argument} is not an executable file")

    return interpreter


def _verify_interpreter(interpreter: Path, installation: Installation) -> Path:
    """Return the soamapi bytecode directory this interpreter resolves to, by running it.

    Symphony decides which bytecode an interpreter gets, so asking it is the only answer that cannot drift
    from a table written here. This also catches a service interpreter missing cloudpickle or tblib, which
    would otherwise fail later as an unexplained task failure.
    """
    library_directory = installation.library_directory
    if not library_directory.is_dir():
        raise SetupError(f"{library_directory} does not exist, check the Symphony installation")

    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(library_directory)
    environment["LD_LIBRARY_PATH"] = os.pathsep.join([str(library_directory), environment.get("LD_LIBRARY_PATH", "")])

    program = "import soamapiversion, soamapi, cloudpickle, tblib.pickling_support; print(soamapi.__file__)"
    completed = subprocess.run(
        [str(interpreter), "-c", program], env=environment, capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise SetupError(
            f"{interpreter} cannot run the service: {completed.stderr.strip()}\n"
            f"  soamapi bytecode present: {', '.join(_installed_bytecode_versions(library_directory)) or 'none'}\n"
            f"  the interpreter also needs cloudpickle and tblib (pip install opengris-scaler)"
        )

    return Path(completed.stdout.strip()).parent


def _installed_bytecode_versions(library_directory: Path) -> List[str]:
    """Return the bytecode directory names, ordered by version so 3.4 does not sort after 3.10."""
    directories = [path.name for path in library_directory.glob("pythonapi_*") if path.is_dir()]
    return sorted(directories, key=_version_sort_key)


def _version_sort_key(directory_name: str) -> Tuple[int, ...]:
    version = directory_name.partition("_")[2]
    return tuple(int(part) if part.isdigit() else -1 for part in version.split("."))


def _render_application_profile(
    arguments: argparse.Namespace, installation: Installation, interpreter: Path, bytecode_directory: Path
) -> str:
    profile = _script_directory_file(APPLICATION_PROFILE_TEMPLATE).read_text()

    replacements = {
        "@APPLICATION@": arguments.application,
        "@CONSUMER@": arguments.consumer,
        "@SERVICE@": arguments.service,
        "@SERVICE_MODULE@": SERVICE_MODULE,
        "@SOAM_VERSION@": installation.version,
        "@PYTHON@": str(interpreter),
        "@LIBRARY_DIRECTORY@": str(installation.library_directory),
        "@BYTECODE_DIRECTORY@": str(bytecode_directory),
    }

    for placeholder, value in replacements.items():
        profile = profile.replace(placeholder, value)

    return profile


def _script_directory_file(name: str) -> Path:
    """Return a file that ships next to this script, refusing rather than failing later on a partial copy."""
    path = Path(__file__).resolve().parent / name
    if not path.is_file():
        raise SetupError(f"{path} is missing, it ships next to this script")

    return path


def _installed_package_file(package: str, name: str) -> Path:
    """Return a file that ships inside the installed scaler package."""
    specification = importlib.util.find_spec(package)
    if specification is None or not specification.submodule_search_locations:
        raise SetupError(f"cannot find {package}, install scaler with 'pip install opengris-scaler'")

    path = Path(list(specification.submodule_search_locations)[0]) / name
    if not path.is_file():
        raise SetupError(f"{path} is missing from the installed {package}")

    return path


def _deploy_service(arguments: argparse.Namespace, installation: Installation) -> None:
    service_module = _script_directory_file(SERVICE_MODULE)
    task_output_module = _installed_package_file(TASK_OUTPUT_PACKAGE, TASK_OUTPUT_MODULE)

    with tempfile.TemporaryDirectory() as working_directory:
        package = Path(working_directory) / f"{arguments.service}.tar.gz"
        with tarfile.open(package, "w:gz") as archive:
            archive.add(service_module, arcname=SERVICE_MODULE)
            # Symphony puts the deployment directory first on the service's PYTHONPATH, so the service
            # imports this flat, as `task_output`, with no scaler package around it.
            archive.add(task_output_module, arcname=TASK_OUTPUT_MODULE)

        _report("packaged", f"{SERVICE_MODULE} and {TASK_OUTPUT_MODULE} into {package.name}")
        _run_symphony_command(
            installation,
            ["soamdeploy", "add", arguments.service, "-p", str(package), "-c", arguments.consumer, "-f"],
        )

    _report("deployed", f"service {arguments.service} to consumer {arguments.consumer}")


def _register_application(arguments: argparse.Namespace, installation: Installation, profile: str) -> None:
    with tempfile.TemporaryDirectory() as working_directory:
        profile_path = Path(working_directory) / f"{arguments.application}.xml"
        profile_path.write_text(profile)

        _run_symphony_command(installation, ["soamreg", str(profile_path)])

    _report("registered", f"application {arguments.application}")


def _report_registered_applications(installation: Installation) -> None:
    completed = _run_symphony_command(installation, ["soamview", "app"], check=False)
    if completed.stdout:
        print(completed.stdout.rstrip())


def _run_symphony_command(
    installation: Installation, command: List[str], check: bool = True
) -> subprocess.CompletedProcess:
    """Run one Symphony CLI command, finding it in the installation when the profile was not sourced."""
    executable = shutil.which(command[0])
    if executable is None:
        candidate = installation.binary_directory / command[0]
        if not candidate.is_file():
            raise SetupError(
                f"{command[0]} was not found on PATH or in {installation.binary_directory}, "
                f"source $SOAM_HOME/conf/profile.soam"
            )
        executable = str(candidate)

    completed = subprocess.run(
        [executable, *command[1:]], capture_output=True, text=True, check=False
    )
    if check and completed.returncode != 0:
        output = (completed.stdout + completed.stderr).strip()
        raise SetupError(f"{' '.join(command)} failed: {output}")

    return completed


def _report(stage: str, message: str) -> None:
    """Print progress, flushed so it stays ordered against anything written to stderr."""
    print(f"[{stage}] {message}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
