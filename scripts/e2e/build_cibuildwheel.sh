#!/bin/bash
#
# Build portable (manylinux/musllinux/macOS/Windows) wheels with cibuildwheel, reading the shared
# [tool.cibuildwheel] config in pyproject.toml -- the exact build the release pipeline runs
# (.github/actions/create-artifacts). Factored out here so it can be run locally and reused by tests that
# need a glibc-portable wheel of the CURRENT source (the plain `python -m build` wheel is linux_x86_64 and
# does not run on older-glibc targets like Amazon Linux 2023).
#
# Usage:
#   ./scripts/e2e/build_cibuildwheel.sh                                  # all non-skipped targets (release parity)
#   ./scripts/e2e/build_cibuildwheel.sh --only cp313-manylinux_x86_64    # one target (fast; e.g. for an e2e)
#   CIBW_OUTPUT_DIR=dist_manylinux ./scripts/e2e/build_cibuildwheel.sh --only cp313-manylinux_x86_64
#
# Needs Docker (Linux wheels build inside a manylinux container). Where the socket is root-only, pass the
# sudo invocation via DOCKER and this script shims it onto PATH for cibuildwheel:
#   DOCKER="sudo docker" ./scripts/e2e/build_cibuildwheel.sh --only cp313-manylinux_x86_64
set -euo pipefail

PYTHON="${PYTHON:-python}"
OUTPUT_DIR="${CIBW_OUTPUT_DIR:-dist}"

# Match the release pipeline's toolchain (uv, not pip -- uv-created venvs ship no pip). uv refuses to
# install into a non-virtual environment without --system: local runs use an activated venv, while the
# release/CI lane has none and installs into the system interpreter that `python -m cibuildwheel` runs below.
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
    uv pip install --python "${PYTHON}" --quiet --upgrade cibuildwheel
else
    uv pip install --system --quiet --upgrade cibuildwheel
fi

# cibuildwheel invokes `docker` directly; where that needs sudo, expose a shim named `docker` on PATH.
DOCKER="${DOCKER:-docker}"
if [[ "${DOCKER}" != "docker" ]]; then
    shim_dir="$(mktemp -d)"
    printf '#!/bin/bash\nexec %s "$@"\n' "${DOCKER}" > "${shim_dir}/docker"
    chmod +x "${shim_dir}/docker"
    export PATH="${shim_dir}:${PATH}"
    trap 'rm -rf "${shim_dir}"' EXIT
fi

exec "${PYTHON}" -m cibuildwheel --output-dir "${OUTPUT_DIR}" "$@"
