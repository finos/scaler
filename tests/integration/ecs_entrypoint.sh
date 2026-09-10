#!/bin/bash
#
# ECS task entrypoint for the floci-backed ECS e2e. The shipped ECSWorkerProvisioner delivers the worker
# manager command through the COMMAND env var (not the container command) and any extra dependencies
# through PYTHON_REQUIREMENTS. The scaler wheel is already baked into the base image (worker.Dockerfile),
# so PYTHON_REQUIREMENTS is left empty by the test and PYTHON_VERSION is ignored -- there is nothing to
# install at task start.
set -euo pipefail

if [ -n "${PYTHON_REQUIREMENTS:-}" ]; then
    uv pip install --python /opt/scaler-venv $(echo "${PYTHON_REQUIREMENTS}" | tr ';' ' ')
fi

if [ -z "${COMMAND:-}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

exec bash -c "${COMMAND}"
