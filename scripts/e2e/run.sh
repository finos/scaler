#!/bin/bash
#
# Run one e2e scaling topology locally (see tests/integration/README.md).
#
# Usage:
#       ./scripts/e2e/run.sh <topology>            container | container_waterfall | ecs | ec2 | ecs_ec2
#       DOCKER="sudo docker" ./scripts/e2e/run.sh ecs        # if the docker socket is root-only
#
# The harness prints a "web GUI: http://localhost:PORT" line you can open to watch the scaling live.
#
# Tunables, same as the workflow_dispatch inputs:
#       SCALER_IT_NUM_TASKS         tasks per burst wave
#       SCALER_IT_TASK_SECONDS      sleep per task, seconds
#       SCALER_IT_WATERFALL_POLICY  raw waterfall_v1 policy for the waterfall topologies, one rule per
#                                   line: "priority,worker_manager_id[,cap]" (ids: wm-<backend>-p<position>)
#   e.g. SCALER_IT_NUM_TASKS=60 SCALER_IT_TASK_SECONDS=0.3 ./scripts/e2e/run.sh container
#
# Wheels are built if missing. After changing PRODUCT code, delete dist/*.whl (and, for the EC2
# topologies, dist_manylinux/*.whl) so the workers run the new build.
set -euo pipefail

TOPOLOGY="${1:-}"
case "${TOPOLOGY}" in
    container|container_waterfall|ecs|ec2|ecs_ec2) ;;
    *) echo "usage: $0 <container|container_waterfall|ecs|ec2|ecs_ec2>" >&2; exit 2 ;;
esac

DOCKER="${DOCKER:-docker}"
PYTHON="${PYTHON:-python}"
WHEEL_DIR="${SCALER_IT_MANYLINUX_WHEEL_DIR:-dist_manylinux}"

if ! ls dist/*.whl >/dev/null 2>&1; then
    echo "No wheel under dist/; building one (recompiles the C++ extensions, may take a few minutes)..."
    "${PYTHON}" -m build --wheel
fi

# The EC2 topologies boot Amazon Linux instances, whose older glibc cannot load the plain dist/ wheel.
if [[ "${TOPOLOGY}" == "ec2" || "${TOPOLOGY}" == "ecs_ec2" ]] && ! ls "${WHEEL_DIR}"/*manylinux*.whl >/dev/null 2>&1; then
    cpython_tag="cp$(${PYTHON} -c 'import sys; print(f"{sys.version_info.major}{sys.version_info.minor}")')"
    echo "No manylinux wheel under ${WHEEL_DIR}; building ${cpython_tag} with cibuildwheel (several minutes)..."
    DOCKER="${DOCKER}" PYTHON="${PYTHON}" CIBW_OUTPUT_DIR="${WHEEL_DIR}" \
        ./scripts/e2e/build_cibuildwheel.sh --only "${cpython_tag}-manylinux_x86_64"
fi

export SCALER_E2E="${TOPOLOGY}"
export SCALER_IT_CONTAINER_CLI="${SCALER_IT_CONTAINER_CLI:-${DOCKER}}"
export SCALER_IT_MANYLINUX_WHEEL_DIR="${WHEEL_DIR}"
# Rebuild the worker image so a wheel change is never silently run against a stale image.
export SCALER_IT_REBUILD=1

exec "${PYTHON}" -m unittest tests.integration.test_scaling -v
