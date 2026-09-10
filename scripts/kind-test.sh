#!/usr/bin/env bash
# scripts/kind-test.sh — KinD integration tests for the k8s_raw worker manager.
#
# This script assumes scaler is already installed in the active Python
# environment (the wheel has been built and installed).  In CI that is handled
# by the preceding workflow steps; locally use the devcontainer or build the
# wheel the same way CI does:
#
#   ./scripts/library_tool.sh capnp   download && compile && install
#   ./scripts/library_tool.sh libuv   download && compile && install
#   ./scripts/library_tool.sh openssl download && compile && install
#   python -m build --wheel
#   pip install dist/*.whl[kubernetes] pytest pytest-timeout
#
# Usage:
#   ./scripts/kind-test.sh                  # full run (build image + create cluster + test)
#   ./scripts/kind-test.sh --no-build       # skip docker image build
#   ./scripts/kind-test.sh --no-cluster     # reuse an existing KinD cluster
#   ./scripts/kind-test.sh --keep           # leave cluster up after tests
#
# Environment overrides:
#   SCALER_KIND_CLUSTER        KinD cluster name      (default: scaler-test)
#   SCALER_KIND_IMAGE          worker image tag        (default: scaler-worker:kind-test)
#   SCALER_KIND_NAMESPACE      Kubernetes namespace    (default: scaler-test)

set -uo pipefail

CLUSTER_NAME="${SCALER_KIND_CLUSTER:-scaler-test}"
IMAGE_TAG="${SCALER_KIND_IMAGE:-scaler-worker:kind-test}"
NAMESPACE="${SCALER_KIND_NAMESPACE:-scaler-test}"
KUBECONFIG_PATH="${TMPDIR:-/tmp}/scaler-kind-${CLUSTER_NAME}-${UID}.kubeconfig"
THIRDPARTY_PREFIX="${SCALER_THIRDPARTY_PREFIX:-/tmp/opengris-thirdparties}"
DO_BUILD=1
DO_CLUSTER=1
KEEP_CLUSTER=0

for arg in "$@"; do
    case "$arg" in
        --no-build)   DO_BUILD=0 ;;
        --no-cluster) DO_CLUSTER=0 ;;
        --keep)       KEEP_CLUSTER=1 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAILED=0

# Activate the repo venv if present so all python/pip calls use it.
if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.venv/bin/activate"
fi

# If the 3rd-party libs were built locally, ensure they are on the library path.
# This mirrors what the CI setup-env action exports via GITHUB_ENV.
if [[ -d "${THIRDPARTY_PREFIX:-/tmp/opengris-thirdparties}/lib" ]]; then
    export LD_LIBRARY_PATH="${THIRDPARTY_PREFIX:-/tmp/opengris-thirdparties}/lib:${LD_LIBRARY_PATH:-}"
fi

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; RESET='\033[0m'
step() { echo -e "\n${BOLD}[step]${RESET} $*"; }
ok()   { echo -e "  ${GREEN}✓${RESET} $*"; }
warn() { echo -e "  ${YELLOW}⚠${RESET}  $*"; }
fail() { echo -e "  ${RED}✗${RESET} $*" >&2; }
die()  { FAILED=1; fail "$*"; exit 1; }

cleanup() {
    echo ""
    if [[ $KEEP_CLUSTER -eq 0 ]]; then
        step "Teardown: deleting KinD cluster '${CLUSTER_NAME}'..."
        kind delete cluster --name "${CLUSTER_NAME}" 2>/dev/null \
            && ok "Cluster deleted" || warn "Cluster already gone"
    else
        warn "Keeping cluster '${CLUSTER_NAME}'. Remove with: kind delete cluster --name ${CLUSTER_NAME}"
    fi
    if [[ $FAILED -ne 0 ]]; then
        echo ""
        fail "Tests failed. Useful debug commands:"
        echo -e "  kubectl --kubeconfig ${KUBECONFIG_PATH} get pods -n ${NAMESPACE}"
        echo -e "  kubectl --kubeconfig ${KUBECONFIG_PATH} logs -n ${NAMESPACE} -l app=scaler-worker"
        exit 1
    fi
}
trap cleanup EXIT

CLI=$(command -v docker 2>/dev/null || command -v podman 2>/dev/null || true)

# ── Step 1: check prerequisites ─────────────────────────────────────────────
step "Checking prerequisites..."
missing=()
for cmd in kind kubectl python; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
done
[[ -n "$CLI" ]] || missing+=("docker or podman")
[[ ${#missing[@]} -eq 0 ]] || { fail "Missing: ${missing[*]}"; exit 1; }

python -c "from scaler import Client" 2>/dev/null \
    || die "scaler not importable — build and install the wheel first (see script header)"
python -c "from scaler.config.section.kubernetes_worker_manager import KubernetesWorkerManagerConfig" 2>/dev/null \
    || die "k8s_raw not found — is this branch installed? Rebuild the wheel."
python -c "import kubernetes" 2>/dev/null \
    || die "kubernetes client missing — pip install 'opengris-scaler[kubernetes]'"

ok "kind:    $(kind version 2>&1 | head -1)"
ok "kubectl: $(kubectl version --client 2>&1 | head -1)"
ok "docker:  $CLI"
ok "python:  $(python --version) at $(command -v python)"
ok "scaler:  $(python -c 'import scaler; print(scaler.__version__)' 2>/dev/null || echo 'ok')"

# ── Step 2: build worker image ───────────────────────────────────────────────
if [[ $DO_BUILD -eq 1 ]]; then
    step "Building worker image '${IMAGE_TAG}'..."
    "$CLI" build \
        --file "${REPO_ROOT}/docker/Dockerfile" \
        --tag  "${IMAGE_TAG}" \
        "${REPO_ROOT}" \
        && ok "Image built" \
        || die "Image build failed"
else
    step "Skipping image build — using existing '${IMAGE_TAG}'"
fi

# ── Step 3: create KinD cluster ──────────────────────────────────────────────
if [[ $DO_CLUSTER -eq 1 ]]; then
    step "Creating KinD cluster '${CLUSTER_NAME}'..."
    kind create cluster \
        --name "${CLUSTER_NAME}" \
        --config "${REPO_ROOT}/scripts/kind-config.yaml" \
        --kubeconfig "${KUBECONFIG_PATH}" \
        --wait 60s \
        && ok "Cluster ready" \
        || die "Cluster creation failed"
else
    step "Reusing KinD cluster '${CLUSTER_NAME}'..."
    kind get kubeconfig --name "${CLUSTER_NAME}" > "${KUBECONFIG_PATH}"
    ok "Kubeconfig written to ${KUBECONFIG_PATH}"
fi

# ── Step 4: load image into cluster ─────────────────────────────────────────
step "Loading image '${IMAGE_TAG}' into cluster..."
kind load docker-image "${IMAGE_TAG}" --name "${CLUSTER_NAME}" \
    && ok "Image loaded" \
    || die "Image load failed — is '${IMAGE_TAG}' built? Run without --no-build."

# ── Step 5: create test namespace ───────────────────────────────────────────
step "Ensuring namespace '${NAMESPACE}'..."
kubectl --kubeconfig "${KUBECONFIG_PATH}" \
    create namespace "${NAMESPACE}" --dry-run=client -o yaml \
    | kubectl --kubeconfig "${KUBECONFIG_PATH}" apply -f - \
    && ok "Namespace ready" \
    || die "Namespace creation failed"

# ── Step 6: detect host IP reachable from KinD pods ─────────────────────────
step "Detecting host IP reachable from pods..."
KIND_BRIDGE_IP=""

KIND_BRIDGE_IP=$("$CLI" network inspect kind \
    --format '{{range .IPAM.Config}}{{.Gateway}}{{end}}' 2>/dev/null \
    | tr ' ' '\n' | grep -E '^[0-9]+\.' | head -1 || true)

if [[ -z "${KIND_BRIDGE_IP}" ]]; then
    KIND_BRIDGE_IP=$(kubectl --kubeconfig "${KUBECONFIG_PATH}" \
        get node "${CLUSTER_NAME}-control-plane" \
        -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null \
        | awk -F'.' '{print $1"."$2"."$3".1"}' || true)
fi

if [[ -z "${KIND_BRIDGE_IP}" ]]; then
    KIND_BRIDGE_IP=$(ip route get 1.1.1.1 2>/dev/null \
        | awk '/src/{for(i=1;i<=NF;i++) if ($i=="src") print $(i+1)}' | head -1 || true)
fi

[[ -n "${KIND_BRIDGE_IP}" ]] \
    || { warn "Could not detect bridge IP; using 172.18.0.1"; KIND_BRIDGE_IP="172.18.0.1"; }
ok "Pods will reach scheduler at ${KIND_BRIDGE_IP}"

# ── Step 7: run tests ────────────────────────────────────────────────────────
step "Running KinD integration tests..."
echo ""

TEST_EXIT=0
SCALER_KIND_TESTS=1 \
SCALER_KIND_IMAGE="${IMAGE_TAG}" \
SCALER_KIND_KUBECONFIG="${KUBECONFIG_PATH}" \
SCALER_KIND_NAMESPACE="${NAMESPACE}" \
SCALER_SCHEDULER_HOST="${KIND_BRIDGE_IP}" \
    python -m unittest discover \
        -v \
        -s "${REPO_ROOT}/tests/worker_manager_adapter/kubernetes" \
        -p "test_kind_integration.py" \
        -t "${REPO_ROOT}" \
    || TEST_EXIT=$?

echo ""
if [[ $TEST_EXIT -eq 0 ]]; then
    ok "All KinD integration tests passed ✓"
else
    FAILED=1
    fail "Tests failed (exit code ${TEST_EXIT})"
fi
