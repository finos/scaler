#!/bin/sh
set -e

if [ -z "${COMMAND}" ]; then
    echo "ERROR: COMMAND environment variable is not set." >&2
    exit 1
fi

if [ -n "${PYTHON_VERSION}" ]; then
    uv python install "${PYTHON_VERSION}"
    uv venv --python "${PYTHON_VERSION}" /opt/opengris-scaler
fi

if [ -n "${PYTHON_REQUIREMENTS}" ]; then
    printf '%s\n' "${PYTHON_REQUIREMENTS}" > /tmp/requirements.txt

    # Source installs need C++ build dependencies for Scaler's native extensions.
    if grep -qE 'git\+|@ git\+' /tmp/requirements.txt; then
        apk add --no-cache \
            ca-certificates git cmake gcc g++ make pkgconf \
            capnproto capnproto-dev \
            libuv-dev openssl-dev
    fi

    uv pip install --no-cache -q --python /opt/opengris-scaler -r /tmp/requirements.txt
fi

if [ -d /opt/opengris-scaler/bin ]; then
    ln -sf /opt/opengris-scaler/bin/scaler /usr/local/bin/scaler 2>/dev/null || true
    ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/ 2>/dev/null || true
fi

echo "Executing: ${COMMAND}"
exec sh -c "${COMMAND}"
