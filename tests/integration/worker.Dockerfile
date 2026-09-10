# Self-contained worker image for the e2e scaling tests (see README.md).
#
# It carries the exact wheel the host just built plus the custom-built capnp/kj shared libs the
# scaler C++ extensions link against, so a container runs the same scaler build and wire protocol
# as the host scheduler -- with none of the host-layout coupling a bind-mount would need.
#
# Built by docker.py, which stages the wheel under wheel/ and the libs
# under libs/. BASE_IMAGE must have a glibc at least as new as the host that built the wheel/libs
# (hence ubuntu:26.04 by default, to match the dev host); PYTHON_VERSION must match the wheel ABI.
ARG BASE_IMAGE=ubuntu:26.04
FROM ${BASE_IMAGE}

ARG PYTHON_VERSION=3.13

# ca-certificates: uv fetches the interpreter and wheel dependencies over HTTPS.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# uv supplies the exact CPython matching the wheel ABI, independent of the base image's Python.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Custom-built capnp/kj runtime libs (not distro packages); ymq statically links libuv so it is absent.
COPY libs/ /usr/local/lib/
RUN ldconfig

COPY wheel/ /tmp/wheel/
RUN uv venv --python "${PYTHON_VERSION}" /opt/scaler-venv \
 && uv pip install --python /opt/scaler-venv /tmp/wheel/*.whl \
 && rm -rf /tmp/wheel

# scaler_worker_manager resolves on PATH; the extensions find their libs via LD_LIBRARY_PATH.
ENV PATH="/opt/scaler-venv/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib"
