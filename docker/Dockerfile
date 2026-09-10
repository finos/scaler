FROM ghcr.io/astral-sh/uv:latest AS uv
FROM alpine:3.23

RUN apk add --no-cache \
    ca-certificates cmake gcc g++ make pkgconf \
    capnproto capnproto-dev \
    libuv-dev openssl-dev

COPY --from=uv /uv /usr/local/bin/uv

ENV UV_CACHE_DIR="/tmp/uv-cache" \
    UV_PYTHON_INSTALL_DIR="/opt/python" \
    PATH="/opt/opengris-scaler/bin:${PATH}"

RUN uv python install 3.12 && \
    chmod -R g+rX /opt/python

COPY . /tmp/scaler-src

RUN uv venv --python 3.12 /opt/opengris-scaler && \
    uv pip install --no-cache -q --python /opt/opengris-scaler /tmp/scaler-src && \
    rm -rf /tmp/scaler-src /tmp/uv-cache

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

RUN adduser -D -u 1001 -G root scaler && \
    mkdir -p /app && \
    chmod g+rwx /app /opt/opengris-scaler

USER 1001

WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]
