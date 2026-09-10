FROM ghcr.io/astral-sh/uv:latest AS uv
FROM ubuntu:24.04

COPY --from=uv /uv /usr/local/bin/uv

COPY scaler/worker_manager/nested/oci/remote/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

# entrypoint reads PYTHON_VERSION, PYTHON_REQUIREMENTS, and COMMAND from env vars
ENTRYPOINT ["/app/entrypoint.sh"]
