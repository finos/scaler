FROM python:3.12-slim

# Install OCI SDK and cloudpickle (the only runtime dependencies for the job runner)
RUN pip install --no-cache-dir cloudpickle oci

# Copy the job runner script into the image
COPY scaler/worker_manager/proxy/oci/remote/job_runner.py /app/job_runner.py

WORKDIR /app

# The job runner reads task configuration from environment variables set by the adapter
ENTRYPOINT ["python", "/app/job_runner.py"]
