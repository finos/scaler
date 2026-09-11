FROM python:3.12-slim

# Install dependencies needed for batch job runner
RUN pip install --no-cache-dir cloudpickle boto3

# Copy only the batch job runner script
COPY scaler/worker_manager/proxy/aws_batch/remote/job_runner.py /app/job_runner.py

WORKDIR /app

# Run as a script, not a module
ENTRYPOINT ["python", "/app/job_runner.py"]
