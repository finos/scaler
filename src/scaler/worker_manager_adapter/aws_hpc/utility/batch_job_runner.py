"""
AWS Batch Job Runner.

Runs inside AWS Batch containers to execute Scaler tasks.
Handles both inline (job parameters) and S3-based payloads,
with optional gzip compression.

This is a standalone script that doesn't depend on the full scaler package.
It only needs: cloudpickle, boto3
"""

import argparse
import base64
import gzip
import logging
import os
import signal
import sys
import traceback

import boto3
import cloudpickle

logger = logging.getLogger(__name__)

COMPRESSION_THRESHOLD_BYTES: int = 4096


def signal_handler(signum, frame):
    """Handle signals to log before crash."""
    sig_name = signal.Signals(signum).name
    logger.error(f"Received signal {sig_name} ({signum})")
    sys.stdout.flush()
    sys.exit(128 + signum)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout, force=True
    )
    # Ensure stdout is unbuffered for immediate log visibility
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


def parse_args() -> dict:
    """Parse command line arguments from job parameters."""
    parser = argparse.ArgumentParser(description="AWS Batch Job Runner for Scaler")
    parser.add_argument("--task_id", default="unknown", help="Task ID")
    parser.add_argument("--s3_bucket", default=None, help="S3 bucket for results")
    parser.add_argument("--s3_prefix", default="scaler-tasks", help="S3 prefix for results")
    parser.add_argument("--compressed", default="0", help="Whether payload is compressed ('0' or '1')")
    parser.add_argument("--payload", default=None, help="Inline base64-encoded payload")
    parser.add_argument("--s3_key", default=None, help="S3 key for payload")

    args = vars(parser.parse_args())

    # Map of "none" handling: if value is "none", use this value instead
    none_mapping = {
        "task_id": "unknown",
        "s3_bucket": None,
        "s3_prefix": "scaler-tasks",
        "payload": None,
        "s3_key": None,
    }

    for key, fallback in none_mapping.items():
        val = args.get(key)
        if isinstance(val, str) and val.lower() == "none":
            args[key] = fallback

    return args


def get_payload(args: dict) -> bytes:
    """Fetch task payload from job parameters or S3.

    For array jobs, the s3_key contains 'ARRAY_INDEX' which is replaced
    with the actual AWS_BATCH_JOB_ARRAY_INDEX environment variable.
    """
    compressed = args.get("compressed", "0") == "1"
    payload_b64 = args.get("payload")
    s3_key = args.get("s3_key")
    s3_bucket = args.get("s3_bucket")

    # Handle array job index substitution
    array_index = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    if s3_key and "ARRAY_INDEX" in s3_key and array_index is not None:
        s3_key = s3_key.replace("ARRAY_INDEX", array_index)
        logger.info(f"Array job index {array_index}, resolved S3 key: {s3_key}")

    # Try inline payload from job parameters
    if payload_b64:
        payload = base64.b64decode(payload_b64)
        if compressed:
            payload = gzip.decompress(payload)
        return payload

    # Fall back to S3
    if not s3_bucket or not s3_key:
        raise ValueError("No payload: need --payload or --s3_key")

    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    payload = response["Body"].read()

    if compressed or s3_key.endswith(".gz"):
        payload = gzip.decompress(payload)

    # Cleanup input from S3
    try:
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    except Exception as e:
        logger.warning(f"Failed to cleanup S3 input: {e}")

    return payload


def store_result(result: bytes, s3_bucket: str, s3_prefix: str, job_id: str):
    """Store result to S3."""
    # Compress if beneficial
    compressed = False
    if len(result) > COMPRESSION_THRESHOLD_BYTES:
        result = gzip.compress(result)
        compressed = True

    result_key = f"{s3_prefix}/results/{job_id}.pkl"
    if compressed:
        result_key += ".gz"

    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=s3_bucket, Key=result_key, Body=result)

    logger.info(f"Result stored to s3://{s3_bucket}/{result_key}")
    return result_key


def main():
    setup_logging()

    args = parse_args()
    task_id = args.get("task_id", "unknown")
    s3_bucket = args.get("s3_bucket")
    s3_prefix = args.get("s3_prefix", "scaler-tasks")
    job_id = os.environ.get("AWS_BATCH_JOB_ID", task_id)

    logger.info(f"Starting task {task_id[:8]}...")
    logger.info(f"Args: task_id={task_id[:8]}, s3_bucket={s3_bucket}, s3_prefix={s3_prefix}")

    try:
        # Get payload
        payload_bytes = get_payload(args)
        task_data = cloudpickle.loads(payload_bytes)

        logger.info(f"Task data loaded, keys: {list(task_data.keys())}")

        # task_data contains:
        # - task_id: hex string
        # - source: hex string
        # - func_object_id: hex string (reference to serialized function in object storage)
        # - args: list of {"type": "objectID"|"task", "data": hex string}

        # For the batch job runner, we need to:
        # 1. The function and args should be embedded in the payload for standalone execution
        # 2. Or we need to fetch from object storage (requires more setup)

        # For now, check if we have embedded function/args
        if "function" in task_data and "arguments" in task_data:
            # Direct execution mode - function and args are in payload
            func = task_data["function"]
            arguments = task_data["arguments"]

            logger.info(f"Executing function with {len(arguments)} arguments")
            sys.stdout.flush()

            logger.info(f"Function type: {type(func)}, name: {getattr(func, '__name__', 'unknown')}")
            logger.info(f"Arguments: {arguments}")

            # Log memory before execution
            try:
                import resource

                usage = resource.getrusage(resource.RUSAGE_SELF)  # type: ignore[attr-defined, unused-ignore]
                mem_before = usage.ru_maxrss
                logger.info(f"Memory before execution: {mem_before} KB")
            except Exception:
                pass
            sys.stdout.flush()

            try:
                logger.info("Calling function now...")
                sys.stdout.flush()
                result = func(*arguments)
                sys.stdout.flush()
                logger.info(f"Function completed, result type: {type(result).__name__}, value: {result}")
                sys.stdout.flush()
            except Exception as func_error:
                logger.error(f"Function execution failed: {func_error}")
                traceback.print_exc()
                sys.stdout.flush()
                raise
        else:
            # Reference mode - need object storage access
            # This requires the worker manager to embed the actual function/args
            raise ValueError("Task data missing 'function' and 'arguments' - reference mode not yet supported")

        # Serialize and store result
        logger.info("Serializing result...")
        result_bytes = cloudpickle.dumps(result)
        logger.info(f"Result serialized, size: {len(result_bytes)} bytes")

        logger.info("Storing result to S3...")
        store_result(result_bytes, s3_bucket, s3_prefix, job_id)

        logger.info(f"Task {task_id[:8]} completed successfully")

    except Exception as e:
        logger.error(f"Task {task_id[:8]} failed: {e}")
        traceback.print_exc()

        # Store error result
        try:
            error_result = {"error": str(e), "traceback": traceback.format_exc()}
            store_result(cloudpickle.dumps(error_result), s3_bucket, s3_prefix, job_id)
        except Exception as store_error:
            logger.error(f"Failed to store error result: {store_error}")

        sys.exit(1)


if __name__ == "__main__":
    main()
