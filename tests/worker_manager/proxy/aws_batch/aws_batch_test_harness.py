#!/usr/bin/env python3
"""
AWS HPC Worker Manager Test Harness.

Validates the AWS Batch worker manager by submitting tasks to a running scheduler.

Usage:
    python tests/aws_batch_test_harness.py --scheduler tcp://127.0.0.1:2345 --test all
    python tests/aws_batch_test_harness.py --scheduler tcp://127.0.0.1:2345 --test sqrt
"""

import argparse
import math
import sys

from scaler import Client

DEFAULT_TIMEOUT = 300  # 5 minutes (EC2 cold start can take 2-3 min)


def simple_task(x: int) -> int:
    return x * 2


def compute_task(n: int) -> float:
    total = 0.0
    for i in range(n):
        total += i * i * 0.01
    return total


def run_sqrt_test(client: Client, timeout: int) -> bool:
    """Test math.sqrt(16) -> 4.0"""
    print("\n--- Test: sqrt ---")
    print("  Submitting: math.sqrt(16)")
    try:
        future = client.submit(math.sqrt, 16)
        result = future.result(timeout=timeout)
        print(f"  Result: {result}")
        passed = result == 4.0
        print(f"  {'PASSED' if passed else 'FAILED: expected 4.0'}")
        return passed
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def run_simple_test(client: Client, timeout: int) -> bool:
    """Test simple_task(21) -> 42"""
    print("\n--- Test: simple ---")
    print("  Submitting: simple_task(21) [returns x * 2]")
    try:
        future = client.submit(simple_task, 21)
        result = future.result(timeout=timeout)
        print(f"  Result: {result}")
        passed = result == 42
        print(f"  {'PASSED' if passed else 'FAILED: expected 42'}")
        return passed
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def run_map_test(client: Client, timeout: int) -> bool:
    """Test client.map with 5 tasks"""
    print("\n--- Test: map ---")
    print("  Submitting: client.map(simple_task, [0,1,2,3,4])")
    try:
        results = client.map(simple_task, list(range(5)))
        print(f"  Results: {results}")
        expected = [0, 2, 4, 6, 8]
        passed = results == expected
        print(f"  {'PASSED' if passed else f'FAILED: expected {expected}'}")
        return passed
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def run_compute_test(client: Client, timeout: int) -> bool:
    """Test compute-intensive task"""
    print("\n--- Test: compute ---")
    print("  Submitting: compute_task(1000) [sum of i*i*0.01 for i in range(1000)]")
    try:
        future = client.submit(compute_task, 1000)
        result = future.result(timeout=timeout)
        print(f"  Result: {result:.2f}")
        passed = 3000000 < result < 4000000
        print(f"  {'PASSED' if passed else 'FAILED: result out of expected range'}")
        return passed
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def square(value: int) -> int:
    return value * value


def run_map_100_test(client: Client, timeout: int) -> bool:
    """Test client.map with 100 tasks using array jobs"""
    count = 100
    print("\n--- Test: map_100 ---")
    print(f"  Submitting: client.map(square, range({count}))")
    try:
        import time

        start = time.time()
        results = client.map(square, list(range(count)))
        elapsed = time.time() - start
        expected = [i * i for i in range(count)]
        passed = results == expected
        avg = elapsed / count
        print(f"  Results: {len(results)} values, first 5: {results[:5]}")
        print(f"  Total time: {elapsed:.1f}s, avg: {avg:.2f}s/task")
        if avg > 2.0:
            print("  WARNING: avg > 2s/task, array jobs may not be working")
        print(f"  {'PASSED' if passed else 'FAILED: results mismatch'}")
        return passed
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


TESTS = {
    "sqrt": run_sqrt_test,
    "simple": run_simple_test,
    "map": run_map_test,
    "compute": run_compute_test,
    "map_100": run_map_100_test,
}


def main():
    parser = argparse.ArgumentParser(description="AWS HPC Worker Manager Test Harness")
    parser.add_argument("--scheduler", default="tcp://127.0.0.1:2345", help="Scheduler address")
    parser.add_argument("--test", default="all", choices=["all"] + list(TESTS.keys()), help="Test to run")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Timeout per task (seconds)")
    args = parser.parse_args()

    print("=" * 50)
    print("AWS HPC Worker Manager Test Harness")
    print("=" * 50)
    print(f"Scheduler: {args.scheduler}")

    # Display AWS credentials in effect
    import boto3

    try:
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        print(f"AWS Account: {identity['Account']}")
        print(f"AWS User/Role: {identity['Arn']}")
    except Exception as e:
        print(f"AWS Credentials: Unable to verify ({e})")

    try:
        with Client(address=args.scheduler) as client:
            print("Connected to scheduler")

            tests_to_run = list(TESTS.keys()) if args.test == "all" else [args.test]
            passed = sum(1 for t in tests_to_run if TESTS[t](client, args.timeout))

            print("\n" + "=" * 50)
            print(f"Results: {passed}/{len(tests_to_run)} passed")
            sys.exit(0 if passed == len(tests_to_run) else 1)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
