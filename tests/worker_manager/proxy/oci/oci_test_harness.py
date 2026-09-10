#!/usr/bin/env python3
"""
OCI HPC Worker Adapter Test Harness.

Validates the OCI Container Instance worker adapter by running a series of
checks against a live OCI tenancy and (optionally) a running Scaler scheduler.

Phases:
    1. OCI connectivity    — verify auth, compartment, subnet, ADs
    2. Object Storage      — write / read / delete a test object
    3. Container Instance  — launch a minimal container, poll to completion, clean up
    4. Scheduler tasks     — submit Scaler tasks through a running scheduler (requires --scheduler)

Usage:
    # Full infrastructure validation (no scheduler required):
    python tests/worker_manager/proxy/oci/oci_test_harness.py \\
        --compartment-id ocid1.compartment.oc1..aaa... \\
        --subnet-id      ocid1.subnet.oc1.iad.aaa... \\
        --availability-domain AD-1 \\
        --container-image iad.ocir.io/namespace/repo:latest

    # End-to-end with a scheduler:
    python tests/worker_manager/proxy/oci/oci_test_harness.py \\
        --compartment-id ocid1.compartment.oc1..aaa... \\
        --subnet-id      ocid1.subnet.oc1.iad.aaa... \\
        --availability-domain AD-1 \\
        --container-image iad.ocir.io/namespace/repo:latest \\
        --scheduler tcp://127.0.0.1:2345 \\
        --test all

    # Run from a provisioner config file:
    python tests/worker_manager/proxy/oci/oci_test_harness.py \\
        --config tests/worker_manager/proxy/oci/.scaler_oci_config.json \\
        --test all
"""

import argparse
import json
import math
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import oci  # noqa: F401
except ImportError:
    print("ERROR: The 'oci' Python SDK is not installed.")
    print("Install it with:  pip install oci")
    sys.exit(1)

from scaler import Client

# ---------------------------------------------------------------------------
# OCI poll settings
# ---------------------------------------------------------------------------
_CI_POLL_INTERVAL_SECONDS = 5
_CI_TIMEOUT_SECONDS = 300  # 5 min for container instance lifecycle

# Scaler task timeout (OCI cold start can take 1-3 min)
_SCHEDULER_TASK_TIMEOUT = 600

# ---------------------------------------------------------------------------
# Test functions (must be picklable — top-level module functions)
# ---------------------------------------------------------------------------


def simple_task(x: int) -> int:
    return x * 2


def compute_task(n: int) -> float:
    total = 0.0
    for i in range(n):
        total += i * i * 0.01
    return total


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_availability_domain(profile: str, compartment_id: str, ad_name: str) -> str:
    """
    Resolve a short AD name (e.g. "AD-1") to the full OCI name
    (e.g. "Uocm:PHX-AD-1"). If the name is already fully qualified, return it as-is.
    """
    if ":" in ad_name:
        return ad_name  # already fully qualified

    config = oci.config.from_file(profile_name=profile)
    identity = oci.identity.IdentityClient(config)
    ads = identity.list_availability_domains(compartment_id=config["tenancy"]).data
    for ad in ads:
        if ad_name in ad.name or ad.name.endswith(ad_name):
            return ad.name

    # Return as-is and let the API give a clear error
    return ad_name


def _print_header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _print_result(name: str, passed: bool, detail: str = "") -> None:
    status = "PASSED" if passed else "FAILED"
    suffix = f" — {detail}" if detail else ""
    print(f"  [{status}] {name}{suffix}")


# ---------------------------------------------------------------------------
# Phase 1 — OCI Connectivity
# ---------------------------------------------------------------------------


def check_oci_auth(profile: str) -> bool:
    """Verify OCI SDK authentication and print tenancy info."""
    print("\n--- Check: OCI Authentication ---")
    try:
        config = oci.config.from_file(profile_name=profile)
        identity = oci.identity.IdentityClient(config)
        tenancy = identity.get_tenancy(tenancy_id=config["tenancy"]).data
        print(f"  Tenancy:  {tenancy.name} ({config['tenancy'][:30]}...)")
        print(f"  Region:   {config.get('region', 'not set')}")
        print(f"  User:     {config.get('user', 'not set')[:40]}...")
        _print_result("OCI Authentication", True)
        return True
    except Exception as exc:
        _print_result("OCI Authentication", False, str(exc))
        return False


def check_compartment(profile: str, compartment_id: str) -> bool:
    """Verify the compartment exists and is active."""
    print("\n--- Check: Compartment ---")
    try:
        config = oci.config.from_file(profile_name=profile)
        identity = oci.identity.IdentityClient(config)
        compartment = identity.get_compartment(compartment_id=compartment_id).data
        active = compartment.lifecycle_state == "ACTIVE"
        print(f"  Name:   {compartment.name}")
        print(f"  State:  {compartment.lifecycle_state}")
        _print_result("Compartment", active, compartment_id[:40])
        return active
    except Exception as exc:
        _print_result("Compartment", False, str(exc))
        return False


def check_subnet(profile: str, subnet_id: str) -> bool:
    """Verify the subnet exists and is available."""
    print("\n--- Check: Subnet ---")
    try:
        config = oci.config.from_file(profile_name=profile)
        vn_client = oci.core.VirtualNetworkClient(config)
        subnet = vn_client.get_subnet(subnet_id=subnet_id).data
        available = subnet.lifecycle_state == "AVAILABLE"
        print(f"  Name:   {subnet.display_name}")
        print(f"  CIDR:   {subnet.cidr_block}")
        print(f"  State:  {subnet.lifecycle_state}")
        _print_result("Subnet", available, subnet_id[:40])
        return available
    except Exception as exc:
        _print_result("Subnet", False, str(exc))
        return False


def check_availability_domain(profile: str, compartment_id: str, ad_name: str) -> bool:
    """Verify the availability domain exists."""
    print("\n--- Check: Availability Domain ---")
    try:
        config = oci.config.from_file(profile_name=profile)
        identity = oci.identity.IdentityClient(config)
        ads = identity.list_availability_domains(compartment_id=config["tenancy"]).data
        ad_names = [ad.name for ad in ads]
        # Accept both short form ("AD-1") and full form ("Uocm:PHX-AD-1")
        found = any(ad_name in name or name == ad_name for name in ad_names)
        print(f"  Available ADs: {ad_names}")
        print(f"  Requested:     {ad_name}")
        _print_result("Availability Domain", found)
        return found
    except Exception as exc:
        _print_result("Availability Domain", False, str(exc))
        return False


# ---------------------------------------------------------------------------
# Phase 2 — Object Storage
# ---------------------------------------------------------------------------


def check_object_storage(profile: str, compartment_id: str) -> bool:
    """Write, read, and delete a test object in a temporary bucket."""
    print("\n--- Check: Object Storage Read/Write ---")
    try:
        config = oci.config.from_file(profile_name=profile)
        os_client = oci.object_storage.ObjectStorageClient(config)
        namespace = os_client.get_namespace().data
        print(f"  Namespace: {namespace}")

        bucket_name = f"scaler-test-{uuid.uuid4().hex[:8]}"
        object_name = "harness-test-object.txt"
        test_payload = b"scaler-oci-harness-test"

        # Create bucket
        os_client.create_bucket(
            namespace_name=namespace,
            create_bucket_details=oci.object_storage.models.CreateBucketDetails(
                name=bucket_name, compartment_id=compartment_id, public_access_type="NoPublicAccess"
            ),
        )
        print(f"  Created bucket: {bucket_name}")

        # Put object
        os_client.put_object(
            namespace_name=namespace, bucket_name=bucket_name, object_name=object_name, put_object_body=test_payload
        )
        print(f"  Put object: {object_name} ({len(test_payload)} bytes)")

        # Get object
        response = os_client.get_object(namespace_name=namespace, bucket_name=bucket_name, object_name=object_name)
        read_back = response.data.content
        match = read_back == test_payload
        print(f"  Get object: {len(read_back)} bytes, match={match}")

        # Cleanup
        os_client.delete_object(namespace_name=namespace, bucket_name=bucket_name, object_name=object_name)
        os_client.delete_bucket(namespace_name=namespace, bucket_name=bucket_name)
        print(f"  Cleaned up bucket: {bucket_name}")

        _print_result("Object Storage Read/Write", match)
        return match
    except Exception as exc:
        _print_result("Object Storage Read/Write", False, str(exc))
        return False


# ---------------------------------------------------------------------------
# Phase 3 — Container Instance Lifecycle
# ---------------------------------------------------------------------------


def check_container_instance_lifecycle(
    profile: str,
    compartment_id: str,
    availability_domain: str,
    subnet_id: str,
    container_image: str,
    instance_shape: str = "CI.Standard.E4.Flex",
    instance_ocpus: float = 1.0,
    instance_memory_gb: float = 2.0,
) -> bool:
    """Launch a minimal container instance, wait for it to finish, and delete it."""
    print("\n--- Check: Container Instance Lifecycle ---")
    instance_id: Optional[str] = None
    try:
        config = oci.config.from_file(profile_name=profile)
        ci_client = oci.container_instances.ContainerInstanceClient(config)

        display_name = f"scaler-harness-{uuid.uuid4().hex[:8]}"
        print(f"  Creating container instance: {display_name}")
        print(f"  Image:  {container_image}")
        print(f"  Shape:  {instance_shape} ({instance_ocpus} OCPU, {instance_memory_gb} GB)")

        # Run a trivial command that exits immediately
        response = ci_client.create_container_instance(
            create_container_instance_details=oci.container_instances.models.CreateContainerInstanceDetails(
                compartment_id=compartment_id,
                availability_domain=availability_domain,
                shape=instance_shape,
                shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                    ocpus=instance_ocpus, memory_in_gbs=instance_memory_gb
                ),
                containers=[
                    oci.container_instances.models.CreateContainerDetails(
                        image_url=container_image,
                        display_name="harness-container",
                        command=["/bin/sh"],
                        arguments=["-c", "echo scaler-harness-ok && sleep 2"],
                    )
                ],
                vnics=[oci.container_instances.models.CreateContainerVnicDetails(subnet_id=subnet_id)],
                display_name=display_name,
                container_restart_policy="NEVER",
            )
        )
        instance_id = response.data.id
        print(f"  Instance OCID: ...{instance_id[-30:]}")

        # Poll until INACTIVE (completed) or FAILED
        start = time.monotonic()
        final_state = "UNKNOWN"
        while time.monotonic() - start < _CI_TIMEOUT_SECONDS:
            time.sleep(_CI_POLL_INTERVAL_SECONDS)
            status_response = ci_client.get_container_instance(container_instance_id=instance_id)
            state = status_response.data.lifecycle_state
            elapsed = int(time.monotonic() - start)
            print(f"  [{elapsed:3d}s] State: {state}")

            if state == "INACTIVE":
                final_state = "INACTIVE"
                break
            elif state == "FAILED":
                detail = getattr(status_response.data, "lifecycle_details", "")
                final_state = "FAILED"
                print(f"  Lifecycle details: {detail}")
                # Dump container-level state for diagnostics
                containers = getattr(status_response.data, "containers", []) or []
                for c in containers:
                    c_state = getattr(c, "lifecycle_state", "?")
                    c_detail = getattr(c, "lifecycle_details", "")
                    c_exit = getattr(c, "exit_code", None)
                    print(
                        f"  Container '{getattr(c, 'display_name', '?')}': "
                        f"state={c_state}, detail={c_detail}, exit_code={c_exit}"
                    )
                # Fetch container instance events if available
                try:
                    faults = getattr(status_response.data, "faults", None)
                    if faults:
                        print(f"  Faults: {faults}")
                except Exception:
                    pass
                break
        else:
            final_state = "TIMEOUT"
            print(f"  Timed out after {_CI_TIMEOUT_SECONDS}s")

        # Cleanup
        try:
            ci_client.delete_container_instance(container_instance_id=instance_id)
            print("  Deleted container instance")
        except oci.exceptions.ServiceError as del_exc:
            if del_exc.status != 404:
                print(f"  Warning: delete failed: {del_exc.message}")

        passed = final_state == "INACTIVE"
        _print_result("Container Instance Lifecycle", passed, f"final_state={final_state}")
        return passed

    except Exception as exc:
        # Best-effort cleanup
        if instance_id:
            try:
                config = oci.config.from_file(profile_name=profile)
                ci_client = oci.container_instances.ContainerInstanceClient(config)
                ci_client.delete_container_instance(container_instance_id=instance_id)
            except Exception:
                pass
        _print_result("Container Instance Lifecycle", False, str(exc))
        return False


# ---------------------------------------------------------------------------
# Phase 4 — Scheduler Task Tests
# ---------------------------------------------------------------------------


def run_sqrt_test(client: Any, timeout: int) -> bool:
    """Test math.sqrt(16) -> 4.0"""
    print("\n--- Test: sqrt ---")
    print("  Submitting: math.sqrt(16)")
    try:
        future = client.submit(math.sqrt, 16)
        result = future.result(timeout=timeout)
        print(f"  Result: {result}")
        passed = result == 4.0
        _print_result("sqrt", passed, f"got {result}, expected 4.0")
        return passed
    except Exception as exc:
        _print_result("sqrt", False, str(exc))
        return False


def run_simple_test(client: Any, timeout: int) -> bool:
    """Test simple_task(21) -> 42"""
    print("\n--- Test: simple ---")
    print("  Submitting: simple_task(21) [returns x * 2]")
    try:
        future = client.submit(simple_task, 21)
        result = future.result(timeout=timeout)
        print(f"  Result: {result}")
        passed = result == 42
        _print_result("simple", passed, f"got {result}, expected 42")
        return passed
    except Exception as exc:
        _print_result("simple", False, str(exc))
        return False


def run_map_test(client: Any, timeout: int) -> bool:
    """Test map-like execution with 5 tasks, enforcing timeout per task."""
    print("\n--- Test: map ---")
    print("  Submitting: simple_task for [0,1,2,3,4]")
    try:
        futures = [client.submit(simple_task, value) for value in range(5)]
        results = [future.result(timeout=timeout) for future in futures]
        print(f"  Results: {results}")
        expected = [0, 2, 4, 6, 8]
        passed = results == expected
        _print_result("map", passed, f"got {results}, expected {expected}")
        return passed
    except Exception as exc:
        _print_result("map", False, str(exc))
        return False


def run_compute_test(client: Any, timeout: int) -> bool:
    """Test compute-intensive task"""
    print("\n--- Test: compute ---")
    print("  Submitting: compute_task(1000) [sum of i*i*0.01 for i in range(1000)]")
    try:
        future = client.submit(compute_task, 1000)
        result = future.result(timeout=timeout)
        print(f"  Result: {result:.2f}")
        passed = 3000000 < result < 4000000
        _print_result("compute", passed, f"got {result:.2f}, expected in (3M, 4M)")
        return passed
    except Exception as exc:
        _print_result("compute", False, str(exc))
        return False


SCHEDULER_TESTS = {"sqrt": run_sqrt_test, "simple": run_simple_test, "map": run_map_test, "compute": run_compute_test}


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def load_config_file(config_path: str) -> Dict[str, Any]:
    """Load provisioner config JSON and map to CLI-compatible keys."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as fp:
        data = json.load(fp)
    return data


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OCI HPC Worker Adapter Test Harness", formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # OCI resource arguments
    parser.add_argument("--compartment-id", default=None, help="OCI Compartment OCID")
    parser.add_argument("--subnet-id", default=None, help="OCI Subnet OCID")
    parser.add_argument("--availability-domain", default=None, help="OCI Availability Domain")
    parser.add_argument("--container-image", default=None, help="OCIR image URI")
    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile (default: DEFAULT)")
    parser.add_argument("--instance-shape", default="CI.Standard.E4.Flex", help="Container Instance shape")
    parser.add_argument("--instance-ocpus", type=float, default=1.0, help="OCPUs for lifecycle test")
    parser.add_argument("--instance-memory-gb", type=float, default=2.0, help="Memory GB for lifecycle test")

    # Config file (alternative to individual args)
    parser.add_argument("--config", default=None, help="Path to provisioner config JSON file")

    # Scheduler arguments (Phase 4)
    parser.add_argument("--scheduler", default=None, help="Scheduler address (e.g. tcp://127.0.0.1:2345)")
    parser.add_argument(
        "--test",
        default="all",
        choices=["all"] + list(SCHEDULER_TESTS.keys()),
        help="Scheduler test to run (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=_SCHEDULER_TASK_TIMEOUT,
        help=f"Timeout per scheduler task in seconds (default: {_SCHEDULER_TASK_TIMEOUT})",
    )

    # Phase selection
    parser.add_argument(
        "--phase",
        default="all",
        choices=["all", "connectivity", "storage", "lifecycle", "scheduler"],
        help="Run only a specific phase (default: all)",
    )

    args = parser.parse_args()

    # Merge config file values with CLI args (CLI takes precedence)
    if args.config:
        cfg = load_config_file(args.config)
        if args.compartment_id is None:
            args.compartment_id = cfg.get("compartment_id")
        if args.subnet_id is None:
            args.subnet_id = cfg.get("subnet_id")
        if args.availability_domain is None:
            args.availability_domain = cfg.get("availability_domain")
        if args.container_image is None:
            args.container_image = cfg.get("container_image")
        if "instance_shape" in cfg and args.instance_shape == "CI.Standard.E4.Flex":
            args.instance_shape = cfg["instance_shape"]
        if "instance_ocpus" in cfg and args.instance_ocpus == 1.0:
            args.instance_ocpus = cfg["instance_ocpus"]
        if "instance_memory_gb" in cfg and args.instance_memory_gb == 2.0:
            args.instance_memory_gb = cfg["instance_memory_gb"]

    # Validate required args
    if not args.compartment_id:
        parser.error("--compartment-id is required (or provide --config)")

    # Resolve short AD names (e.g. "AD-1") to fully qualified names
    if args.availability_domain:
        args.availability_domain = _resolve_availability_domain(
            args.profile, args.compartment_id, args.availability_domain
        )

    run_all = args.phase == "all"
    results: List[bool] = []
    test_names: List[str] = []

    _print_header("OCI HPC Worker Adapter Test Harness")
    print(f"  Profile:             {args.profile}")
    print(f"  Compartment:         {(args.compartment_id or '')[:40]}...")
    print(f"  Subnet:              {(args.subnet_id or 'not set')[:40]}...")
    print(f"  Availability Domain: {args.availability_domain or 'not set'}")
    print(f"  Container Image:     {args.container_image or 'not set'}")
    print(f"  Scheduler:           {args.scheduler or 'not set (Phase 4 skipped)'}")
    print(f"  Phase:               {args.phase}")

    # --- Phase 1: Connectivity ---
    if run_all or args.phase == "connectivity":
        _print_header("Phase 1: OCI Connectivity")

        test_names.append("OCI Auth")
        results.append(check_oci_auth(args.profile))

        test_names.append("Compartment")
        results.append(check_compartment(args.profile, args.compartment_id))

        if args.subnet_id:
            test_names.append("Subnet")
            results.append(check_subnet(args.profile, args.subnet_id))

        if args.availability_domain:
            test_names.append("Availability Domain")
            results.append(check_availability_domain(args.profile, args.compartment_id, args.availability_domain))

    # --- Phase 2: Object Storage ---
    if run_all or args.phase == "storage":
        _print_header("Phase 2: Object Storage")
        test_names.append("Object Storage R/W")
        results.append(check_object_storage(args.profile, args.compartment_id))

    # --- Phase 3: Container Instance Lifecycle ---
    if run_all or args.phase == "lifecycle":
        if not args.subnet_id or not args.availability_domain or not args.container_image:
            print("\n  Skipping Phase 3: --subnet-id, --availability-domain, and --container-image are required")
        else:
            _print_header("Phase 3: Container Instance Lifecycle")
            test_names.append("Container Instance Lifecycle")
            results.append(
                check_container_instance_lifecycle(
                    profile=args.profile,
                    compartment_id=args.compartment_id,
                    availability_domain=args.availability_domain,
                    subnet_id=args.subnet_id,
                    container_image=args.container_image,
                    instance_shape=args.instance_shape,
                    instance_ocpus=args.instance_ocpus,
                    instance_memory_gb=args.instance_memory_gb,
                )
            )

    # --- Phase 4: Scheduler Tasks ---
    if (run_all or args.phase == "scheduler") and args.scheduler:
        _print_header("Phase 4: Scheduler Task Tests")

        try:
            with Client(address=args.scheduler) as client:
                print("  Connected to scheduler")

                tests_to_run = list(SCHEDULER_TESTS.keys()) if args.test == "all" else [args.test]
                for test_name in tests_to_run:
                    test_names.append(f"Scheduler: {test_name}")
                    results.append(SCHEDULER_TESTS[test_name](client, args.timeout))

        except ImportError:
            print("  Skipping: scaler package not installed")
        except Exception as exc:
            print(f"  Failed to connect to scheduler: {exc}")
            test_names.append("Scheduler: connect")
            results.append(False)
    elif (run_all or args.phase == "scheduler") and not args.scheduler:
        print("\n  Skipping Phase 4: --scheduler not provided")

    # --- Summary ---
    _print_header("Summary")
    total = len(results)
    passed = sum(results)
    for name, result in zip(test_names, results):
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {name}")

    print(f"\n  {passed}/{total} checks passed")

    if passed == total:
        print("\n  All checks passed!")
    else:
        print(f"\n  {total - passed} check(s) failed")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
