"""
OCI Object Storage, IAM, and Container Registry Provisioner.

Simple provisioning for OCI resources required by the Scaler OCI HPC adapter:
    - OCI Object Storage bucket  (analogous to AWS S3 bucket)
    - OCI Dynamic Group          (analogous to AWS IAM Role trust policy)
    - OCI IAM Policies           (analogous to AWS IAM role policies)
    - OCI Container Registry     (analogous to AWS ECR)

Service Mapping (AWS → OCI):
    - S3 bucket              → OCI Object Storage bucket
    - IAM role (Batch jobs)  → OCI Dynamic Group + IAM Policy
    - ECR repository         → OCI Container Registry (OCIR) repository
    - CloudWatch Log Group   → OCI Logging (log group managed automatically)
    - Compute Environment    → N/A (OCI Container Instances are fully on-demand)
    - Job Queue              → N/A (concurrency is managed by the adapter semaphore)

OCI Authentication:
    The provisioner uses the OCI Python SDK and reads credentials from the
    standard OCI config file (``~/.oci/config``). The profile can be overridden
    via the ``--profile`` CLI argument.

OCI Resource Identifiers:
    Most OCI resources are identified by OCIDs, e.g.:
        ``ocid1.compartment.oc1..aaaa...``
    The compartment OCID is required for all resource creation calls.
"""

import argparse
import json
import logging
import subprocess
from pathlib import Path

import oci

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "scaler-oci"
DEFAULT_CONFIG_FILE = ".scaler_oci_config.json"
DEFAULT_ENV_FILE = ".scaler_oci_hpc.env"

OCI_TASK_PREFIX = "scaler-tasks"
OCI_BUCKET_LIFECYCLE_DAYS = 7

# OCI Container Instance shapes (x86_64)
DEFAULT_INSTANCE_SHAPE = "CI.Standard.E4.Flex"

# Number of OCIR images to retain (older images are deleted automatically via lifecycle policy)
OCIR_IMAGES_TO_KEEP = 3


class OCIProvisioner:
    """
    Provisions OCI resources for the Scaler OCI Container Instance adapter.

    Creates:
        - OCI Object Storage bucket (for task payloads and results)
        - OCI Dynamic Group        (grants Container Instances Resource Principal auth)
        - OCI IAM Policy           (grants Object Storage access to the Dynamic Group)
        - OCI Container Registry   (for the container image used by instances)

    Unlike the AWS Batch adapter, there is no equivalent of a Compute Environment
    or Job Queue — OCI Container Instances are created on-demand per task, and
    concurrency is governed by the adapter's semaphore (``base_concurrency``).
    """

    def __init__(
        self,
        compartment_id: str,
        oci_region: str = "us-ashburn-1",
        prefix: str = DEFAULT_PREFIX,
        oci_config_profile: str = "DEFAULT",
    ) -> None:
        self._compartment_id = compartment_id
        self._region = oci_region
        self._prefix = prefix
        self._oci_config_profile = oci_config_profile

        config = oci.config.from_file(profile_name=oci_config_profile)
        self._tenancy_id = config["tenancy"]

        self._object_storage = oci.object_storage.ObjectStorageClient(config)
        self._identity = oci.identity.IdentityClient(config)

        # Tenancy namespace is required for all Object Storage API calls
        self._namespace = self._object_storage.get_namespace().data

    # ------------------------------------------------------------------
    # Top-level provisioning
    # ------------------------------------------------------------------

    def provision_all(
        self,
        container_image: str,
        availability_domain: str,
        subnet_id: str,
        instance_shape: str = DEFAULT_INSTANCE_SHAPE,
        instance_ocpus: float = 1.0,
        instance_memory_gb: float = 6.0,
        job_timeout_seconds: int = 3600,
    ) -> dict[str, object]:
        """
        Provision all required OCI resources for the adapter.

        Args:
            container_image: OCIR image URI for the job runner container.
            availability_domain: OCI Availability Domain for container instances
                (e.g., ``"AD-1"`` or the full name ``"Uocm:PHX-AD-1"``).
            subnet_id: OCID of the subnet where container instances will run.
            instance_shape: OCI Container Instance shape.
            instance_ocpus: OCPUs per container instance.
            instance_memory_gb: Memory (GB) per container instance.
            job_timeout_seconds: Maximum runtime for a single container instance.

        Returns:
            Dictionary of provisioned resource identifiers, suitable for
            saving with :meth:`save_config`.
        """
        logger.info(f"Provisioning OCI resources with prefix '{self._prefix}'...")

        bucket_name = self.provision_object_storage_bucket()
        self.provision_dynamic_group(bucket_name)
        self.provision_iam_policy(bucket_name)

        result = {
            "oci_region": self._region,
            "tenancy_id": self._tenancy_id,
            "compartment_id": self._compartment_id,
            "prefix": self._prefix,
            "object_storage_namespace": self._namespace,
            "object_storage_bucket": bucket_name,
            "object_storage_prefix": OCI_TASK_PREFIX,
            "container_image": container_image,
            "availability_domain": availability_domain,
            "subnet_id": subnet_id,
            "instance_shape": instance_shape,
            "instance_ocpus": instance_ocpus,
            "instance_memory_gb": instance_memory_gb,
            "job_timeout_seconds": job_timeout_seconds,
            "dynamic_group_name": f"{self._prefix}-dg",
            "iam_policy_name": f"{self._prefix}-policy",
        }

        logger.info("Provisioning complete!")
        return result

    # ------------------------------------------------------------------
    # Resource provisioning
    # ------------------------------------------------------------------

    def provision_object_storage_bucket(self) -> str:
        """
        Create an OCI Object Storage bucket for task data.

        Sets a lifecycle rule to auto-delete objects under the task prefix
        after ``OCI_BUCKET_LIFECYCLE_DAYS`` days.

        Returns:
            The bucket name.
        """
        bucket_name = f"{self._prefix}-{self._namespace}-{self._region}"

        try:
            self._object_storage.create_bucket(
                namespace_name=self._namespace,
                create_bucket_details=oci.object_storage.models.CreateBucketDetails(
                    name=bucket_name, compartment_id=self._compartment_id, public_access_type="NoPublicAccess"
                ),
            )
            logger.info(f"Created Object Storage bucket: {bucket_name}")
        except oci.exceptions.ServiceError as exc:
            if exc.status == 409:  # BucketAlreadyExists
                logger.info(f"Object Storage bucket already exists: {bucket_name}")
            else:
                raise

        # set lifecycle rule to clean up task objects after 1 day
        try:
            self._object_storage.put_object_lifecycle_policy(
                namespace_name=self._namespace,
                bucket_name=bucket_name,
                put_object_lifecycle_policy_details=oci.object_storage.models.PutObjectLifecyclePolicyDetails(
                    items=[
                        oci.object_storage.models.ObjectLifecycleRule(
                            name="cleanup-old-tasks",
                            action="DELETE",
                            time_amount=OCI_BUCKET_LIFECYCLE_DAYS,
                            time_unit="DAYS",
                            is_enabled=True,
                            object_name_filter=oci.object_storage.models.ObjectNameFilter(
                                inclusion_prefixes=[f"{OCI_TASK_PREFIX}/"]
                            ),
                        )
                    ]
                ),
            )
            logger.info(f"set Object Storage lifecycle rule: delete after {OCI_BUCKET_LIFECYCLE_DAYS} day(s)")
        except Exception as exc:
            logger.warning(f"Failed to set lifecycle rule: {exc}")

        return bucket_name

    def provision_dynamic_group(self, bucket_name: str) -> str:
        """
        Create an OCI Dynamic Group that includes all Container Instances in the compartment.

        Container Instances that are members of this Dynamic Group can authenticate
        using Resource Principals, which are used by the job runner to access
        Object Storage without embedding credentials.

        Returns:
            The Dynamic Group OCID.
        """
        dg_name = f"{self._prefix}-dg"
        matching_rule = (
            f"ALL {{resource.type='computecontainerinstance', " f"resource.compartment.id='{self._compartment_id}'}}"
        )

        try:
            response = self._identity.create_dynamic_group(
                create_dynamic_group_details=oci.identity.models.CreateDynamicGroupDetails(
                    compartment_id=self._tenancy_id,  # Dynamic Groups live at the tenancy level
                    name=dg_name,
                    matching_rule=matching_rule,
                    description=f"Scaler OCI HPC adapter — container instances in compartment {self._compartment_id}",
                )
            )
            dg_id = response.data.id
            logger.info(f"Created Dynamic Group: {dg_name} ({dg_id})")
            return dg_id
        except oci.exceptions.ServiceError as exc:
            if exc.status == 409:  # EntityAlreadyExists
                logger.info(f"Dynamic Group already exists: {dg_name}")
                groups = self._identity.list_dynamic_groups(compartment_id=self._tenancy_id).data
                for group in groups:
                    if group.name == dg_name:
                        return group.id
            raise

    def provision_iam_policy(self, bucket_name: str) -> str:
        """
        Create an OCI IAM Policy granting the Dynamic Group read/write access
        to the Object Storage bucket used for task payloads and results.

        Returns:
            The IAM Policy OCID.
        """
        dg_name = f"{self._prefix}-dg"
        policy_name = f"{self._prefix}-policy"

        statements = [
            (
                f"Allow dynamic-group {dg_name} to manage objects in compartment id {self._compartment_id} "
                f"where target.bucket.name='{bucket_name}'"
            )
        ]

        try:
            response = self._identity.create_policy(
                create_policy_details=oci.identity.models.CreatePolicyDetails(
                    compartment_id=self._compartment_id,
                    name=policy_name,
                    statements=statements,
                    description="Scaler OCI HPC adapter — Object Storage access for container instances",
                )
            )
            policy_id = response.data.id
            logger.info(f"Created IAM Policy: {policy_name} ({policy_id})")
            return policy_id
        except oci.exceptions.ServiceError as exc:
            if exc.status == 409:  # PolicyAlreadyExists
                logger.info(f"IAM Policy already exists: {policy_name}")
                policies = self._identity.list_policies(compartment_id=self._compartment_id).data
                for policy in policies:
                    if policy.name == policy_name:
                        return policy.id
            raise

    def build_and_push_image(self) -> str:
        """
        Build the job runner Docker image and push it to OCI Container Registry (OCIR).

        Requires Docker and the OCI CLI (or manual auth token setup) to be available
        in the environment.

        The OCIR registry URL format is: ``<region>.ocir.io/<namespace>/<repo>:<tag>``

        Returns:
            The full OCIR image URI.
        """
        config = oci.config.from_file(profile_name=self._oci_config_profile)
        artifacts_client = oci.artifacts.ArtifactsClient(config)

        repo_name = f"{self._prefix}-worker"
        image_uri = f"{self._region}.ocir.io/{self._namespace}/{repo_name}:latest"

        # Create OCIR repository if it doesn't exist
        try:
            artifacts_client.create_repository(
                create_repository_details=oci.artifacts.models.CreateContainerRepositoryDetails(
                    compartment_id=self._compartment_id, display_name=repo_name, is_public=False, readme=None
                )
            )
            logger.info(f"Created OCIR repository: {repo_name}")
        except oci.exceptions.ServiceError as exc:
            if exc.status == 409:  # RepositoryAlreadyExists
                logger.info(f"OCIR repository already exists: {repo_name}")
            else:
                raise

        # Build image (linux/amd64 for CI.Standard.E4.Flex shape)
        dockerfile_path = Path(__file__).parent / "Dockerfile.container_instance"
        # Path(__file__) is: .../src/scaler/worker_manager_adapter/oci_hpc/utility/provisioner.py
        # Go up 5 levels to reach the repository src/ directory
        src_root = Path(__file__).parent.parent.parent.parent.parent / "src"

        build_cmd = [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "-f",
            str(dockerfile_path),
            "-t",
            image_uri,
            str(src_root),
        ]
        logger.info(f"Building image for linux/amd64: {image_uri}")
        subprocess.run(build_cmd, check=True)

        # Push image to OCIR
        # NOTE: Docker must be logged in to the OCIR registry before pushing.
        # Use: docker login <region>.ocir.io -u <namespace>/<username> -p <auth_token>
        # Auth tokens are created in OCI Console → Identity → Users → Auth Tokens.
        logger.info(f"Pushing image to OCIR: {image_uri}")
        subprocess.run(["docker", "push", image_uri], check=True)

        logger.info(f"Image pushed: {image_uri}")
        return image_uri

    # ------------------------------------------------------------------
    # Config persistence
    # ------------------------------------------------------------------

    @staticmethod
    def save_config(config: dict[str, object], config_file: str = DEFAULT_CONFIG_FILE) -> None:
        """Save provisioned config to a JSON file."""
        path = Path(config_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fp:
            json.dump(config, fp, indent=2)
        logger.info(f"Config saved to {path.absolute()}")

    @staticmethod
    def load_config(config_file: str = DEFAULT_CONFIG_FILE) -> dict[str, object]:
        """Load provisioned config from a JSON file."""
        path = Path(config_file)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path.absolute()}")
        with open(path) as fp:
            return json.load(fp)

    @staticmethod
    def print_export_commands(config: dict[str, object]) -> None:
        """Print shell export commands for the most commonly needed config values."""
        print(f'''\
export SCALER_OCI_REGION="{config["oci_region"]}"
export SCALER_OCI_COMPARTMENT_ID="{config["compartment_id"]}"
export SCALER_OCI_NAMESPACE="{config["object_storage_namespace"]}"
export SCALER_OCI_BUCKET="{config["object_storage_bucket"]}"
export SCALER_OCI_CONTAINER_IMAGE="{config["container_image"]}"
export SCALER_OCI_SUBNET_ID="{config["subnet_id"]}"
export SCALER_OCI_AVAILABILITY_DOMAIN="{config["availability_domain"]}"''')

    @staticmethod
    def save_env_file(config: dict[str, object], env_file: str = DEFAULT_ENV_FILE) -> None:
        """Save config as a sourceable shell env file."""
        path = Path(env_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fp:
            fp.write(f"""\
export SCALER_OCI_REGION="{config["oci_region"]}"
export SCALER_OCI_COMPARTMENT_ID="{config["compartment_id"]}"
export SCALER_OCI_NAMESPACE="{config["object_storage_namespace"]}"
export SCALER_OCI_BUCKET="{config["object_storage_bucket"]}"
export SCALER_OCI_CONTAINER_IMAGE="{config["container_image"]}"
export SCALER_OCI_SUBNET_ID="{config["subnet_id"]}"
export SCALER_OCI_AVAILABILITY_DOMAIN="{config["availability_domain"]}"
""")
        logger.info(f"Env file saved: {path.absolute()}")
        logger.info(f"Run: source {env_file}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self) -> None:
        """Delete all provisioned OCI resources."""
        logger.info("Cleaning up OCI resources...")

        bucket_name = f"{self._prefix}-{self._namespace}-{self._region}"
        dg_name = f"{self._prefix}-dg"
        policy_name = f"{self._prefix}-policy"

        # Delete IAM policy
        try:
            policies = self._identity.list_policies(compartment_id=self._compartment_id).data
            for policy in policies:
                if policy.name == policy_name:
                    self._identity.delete_policy(policy_id=policy.id)
                    logger.info(f"Deleted IAM Policy: {policy_name}")
                    break
        except Exception as exc:
            logger.warning(f"Failed to delete IAM Policy: {exc}")

        # Delete Dynamic Group
        try:
            groups = self._identity.list_dynamic_groups(compartment_id=self._tenancy_id).data
            for group in groups:
                if group.name == dg_name:
                    self._identity.delete_dynamic_group(dynamic_group_id=group.id)
                    logger.info(f"Deleted Dynamic Group: {dg_name}")
                    break
        except Exception as exc:
            logger.warning(f"Failed to delete Dynamic Group: {exc}")

        # Empty and delete Object Storage bucket
        try:
            # Paginate through all objects and delete them
            next_start = None
            while True:
                kwargs = {"namespace_name": self._namespace, "bucket_name": bucket_name}
                if next_start is not None:
                    kwargs["start"] = next_start

                list_response = self._object_storage.list_objects(**kwargs)
                for obj in list_response.data.objects:
                    self._object_storage.delete_object(
                        namespace_name=self._namespace, bucket_name=bucket_name, object_name=obj.name
                    )

                next_start = list_response.data.next_start_with
                if not next_start:
                    break

            self._object_storage.delete_bucket(namespace_name=self._namespace, bucket_name=bucket_name)
            logger.info(f"Deleted Object Storage bucket: {bucket_name}")
        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                logger.info(f"Object Storage bucket not found, skipping: {bucket_name}")
            else:
                logger.warning(f"Failed to delete Object Storage bucket: {exc}")

        # Delete OCIR repository
        try:
            artifacts_client = oci.artifacts.ArtifactsClient(
                oci.config.from_file(profile_name=self._oci_config_profile)
            )
            repos = artifacts_client.list_repositories(compartment_id=self._compartment_id).data.items
            repo_name = f"{self._prefix}-worker"
            for repo in repos:
                if repo.display_name == repo_name:
                    artifacts_client.delete_repository(repository_id=repo.id)
                    logger.info(f"Deleted OCIR repository: {repo_name}")
                    break
        except Exception as exc:
            logger.warning(f"Failed to delete OCIR repository: {exc}")

        logger.info("Cleanup complete!")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def list_availability_domains(self) -> list[str]:
        """Return the names of all Availability Domains in the compartment's region."""
        ads = self._identity.list_availability_domains(compartment_id=self._tenancy_id).data
        return [ad.name for ad in ads]

    def get_namespace(self) -> str:
        """Return the OCI Object Storage tenancy namespace."""
        return self._namespace


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------


def main() -> None:
    """CLI for provisioning OCI resources."""
    parser = argparse.ArgumentParser(description="Provision OCI resources for Scaler OCI HPC adapter")
    parser.add_argument(
        "action", choices=["provision", "cleanup", "show", "build-image", "list-ads"], help="Action to perform"
    )
    parser.add_argument("--compartment-id", required=True, help="OCI Compartment OCID")
    parser.add_argument("--region", default="us-ashburn-1", help="OCI region (default: us-ashburn-1)")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="Resource name prefix")
    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile (default: DEFAULT)")
    parser.add_argument("--image", default=None, help="Container image URI (default: builds and pushes to OCIR)")
    parser.add_argument("--availability-domain", default=None, help="OCI Availability Domain for container instances")
    parser.add_argument("--subnet-id", default=None, help="OCI Subnet OCID for container instances")
    parser.add_argument("--instance-shape", default=DEFAULT_INSTANCE_SHAPE, help="OCI Container Instance shape")
    parser.add_argument("--instance-ocpus", type=float, default=1.0, help="OCPUs per container instance")
    parser.add_argument("--instance-memory-gb", type=float, default=6.0, help="Memory (GB) per container instance")
    parser.add_argument("--job-timeout", type=int, default=60, help="Job timeout in minutes (default: 60)")
    parser.add_argument("--config", default=DEFAULT_CONFIG_FILE, help="Config file path")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help="Env file path")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    provisioner = OCIProvisioner(
        compartment_id=args.compartment_id, oci_region=args.region, prefix=args.prefix, oci_config_profile=args.profile
    )

    if args.action == "list-ads":
        ads = provisioner.list_availability_domains()
        print("Availability Domains:")
        for ad in ads:
            print(f"  {ad}")
        return

    if args.action == "show":
        try:
            config = OCIProvisioner.load_config(args.config)
            print("\n=== Saved OCI Config ===")
            for key, value in config.items():
                print(f"  {key}: {value}")
            print(f"\nTo load env vars: source {args.env_file}")
        except FileNotFoundError as exc:
            print(f"Error: {exc}")
            print("Run 'provision' first to create resources.")
        return

    if args.action == "build-image":
        image_uri = provisioner.build_and_push_image()
        print(f"\nImage URI: {image_uri}")
        print("\nTo use this image, run provision with:")
        print(f"  --image {image_uri}")
        return

    if args.action == "provision":
        if args.availability_domain is None:
            parser.error("--availability-domain is required for 'provision'")
        if args.subnet_id is None:
            parser.error("--subnet-id is required for 'provision'")

        container_image = args.image
        if container_image is None:
            logger.info("No --image specified, building and pushing to OCIR...")
            container_image = provisioner.build_and_push_image()

        result = provisioner.provision_all(
            container_image=container_image,
            availability_domain=args.availability_domain,
            subnet_id=args.subnet_id,
            instance_shape=args.instance_shape,
            instance_ocpus=args.instance_ocpus,
            instance_memory_gb=args.instance_memory_gb,
            job_timeout_seconds=args.job_timeout * 60,
        )
        OCIProvisioner.save_config(result, args.config)
        OCIProvisioner.save_env_file(result, args.env_file)

        print("\n=== Provisioned Resources ===")
        for key, value in result.items():
            print(f"  {key}: {value}")
        print(f"\nTo load env vars: source {args.env_file}")

    elif args.action == "cleanup":
        provisioner.cleanup()


if __name__ == "__main__":
    main()
