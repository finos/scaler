"""
AWS Batch and S3 Provisioner.

Simple provisioning for AWS Batch compute environment, job queue,
job definition, and S3 bucket required for the Scaler AWS Batch worker manager.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "scaler-batch"
DEFAULT_CONFIG_FILE = ".scaler_aws_batch_config.json"

S3_TASK_PREFIX = "scaler-tasks"
S3_LIFECYCLE_EXPIRATION_DAYS = 1

MEMORY_BASE_MB = 2048
MEMORY_UTILIZATION_FACTOR = 0.9

ALLOCATION_STRATEGY = "BEST_FIT_PROGRESSIVE"

CLOUDWATCH_LOG_GROUP = "/aws/batch/job"
CLOUDWATCH_RETENTION_DAYS = 30

COMPUTE_ENV_WAIT_TIMEOUT_SECONDS = 300

ECR_IMAGES_TO_KEEP = 3
JOB_DEFINITION_REVISIONS_TO_KEEP = 2

IAM_POLICY_ECS_TASK_EXECUTION = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
IAM_POLICY_EC2_CONTAINER_SERVICE = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"


class AWSBatchInfrastructureProvisioner:
    """
    Provisions AWS resources for Scaler AWS Batch worker manager.

    Creates:
        - S3 bucket for task payloads and results
        - IAM role for Batch jobs (with S3 access)
        - EC2 compute environment
        - Batch job queue
        - Batch job definition
    """

    def __init__(
        self,
        aws_region: str = "us-east-1",
        prefix: str = DEFAULT_PREFIX,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        self._region = aws_region
        self._prefix = prefix

        session_kwargs = {"region_name": aws_region}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key

        self._session = boto3.Session(**session_kwargs)
        self._s3 = self._session.client("s3")
        self._batch = self._session.client("batch")
        self._iam = self._session.client("iam")
        self._sts = self._session.client("sts")

        self._account_id = self._sts.get_caller_identity()["Account"]

    def provision_all(
        self,
        container_image: str,
        vcpus: int = 1,
        memory_mb: int = 2048,
        max_vcpus: int = 256,
        instance_types: Optional[List[str]] = None,
        job_timeout_seconds: int = 3600,
    ) -> Dict[str, object]:
        """
        Provision all required AWS resources.

        Args:
            container_image: Docker image for job definition
            vcpus: vCPUs per job (integer for EC2)
            memory_mb: Memory per job in MB (will use 90% of nearest 2048MB multiple)
            max_vcpus: Max vCPUs for compute environment
            instance_types: List of EC2 instance types (default: ["default_x86_64"])
            job_timeout_seconds: Max job runtime in seconds (default: 3600 = 1 hour)

        Returns:
            dict with resource names/ARNs
        """
        logger.info(f"Provisioning AWS Batch (EC2) resources with prefix '{self._prefix}'...")

        if instance_types is None:
            instance_types = ["default_x86_64"]

        # 1. S3 bucket
        bucket_name = self.provision_s3_bucket()

        # 2. IAM role for jobs
        role_arn = self.provision_iam_role(bucket_name)

        # 3. Compute environment (EC2)
        compute_env_arn = self.provision_compute_environment(max_vcpus, instance_types)

        # 4. Job queue
        job_queue_arn = self.provision_job_queue(compute_env_arn)

        # 5. Job definition
        job_def_arn = self.provision_job_definition(
            container_image=container_image,
            role_arn=role_arn,
            vcpus=vcpus,
            memory_mb=memory_mb,
            job_timeout_seconds=job_timeout_seconds,
        )

        # Calculate effective memory for reporting
        memory_multiple = max(1, round(memory_mb / MEMORY_BASE_MB))
        effective_memory = int(memory_multiple * MEMORY_BASE_MB * MEMORY_UTILIZATION_FACTOR)

        result = {
            "aws_region": self._region,
            "aws_account_id": self._account_id,
            "prefix": self._prefix,
            "compute_type": "EC2",
            "s3_bucket": bucket_name,
            "s3_prefix": S3_TASK_PREFIX,
            "iam_role_arn": role_arn,
            "compute_environment_arn": compute_env_arn,
            "job_queue_arn": job_queue_arn,
            "job_queue_name": f"{self._prefix}-queue",
            "job_definition_arn": job_def_arn,
            "job_definition_name": f"{self._prefix}-job",
            "container_image": container_image,
            "vcpus": vcpus,
            "memory_mb": effective_memory,
            "instance_types": instance_types,
        }

        logger.info("Provisioning complete!")
        return result

    @staticmethod
    def save_config(config: Dict[str, object], config_file: str = DEFAULT_CONFIG_FILE) -> None:
        """Save provisioned config to file."""
        path = Path(config_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        logger.info(f"Config saved to {path.absolute()}")

    @staticmethod
    def load_config(config_file: str = DEFAULT_CONFIG_FILE) -> Dict[str, object]:
        """Load provisioned config from file."""
        path = Path(config_file)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path.absolute()}")
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def print_export_commands(config: Dict[str, object]) -> None:
        """Print shell export commands for config values."""
        print(f"export SCALER_AWS_REGION=\"{config['aws_region']}\"")
        print(f"export SCALER_S3_BUCKET=\"{config['s3_bucket']}\"")
        print(f"export SCALER_JOB_QUEUE=\"{config['job_queue_name']}\"")
        print(f"export SCALER_JOB_DEFINITION=\"{config['job_definition_name']}\"")

    @staticmethod
    def save_env_file(config: Dict[str, object], env_file: str = ".scaler_aws_hpc.env") -> None:
        """Save config as sourceable shell env file."""
        path = Path(env_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(f"export SCALER_AWS_REGION=\"{config['aws_region']}\"\n")
            f.write(f"export SCALER_S3_BUCKET=\"{config['s3_bucket']}\"\n")
            f.write(f"export SCALER_JOB_QUEUE=\"{config['job_queue_name']}\"\n")
            f.write(f"export SCALER_JOB_DEFINITION=\"{config['job_definition_name']}\"\n")
        logger.info(f"Env file saved: {path.absolute()}")
        logger.info(f"Run: source {env_file}")

    def provision_s3_bucket(self) -> str:
        """Create S3 bucket for task data."""
        bucket_name = f"{self._prefix}-{self._account_id}-{self._region}"

        try:
            if self._region == "us-east-1":
                self._s3.create_bucket(Bucket=bucket_name)
            else:
                self._s3.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region}
                )
            logger.info(f"Created S3 bucket: {bucket_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                logger.info(f"S3 bucket already exists: {bucket_name}")
            else:
                raise

        # Enable lifecycle to clean up old objects
        self._s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "cleanup-old-tasks",
                        "Status": "Enabled",
                        "Filter": {"Prefix": f"{S3_TASK_PREFIX}/"},
                        "Expiration": {"Days": S3_LIFECYCLE_EXPIRATION_DAYS},
                    }
                ]
            },
        )

        return bucket_name

    def provision_iam_role(self, bucket_name: str) -> str:
        """Create IAM role for Batch jobs with S3 access and ECS task execution permissions."""
        role_name = f"{self._prefix}-job-role"

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }

        s3_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": f"arn:aws:s3:::{bucket_name}/{S3_TASK_PREFIX}/*",
                }
            ],
        }

        try:
            response = self._iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for Scaler AWS Batch jobs",
            )
            role_arn = response["Role"]["Arn"]
            logger.info(f"Created IAM role: {role_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                role_arn = f"arn:aws:iam::{self._account_id}:role/{role_name}"
                logger.info(f"IAM role already exists: {role_name}")
            else:
                raise

        # Attach AWS managed policy for ECS task execution (covers CloudWatch Logs, ECR)
        try:
            self._iam.attach_role_policy(RoleName=role_name, PolicyArn=IAM_POLICY_ECS_TASK_EXECUTION)
            logger.info(f"Attached AmazonECSTaskExecutionRolePolicy to {role_name}")
        except ClientError:
            pass  # May already be attached

        # Attach S3 policy for task data
        policy_name = f"{self._prefix}-s3-policy"
        try:
            self._iam.put_role_policy(RoleName=role_name, PolicyName=policy_name, PolicyDocument=json.dumps(s3_policy))
        except ClientError:
            pass  # Policy may already exist

        return role_arn

    def provision_compute_environment(self, max_vcpus: int, instance_types: List[str]) -> str:
        """Create EC2 compute environment (not Fargate for better container reuse)."""
        env_name = f"{self._prefix}-compute"

        try:
            response = self._batch.create_compute_environment(
                computeEnvironmentName=env_name,
                type="MANAGED",
                state="ENABLED",
                computeResources={
                    "type": "EC2",  # Use EC2 instead of Fargate for container reuse
                    "allocationStrategy": ALLOCATION_STRATEGY,
                    "minvCpus": 0,
                    "maxvCpus": max_vcpus,
                    "desiredvCpus": 0,
                    "instanceTypes": instance_types,
                    "subnets": self._get_default_subnets(),
                    "securityGroupIds": self._get_default_security_group(),
                    "instanceRole": self._get_or_create_instance_profile(),
                },
            )
            env_arn = response["computeEnvironmentArn"]
            logger.info(f"Created EC2 compute environment: {env_name} (instance_types={instance_types})")

            # Wait for compute environment to be valid
            self._wait_for_compute_environment(env_name)

        except ClientError as e:
            if "already exists" in str(e):
                env_arn = f"arn:aws:batch:{self._region}:{self._account_id}:compute-environment/{env_name}"
                logger.info(f"Compute environment already exists: {env_name}")
            else:
                raise

        return env_arn

    def _get_or_create_instance_profile(self) -> str:
        """Get or create EC2 instance profile for Batch compute environment."""
        profile_name = f"{self._prefix}-instance-profile"
        role_name = f"{self._prefix}-instance-role"

        # Trust policy for EC2
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }

        # Create role if it doesn't exist
        try:
            self._iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for Scaler AWS Batch EC2 instances",
            )
            logger.info(f"Created instance role: {role_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityAlreadyExists":
                raise

        # Attach required policies for Batch EC2 instances
        required_policies = [IAM_POLICY_EC2_CONTAINER_SERVICE]
        for policy_arn in required_policies:
            try:
                self._iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            except ClientError:
                pass

        # Create instance profile if it doesn't exist
        try:
            self._iam.create_instance_profile(InstanceProfileName=profile_name)
            logger.info(f"Created instance profile: {profile_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "EntityAlreadyExists":
                raise

        # Add role to instance profile
        try:
            self._iam.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)
        except ClientError as e:
            if "LimitExceeded" not in str(e) and "already exists" not in str(e).lower():
                raise

        return f"arn:aws:iam::{self._account_id}:instance-profile/{profile_name}"

    def provision_job_queue(self, compute_env_arn: str) -> str:
        """Create job queue."""
        queue_name = f"{self._prefix}-queue"

        try:
            response = self._batch.create_job_queue(
                jobQueueName=queue_name,
                state="ENABLED",
                priority=1,
                computeEnvironmentOrder=[{"order": 1, "computeEnvironment": compute_env_arn}],
            )
            queue_arn = response["jobQueueArn"]
            logger.info(f"Created job queue: {queue_name}")
        except ClientError as e:
            if "already exists" in str(e):
                queue_arn = f"arn:aws:batch:{self._region}:{self._account_id}:job-queue/{queue_name}"
                logger.info(f"Job queue already exists: {queue_name}")
            else:
                raise

        return queue_arn

    def provision_job_definition(
        self, container_image: str, role_arn: str, vcpus: int, memory_mb: int, job_timeout_seconds: int
    ) -> str:
        """Create job definition for EC2 compute environment."""
        job_def_name = f"{self._prefix}-job"

        # Round memory to nearest multiple of MEMORY_BASE_MB and use MEMORY_UTILIZATION_FACTOR
        memory_multiple = max(1, round(memory_mb / MEMORY_BASE_MB))
        total_memory = memory_multiple * MEMORY_BASE_MB
        effective_memory = int(total_memory * MEMORY_UTILIZATION_FACTOR)

        logger.info(
            f"Memory config: requested={memory_mb}MB, "
            f"multiple={memory_multiple}x{MEMORY_BASE_MB}={total_memory}MB, "
            f"effective({int(MEMORY_UTILIZATION_FACTOR * 100)}%)={effective_memory}MB"
        )

        # Set up CloudWatch Logs retention (30 days)
        self._setup_cloudwatch_logs_retention()

        response = self._batch.register_job_definition(
            jobDefinitionName=job_def_name,
            type="container",
            # No platformCapabilities for EC2 (that's Fargate-only)
            parameters={
                "task_id": "none",
                "payload": "none",
                "compressed": "0",
                "s3_bucket": "none",
                "s3_prefix": "none",
                "s3_key": "none",
            },
            containerProperties={
                "image": container_image,
                "command": [
                    "--task_id",
                    "Ref::task_id",
                    "--payload",
                    "Ref::payload",
                    "--compressed",
                    "Ref::compressed",
                    "--s3_bucket",
                    "Ref::s3_bucket",
                    "--s3_prefix",
                    "Ref::s3_prefix",
                    "--s3_key",
                    "Ref::s3_key",
                ],
                "jobRoleArn": role_arn,
                "vcpus": int(vcpus),  # EC2 requires integer vCPUs
                "memory": effective_memory,  # 90% of multiple of 2048MB
            },
            timeout={"attemptDurationSeconds": job_timeout_seconds},
        )

        job_def_arn = response["jobDefinitionArn"]
        logger.info(f"Registered job definition: {job_def_name} (vcpus={int(vcpus)}, memory={effective_memory}MB)")

        # Cleanup old job definition revisions, keep only latest N
        self._cleanup_old_job_definitions(job_def_name, keep_latest=JOB_DEFINITION_REVISIONS_TO_KEEP)

        return job_def_arn

    def _cleanup_old_job_definitions(
        self, job_def_name: str, keep_latest: int = JOB_DEFINITION_REVISIONS_TO_KEEP
    ) -> None:
        """Deregister old job definition revisions, keeping only the latest N."""
        try:
            response = self._batch.describe_job_definitions(jobDefinitionName=job_def_name, status="ACTIVE")

            job_defs = response.get("jobDefinitions", [])
            if len(job_defs) <= keep_latest:
                return

            # Sort by revision (highest first)
            job_defs.sort(key=lambda x: x.get("revision", 0), reverse=True)

            # Deregister all but the latest N
            for job_def in job_defs[keep_latest:]:
                try:
                    self._batch.deregister_job_definition(jobDefinition=job_def["jobDefinitionArn"])
                    logger.info(f"Deregistered old job definition: {job_def['jobDefinitionArn']}")
                except ClientError as e:
                    logger.warning(f"Failed to deregister {job_def['jobDefinitionArn']}: {e}")

        except ClientError as e:
            logger.warning(f"Failed to cleanup old job definitions: {e}")

    def _setup_cloudwatch_logs_retention(self, retention_days: int = CLOUDWATCH_RETENTION_DAYS) -> None:
        """Set CloudWatch Logs retention policy for AWS Batch logs."""
        logs_client = self._session.client("logs")
        log_group_name = CLOUDWATCH_LOG_GROUP

        try:
            # Create log group if it doesn't exist
            try:
                logs_client.create_log_group(logGroupName=log_group_name)
                logger.info(f"Created CloudWatch log group: {log_group_name}")
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                    raise

            # Set retention policy
            logs_client.put_retention_policy(logGroupName=log_group_name, retentionInDays=retention_days)
            logger.info(f"Set CloudWatch Logs retention: {retention_days} days for {log_group_name}")
        except ClientError as e:
            logger.warning(f"Failed to set CloudWatch Logs retention: {e}")

    def _get_default_subnets(self) -> List[str]:
        """Get default VPC subnets."""
        ec2 = self._session.client("ec2")
        response = ec2.describe_subnets(Filters=[{"Name": "default-for-az", "Values": ["true"]}])
        return [s["SubnetId"] for s in response["Subnets"]]

    def _get_default_security_group(self) -> List[str]:
        """Get default security group."""
        ec2 = self._session.client("ec2")
        response = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": ["default"]}])
        return [response["SecurityGroups"][0]["GroupId"]]

    def _wait_for_compute_environment(self, env_name: str, timeout: int = COMPUTE_ENV_WAIT_TIMEOUT_SECONDS) -> None:
        """Wait for compute environment to become VALID."""
        import time

        start = time.time()
        logger.info(f"Waiting for compute environment {env_name} to become VALID (timeout: {timeout}s)...")
        while time.time() - start < timeout:
            response = self._batch.describe_compute_environments(computeEnvironments=[env_name])
            if not response["computeEnvironments"]:
                logger.warning(f"Compute environment {env_name} not found, waiting...")
                time.sleep(5)
                continue
            status = response["computeEnvironments"][0]["status"]
            logger.info(f"Compute environment status: {status}")
            if status == "VALID":
                logger.info(f"Compute environment {env_name} is VALID")
                return
            if status == "INVALID":
                status_reason = response["computeEnvironments"][0].get("statusReason", "Unknown")
                raise RuntimeError(f"Compute environment {env_name} is INVALID: {status_reason}")

            if status not in ("CREATING", "UPDATING"):
                raise RuntimeError(f"Compute environment {env_name} has unknown status: {status}")

            time.sleep(10)
        raise TimeoutError(f"Compute environment {env_name} did not become VALID within {timeout}s")

    def build_and_push_image(self) -> str:
        """Build Docker image and push to ECR."""
        import base64
        import subprocess

        ecr = self._session.client("ecr")
        repo_name = f"{self._prefix}-worker"

        # Create ECR repository if it doesn't exist
        try:
            ecr.create_repository(repositoryName=repo_name)
            logger.info(f"Created ECR repository: {repo_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "RepositoryAlreadyExistsException":
                raise
            logger.info(f"ECR repository already exists: {repo_name}")

        # Set lifecycle policy to keep only ECR_IMAGES_TO_KEEP latest images
        lifecycle_policy = {
            "rules": [
                {
                    "rulePriority": 1,
                    "description": f"Keep only {ECR_IMAGES_TO_KEEP} latest images",
                    "selection": {
                        "tagStatus": "any",
                        "countType": "imageCountMoreThan",
                        "countNumber": ECR_IMAGES_TO_KEEP,
                    },
                    "action": {"type": "expire"},
                }
            ]
        }
        try:
            ecr.put_lifecycle_policy(repositoryName=repo_name, lifecyclePolicyText=json.dumps(lifecycle_policy))
            logger.info(f"Set ECR lifecycle policy: keep {ECR_IMAGES_TO_KEEP} latest images")
        except ClientError as e:
            logger.warning(f"Failed to set lifecycle policy: {e}")

        # Get ECR login credentials
        auth = ecr.get_authorization_token()
        token = auth["authorizationData"][0]["authorizationToken"]
        registry = auth["authorizationData"][0]["proxyEndpoint"]

        # Decode credentials
        username, password = base64.b64decode(token).decode().split(":")

        # Docker login
        login_cmd = f"echo {password} | docker login --username {username} --password-stdin {registry}"
        subprocess.run(login_cmd, shell=True, check=True, capture_output=True)
        logger.info("Logged in to ECR")

        # Build image
        image_uri = f"{self._account_id}.dkr.ecr.{self._region}.amazonaws.com/{repo_name}:latest"
        dockerfile_path = Path(__file__).parent.parent / "remote" / "Dockerfile"

        # The Dockerfile COPYs paths of the form scaler/..., so the build context is the src/ directory.
        # This file is src/scaler/worker_manager/proxy/aws_batch/tools/provision_infrastructure.py, so
        # parents[5] is src/ (tools, aws_batch, proxy, worker_manager, scaler, src).
        build_context = Path(__file__).parents[5]

        # Build for linux/amd64 (EC2 runs on x86_64)
        build_cmd = [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "-f",
            str(dockerfile_path),
            "-t",
            image_uri,
            str(build_context),
        ]
        logger.info(f"Building image for linux/amd64: {image_uri}")
        subprocess.run(build_cmd, check=True)

        # Push image
        logger.info("Pushing image to ECR...")
        push_cmd = ["docker", "push", image_uri]
        subprocess.run(push_cmd, check=True)

        logger.info(f"Image pushed: {image_uri}")
        return image_uri

    def cleanup(self) -> None:
        """Delete all provisioned resources."""
        import time

        logger.info("Cleaning up AWS resources...")

        queue_name = f"{self._prefix}-queue"
        env_name = f"{self._prefix}-compute"
        job_def_name = f"{self._prefix}-job"
        role_name = f"{self._prefix}-job-role"
        instance_role_name = f"{self._prefix}-instance-role"
        instance_profile_name = f"{self._prefix}-instance-profile"
        bucket_name = f"{self._prefix}-{self._account_id}-{self._region}"

        # Disable and delete job queue
        try:
            logger.info(f"Disabling job queue: {queue_name}")
            self._batch.update_job_queue(jobQueue=queue_name, state="DISABLED")

            # Wait for queue to be disabled
            for _ in range(30):
                response = self._batch.describe_job_queues(jobQueues=[queue_name])
                if not response["jobQueues"]:
                    break
                status = response["jobQueues"][0]["status"]
                if status == "VALID":
                    state = response["jobQueues"][0]["state"]
                    if state == "DISABLED":
                        break
                elif status not in ("CREATING", "UPDATING", "INVALID"):
                    raise RuntimeError(f"Job queue {queue_name} has unknown status: {status}")
                time.sleep(2)

            self._batch.delete_job_queue(jobQueue=queue_name)
            logger.info(f"Deleted job queue: {queue_name}")

            # Wait for queue deletion
            for _ in range(30):
                response = self._batch.describe_job_queues(jobQueues=[queue_name])
                if not response["jobQueues"]:
                    break
                time.sleep(2)

        except ClientError as e:
            logger.warning(f"Failed to delete job queue: {e}")

        # Disable and delete compute environment
        try:
            logger.info(f"Disabling compute environment: {env_name}")
            self._batch.update_compute_environment(computeEnvironment=env_name, state="DISABLED")

            # Wait for compute env to be disabled
            for _ in range(60):
                response = self._batch.describe_compute_environments(computeEnvironments=[env_name])
                if not response["computeEnvironments"]:
                    break
                status = response["computeEnvironments"][0]["status"]
                if status == "VALID":
                    state = response["computeEnvironments"][0]["state"]
                    if state == "DISABLED":
                        break
                elif status not in ("CREATING", "UPDATING", "INVALID"):
                    raise RuntimeError(f"Compute environment {env_name} has unknown status: {status}")
                time.sleep(5)

            self._batch.delete_compute_environment(computeEnvironment=env_name)
            logger.info(f"Deleted compute environment: {env_name}")

            # Wait for compute env deletion
            for _ in range(60):
                response = self._batch.describe_compute_environments(computeEnvironments=[env_name])
                if not response["computeEnvironments"]:
                    break
                time.sleep(5)

        except ClientError as e:
            logger.warning(f"Failed to delete compute environment: {e}")

        # Deregister job definitions
        try:
            response = self._batch.describe_job_definitions(jobDefinitionName=job_def_name, status="ACTIVE")
            for job_def in response.get("jobDefinitions", []):
                self._batch.deregister_job_definition(jobDefinition=job_def["jobDefinitionArn"])
            logger.info(f"Deregistered job definitions: {job_def_name}")
        except ClientError as e:
            logger.warning(f"Failed to deregister job definitions: {e}")

        # Delete job IAM role
        try:
            self._iam.detach_role_policy(RoleName=role_name, PolicyArn=IAM_POLICY_ECS_TASK_EXECUTION)
        except ClientError:
            pass
        try:
            self._iam.delete_role_policy(RoleName=role_name, PolicyName=f"{self._prefix}-s3-policy")
        except ClientError:
            pass
        try:
            self._iam.delete_role(RoleName=role_name)
            logger.info(f"Deleted IAM role: {role_name}")
        except ClientError as e:
            logger.warning(f"Failed to delete IAM role: {e}")

        # Delete instance profile and role
        try:
            self._iam.remove_role_from_instance_profile(
                InstanceProfileName=instance_profile_name, RoleName=instance_role_name
            )
        except ClientError:
            pass
        try:
            self._iam.delete_instance_profile(InstanceProfileName=instance_profile_name)
            logger.info(f"Deleted instance profile: {instance_profile_name}")
        except ClientError as e:
            logger.warning(f"Failed to delete instance profile: {e}")

        try:
            self._iam.detach_role_policy(RoleName=instance_role_name, PolicyArn=IAM_POLICY_EC2_CONTAINER_SERVICE)
        except ClientError:
            pass
        try:
            self._iam.delete_role(RoleName=instance_role_name)
            logger.info(f"Deleted instance role: {instance_role_name}")
        except ClientError as e:
            logger.warning(f"Failed to delete instance role: {e}")

        # Delete S3 bucket (must be empty)
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    self._s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            self._s3.delete_bucket(Bucket=bucket_name)
            logger.info(f"Deleted S3 bucket: {bucket_name}")
        except ClientError as e:
            logger.warning(f"Failed to delete S3 bucket: {e}")

        # Delete ECR repository
        ecr_repo_name = f"{self._prefix}-worker"
        try:
            ecr = self._session.client("ecr")
            ecr.delete_repository(repositoryName=ecr_repo_name, force=True)
            logger.info(f"Deleted ECR repository: {ecr_repo_name}")
        except ClientError as e:
            logger.warning(f"Failed to delete ECR repository: {e}")

        logger.info("Cleanup complete!")


def main() -> None:
    """CLI for provisioning."""
    import argparse

    parser = argparse.ArgumentParser(description="Provision AWS Batch resources for Scaler")
    parser.add_argument("action", choices=["provision", "cleanup", "show", "build-image"], help="Action to perform")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="Resource name prefix")
    parser.add_argument("--image", default=None, help="Container image (default: builds and pushes to ECR)")
    parser.add_argument("--vcpus", type=int, default=1, help="vCPUs per job (integer for EC2)")
    parser.add_argument(
        "--memory", type=int, default=2048, help="Memory per job (MB, will use 90%% of nearest 2048MB multiple)"
    )
    parser.add_argument("--max-vcpus", type=int, default=256, help="Max vCPUs for compute env")
    parser.add_argument("--instance-types", default="default_x86_64", help="Comma-separated instance types")
    parser.add_argument("--job-timeout", type=int, default=60, help="Job timeout in minutes (default: 60 = 1 hour)")
    parser.add_argument(
        "--config",
        default="tests/worker_manager/proxy/aws_batch/.scaler_aws_batch_config.json",
        help="Config file path",
    )
    parser.add_argument(
        "--env-file", default="tests/worker_manager/proxy/aws_batch/.scaler_aws_hpc.env", help="Env file path"
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.action == "show":
        try:
            config = AWSBatchInfrastructureProvisioner.load_config(args.config)
            print("\n=== Saved AWS Batch Config ===")
            for key, value in config.items():
                print(f"  {key}: {value}")
            print(f"\nTo load env vars: source {args.env_file}")
        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("Run 'provision' first to create resources.")
        return

    provisioner = AWSBatchInfrastructureProvisioner(aws_region=args.region, prefix=args.prefix)

    if args.action == "build-image":
        image_uri = provisioner.build_and_push_image()
        print(f"\nImage URI: {image_uri}")
        print("\nTo use this image, run provision with:")
        print(f"  --image {image_uri}")
        return

    if args.action == "provision":
        # If no image specified, build and push to ECR
        if args.image is None:
            logger.info("No --image specified, building and pushing to ECR...")
            container_image = provisioner.build_and_push_image()
        else:
            container_image = args.image

        # Parse instance types
        instance_types = [t.strip() for t in args.instance_types.split(",")]

        result = provisioner.provision_all(
            container_image=container_image,
            vcpus=args.vcpus,
            memory_mb=args.memory,
            max_vcpus=args.max_vcpus,
            instance_types=instance_types,
            job_timeout_seconds=args.job_timeout * 60,  # convert minutes to seconds
        )
        AWSBatchInfrastructureProvisioner.save_config(result, args.config)
        AWSBatchInfrastructureProvisioner.save_env_file(result, args.env_file)
        print("\n=== Provisioned Resources ===")
        for key, value in result.items():
            print(f"  {key}: {value}")
        print(f"\nTo load env vars: source {args.env_file}")
    else:
        provisioner.cleanup()


if __name__ == "__main__":
    main()
