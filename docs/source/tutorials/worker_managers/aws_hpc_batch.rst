AWS HPC Batch Worker Manager
============================

The AWS HPC worker manager offloads task execution to `AWS Batch <https://aws.amazon.com/batch/>`_, running each Scaler task as a containerized job on managed EC2 compute. Use this worker manager when you need to burst workloads to the cloud, access specific hardware (GPUs, high memory), or run long-running jobs at scale.

The worker manager is designed as an extensible HPC framework — AWS Batch is the currently supported backend.

Prerequisites
-------------

* An AWS account
* `AWS CLI <https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html>`_ installed and configured (``aws configure``)
* `Docker <https://docs.docker.com/get-docker/>`_ installed (for building the worker container image)
* Python 3.10+ with ``pip install opengris-scaler[aws]``

.. note::
   Building from source requires C++ build tools (``cmake``, ``ninja``, ``pkg-config``, and a C++20 compiler).
   On macOS: ``brew install ninja pkg-config``.
   Alternatively, use the project's devcontainer which has all dependencies pre-installed:

   .. code-block:: bash

      docker build -t scaler-dev -f .devcontainer/Dockerfile .
      docker run -it --rm -v $(pwd):/workspace -w /workspace scaler-dev bash

.. _aws_hpc_batch_quick_start:

Quick Start
-----------

Install AWS CLI v2:

.. warning::

   Do not use ``pip install awscli`` for this setup. That installs AWS CLI v1.
   Use the official AWS CLI v2 installer instead.

.. tabs::

   .. group-tab:: Linux x86_64

      .. code-block:: bash

         curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
         unzip awscliv2.zip
         sudo ./aws/install

         aws --version

   .. group-tab:: Linux ARM64

      .. code-block:: bash

         curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
         unzip awscliv2.zip
         sudo ./aws/install

Create a virtual environment, install Scaler AWS extras, and authenticate:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[aws]

.. tabs::

   .. group-tab:: Local Machine

      .. code-block:: bash

         aws login

      Click the page link and proceed in your default browser to sign in, then
      follow the AWS CLI instructions in the terminal.

   .. group-tab:: Remote Server

      .. code-block:: bash

         aws login --remote

      Open the URL shown by the command in your local browser, complete
      sign-in, then copy the returned code/token and paste it back into the
      remote terminal to complete authentication.

Provision required AWS resources (S3 bucket, IAM roles, compute environment,
job queue, and job definition):

.. code-block:: bash

   python -m scaler.worker_manager.proxy.aws_batch.tools.provision_infrastructure provision --region us-east-1 --prefix scaler-batch --vcpus 1 --memory 2048 --max-vcpus 256
   source tests/worker_manager/proxy/aws_batch/.scaler_aws_hpc.env

The provisioner also builds and pushes the worker image from ``Dockerfile.batch``.
The image must use the same Python version as the client and include ``cloudpickle`` and ``boto3``.
The provisioner also creates required AWS resources and applies required IAM
permissions. See :ref:`aws_hpc_required_resources` and
:ref:`aws_hpc_required_iam_permissions`.

Start scheduler and worker manager from one TOML file (replace account ID):

.. code-block:: toml
   :caption: config.toml

   [object_storage_server]
   bind_address = "tcp://127.0.0.1:2346"

   [scheduler]
   bind_address = "tcp://127.0.0.1:2345"
   object_storage_address = "tcp://127.0.0.1:2346"

   [[worker_manager]]
   type = "aws_hpc"
   scheduler_address = "tcp://127.0.0.1:2345"
   object_storage_address = "tcp://127.0.0.1:2346"
   worker_manager_id = "wm-batch"
   job_queue = "scaler-batch-queue"
   job_definition = "scaler-batch-job"
   s3_bucket = "scaler-batch-123456789012-us-east-1"  # replace 123456789012 with your account ID
   aws_region = "us-east-1"
   max_concurrent_jobs = 100
   job_timeout_minutes = 60

.. code-block:: bash

   scaler config.toml

.. code-block:: python
   :caption: my_client.py (Terminal 2)

   from scaler import Client

   def heavy_computation(x):
       return x ** 2

   with Client(address="tcp://127.0.0.1:2345") as client:
       results = client.map(heavy_computation, range(50))
       print(results)

Detailed Setup
--------------

Step 1: Provision AWS Resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::
   The provisioner creates resources for quick testing and development only.
   For production deployments, use your organization's infrastructure-as-code tools
   (CloudFormation, CDK, Terraform) with proper security configurations.

Scaler includes a provisioner script that creates all required AWS infrastructure (S3 bucket, IAM roles, EC2 compute environment, job queue, job definition, and ECR repository):

.. code-block:: bash

   python -m scaler.worker_manager.proxy.aws_batch.tools.provision_infrastructure provision \
       --region us-east-1 \
       --prefix scaler-batch \
       --vcpus 1 \
       --memory 2048 \
       --max-vcpus 256

This will:

1. Build and push a Docker worker image to ECR
2. Create an S3 bucket for task payloads and results (with 1-day lifecycle policy)
3. Create IAM roles with the minimum required permissions
4. Create an EC2 compute environment and job queue
5. Register a Batch job definition

The provisioner saves its configuration to:

* ``tests/worker_manager/proxy/aws_batch/.scaler_aws_hpc.env`` — shell environment file
* ``tests/worker_manager/proxy/aws_batch/.scaler_aws_batch_config.json`` — full resource details (used for cleanup)

Source the env file to set variables for subsequent commands:

.. code-block:: bash

   source tests/worker_manager/proxy/aws_batch/.scaler_aws_hpc.env

**Memory configuration:** Memory is rounded to the nearest multiple of 2048 MB and 90% is allocated to the container. For example, ``--memory 4000`` → 4096 MB total → 3686 MB effective.

Using Existing Infrastructure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already have AWS Batch resources (created via CloudFormation, CDK, Terraform, etc.), skip the provisioner and create the env file manually:

.. code-block:: bash

   cat > .scaler_aws_hpc.env << 'EOF'
   export SCALER_AWS_REGION="us-east-1"
   export SCALER_S3_BUCKET="your-existing-bucket"
   export SCALER_JOB_QUEUE="your-existing-queue"
   export SCALER_JOB_DEFINITION="your-existing-job-def"
   EOF
   source .scaler_aws_hpc.env

Then continue from Step 2.

Step 2: Set Up the Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Option A: Native install (macOS/Linux)**

.. code-block:: bash

   pip install opengris-scaler[aws]

On macOS, you may need build tools: ``brew install ninja pkg-config``.

**Option B: Devcontainer (recommended for development/testing)**

The devcontainer has all C++ build tools and dependencies pre-installed.

1. Build the devcontainer image (on host):

.. code-block:: bash

   docker build -t scaler-dev -f .devcontainer/Dockerfile .

2. Export AWS credentials and start the container:

.. code-block:: bash

   # If using assumed roles (isengardcli, SSO, etc.)
   eval $(aws configure export-credentials --format env)

   docker run -it --rm \
       -e AWS_ACCESS_KEY_ID \
       -e AWS_SECRET_ACCESS_KEY \
       -e AWS_SESSION_TOKEN \
       -e AWS_DEFAULT_REGION=us-east-1 \
       -v $(pwd):/workspace -w /workspace scaler-dev bash

   # If using static credentials (~/.aws/credentials)
   # docker run -it --rm -v ~/.aws:/root/.aws:ro \
   #     -v $(pwd):/workspace -w /workspace scaler-dev bash

3. Install inside the container:

.. code-block:: bash

   python3 -m venv .venv
   source .venv/bin/activate
   pip install -e ".[dev]"
   pip install boto3

.. note::
   Session credentials expire (typically 1 hour). If you get credential errors, exit the container, re-export credentials on the host, and restart the container.

Step 3: Start All Processes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use a single TOML configuration file to start the object storage server, scheduler, and worker manager together.

1. Source the provisioned config and generate the TOML:

.. code-block:: bash

   source tests/worker_manager/proxy/aws_batch/.scaler_aws_hpc.env
   sed -e "s|scaler-batch-queue|${SCALER_JOB_QUEUE}|" \
       -e "s|scaler-batch-job|${SCALER_JOB_DEFINITION}|" \
       -e "s|scaler-batch-ACCOUNT_ID-us-east-1|${SCALER_S3_BUCKET}|" \
       -e "s|aws_region = \"us-east-1\"|aws_region = \"${SCALER_AWS_REGION}\"|" \
       tests/worker_manager/proxy/aws_batch/scaler_aws_batch.toml > /tmp/config.toml

2. Start all processes:

.. code-block:: bash

   scaler /tmp/config.toml

See ``tests/worker_manager/proxy/aws_batch/scaler_aws_batch.toml`` for the template.

Alternatively, start each process separately:

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:2346 &
   scaler_scheduler tcp://0.0.0.0:2345 --object-storage-address tcp://127.0.0.1:2346 &
   scaler_worker_manager aws_hpc tcp://127.0.0.1:2345 -wmi wm-batch \
       --job-queue "$SCALER_JOB_QUEUE" --job-definition "$SCALER_JOB_DEFINITION" \
       --s3-bucket "$SCALER_S3_BUCKET" --aws-region "$SCALER_AWS_REGION" &

.. note::
   The scheduler address must be reachable from the machine running the AWS HPC worker manager. Use ``0.0.0.0`` to bind to all interfaces, or your machine's public/private IP.

Step 4: Run Tests
~~~~~~~~~~~~~~~~~

With all processes running (from Step 3), submit tasks:

.. code-block:: bash

   python tests/worker_manager/proxy/aws_batch/aws_batch_test_harness.py \
       --scheduler tcp://127.0.0.1:2345 --test all

Or use the Scaler client directly:

.. code-block:: python

   from scaler import Client

   def heavy_computation(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_IP>:2345") as client:
       futures = client.map(heavy_computation, range(50))
       results = [f.result() for f in futures]
       print(results)

Cleanup
~~~~~~~

To tear down all provisioned AWS resources:

.. code-block:: bash

   python -m scaler.worker_manager.proxy.aws_batch.tools.provision_infrastructure cleanup \
       --region us-east-1 \
       --prefix scaler-batch

How It Works
------------

1. The worker manager connects to the Scaler scheduler as a worker and receives tasks.
2. Each task is serialized with ``cloudpickle`` and either passed inline (≤ 28 KB) or uploaded to S3.
3. When multiple tasks arrive within a short window (0.5s), they are automatically batched into a single AWS Batch **array job**, reducing API calls from N to 1. Single tasks are submitted individually.
4. Inside the Batch container, a runner script (``batch_job_runner.py``) deserializes the task, executes the function, and writes the result to S3. For array jobs, each child container uses its ``AWS_BATCH_JOB_ARRAY_INDEX`` to pick the correct payload.
5. The worker manager polls for job completion, fetches the result from S3, and returns it to the scheduler.

A semaphore limits concurrent Batch jobs (``--max-concurrent-jobs``) to prevent exceeding AWS service quotas. All AWS API calls run in a thread pool to avoid blocking the heartbeat loop.

Configuration Reference
------------------------

AWS HPC Parameters
~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--worker-manager-id`` (``-wmi``, required): Unique identifier for this worker manager instance.
* ``--job-queue`` (``-q``, required): AWS Batch job queue name.
* ``--job-definition`` (``-d``, required): AWS Batch job definition name.
* ``--s3-bucket`` (required): S3 bucket for task payloads and results.
* ``--aws-region``: AWS region (default: ``us-east-1``).
* ``--s3-prefix``: S3 key prefix (default: ``scaler-tasks``).
* ``--max-concurrent-jobs`` (``-mcj``): Max concurrent Batch jobs (default: ``100``).
* ``--job-timeout-minutes``: Max job runtime in minutes (default: ``60``).
* ``--backend`` (``-b``): HPC backend (default: ``batch``).
* ``--name`` (``-n``): Custom name for the worker manager instance.

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behavior, logging, and event loop options, see :doc:`common_parameters`.

Provisioner Reference
---------------------

The provisioner supports these commands:

.. code-block:: text

   provision      Create all AWS resources and push Docker image
   cleanup        Tear down all AWS resources
   show           Display saved configuration
   build-image    Build and push Docker image only

.. list-table:: Provisioner Flags
   :header-rows: 1
   :widths: 25 15 60

   * - Flag
     - Default
     - Description
   * - ``--region``
     - ``us-east-1``
     - AWS region
   * - ``--prefix``
     - ``scaler-batch``
     - Resource name prefix
   * - ``--image``
     - (auto-build)
     - Container image URI (if provided, skips Docker build and push to ECR)
   * - ``--vcpus``
     - ``1``
     - vCPUs per job
   * - ``--memory``
     - ``2048``
     - Memory per job in MB (uses 90% of nearest 2048 MB multiple)
   * - ``--max-vcpus``
     - ``256``
     - Max vCPUs for compute environment
   * - ``--instance-types``
     - ``default_x86_64``
     - EC2 instance types (comma-separated)
   * - ``--job-timeout``
     - ``60``
     - Job timeout in minutes

Architecture
------------

.. code-block:: text

   ┌─────────┐     ┌───────────┐     ┌───────────────┐     ┌──────────────────────────┐     ┌───────────┐
   │  Client  │────>│ Scheduler │────>│ WorkerProcess │────>│ AWSBatchExecutionBackend │────>│ AWS Batch │
   └─────────┘     └───────────┘     └───────────────┘     └──────────────────────────┘     └───────────┘
                                              │                                                    │
                                              v                                                    v
                                     ┌─────────────────┐                                   ┌───────────┐
                                     │HeartbeatManager │                                   │ S3 Bucket │
                                     └─────────────────┘                                   └───────────┘

.. list-table:: Components
   :header-rows: 1
   :widths: 25 75

   * - Component
     - Description
   * - **Client**
     - Submits tasks to the scheduler using the Scaler API
   * - **Scheduler**
     - Distributes tasks to available workers via ZMQ streaming
   * - **WorkerProcess**
     - Shared process class that connects to the scheduler, manages the heartbeat loop, and routes messages to the execution backend
   * - **AWSBatchExecutionBackend**
     - Handles AWS Batch job submission, array job batching, S3 payload storage, job monitoring, and result fetching
   * - **HeartbeatManager**
     - Sends periodic heartbeats to the scheduler with worker status
   * - **S3 Bucket**
     - Stores task payloads (for large tasks) and job results
   * - **AWS Batch**
     - Executes tasks as containerized jobs on an EC2 compute environment

Payload Handling
~~~~~~~~~~~~~~~~

Task payloads are serialized with ``cloudpickle`` and delivered to AWS Batch jobs.
Payloads larger than 4 KB are gzip-compressed before transfer. The resulting payload
(compressed or not) is passed inline via job parameters if it fits within 28 KiB;
otherwise it is uploaded to S3.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Condition
     - Method
   * - Raw payload ≤ 4 KB
     - Inline (uncompressed)
   * - Raw payload > 4 KB, compressed ≤ 28 KB
     - Inline (gzip compressed)
   * - Raw payload > 4 KB, compressed > 28 KB
     - S3 upload

Troubleshooting
---------------

**Jobs stuck in RUNNABLE:**
Check that your compute environment has sufficient capacity (``--max-vcpus``) and that subnets have internet access for pulling container images.

**Permission errors:**
Ensure the IAM role attached to the job definition has S3 read/write access to the task bucket. The provisioner creates this automatically.

**Credential expiration:**
The worker manager auto-refreshes expired AWS credentials. If using temporary credentials, ensure your session token is valid.

**Container image issues:**
Your job definition image must have the same Python version as the client (required for ``cloudpickle`` compatibility), plus ``cloudpickle`` and ``boto3`` installed.

**Timeout waiting for result:**
Check the AWS Batch console for job status. Increase ``--max-concurrent-jobs`` if jobs are queued, or check compute environment capacity.

.. _aws_hpc_required_resources:

Required AWS Resources
----------------------

The provisioner creates these resources automatically:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Resource
     - Name pattern
     - Purpose
   * - S3 bucket
     - ``{prefix}-{account_id}-{region}``
     - Task payloads and results (1-day lifecycle)
   * - IAM job role
     - ``{prefix}-job-role``
     - Assumed by Batch container tasks
   * - IAM instance role
     - ``{prefix}-instance-role``
     - Assumed by EC2 instances in compute environment
   * - EC2 instance profile
     - ``{prefix}-instance-profile``
     - Wraps instance role for EC2
   * - ECR repository
     - ``{prefix}-worker``
     - Stores worker Docker image
   * - Compute environment
     - ``{prefix}-compute``
     - Managed EC2 fleet
   * - Job queue
     - ``{prefix}-queue``
     - Routes jobs to compute environment
   * - Job definition
     - ``{prefix}-job``
     - Container spec and entrypoint
   * - CloudWatch log group
     - ``/aws/batch/job``
     - Job logs (30-day retention)

.. _aws_hpc_required_iam_permissions:

Required AWS IAM Permissions
----------------------------

1. Your IAM user/role (to run the provisioner and worker manager)

* **S3**: ``s3:CreateBucket``, ``s3:PutObject``, ``s3:GetObject``, ``s3:DeleteObject``, ``s3:PutLifecycleConfiguration``
* **IAM**: ``iam:CreateRole``, ``iam:AttachRolePolicy``, ``iam:PutRolePolicy``, ``iam:CreateInstanceProfile``, ``iam:AddRoleToInstanceProfile``, ``iam:GetRole``, ``iam:PassRole``
* **Batch**: ``batch:CreateComputeEnvironment``, ``batch:CreateJobQueue``, ``batch:RegisterJobDefinition``, ``batch:SubmitJob``, ``batch:DescribeJobs``, ``batch:DescribeComputeEnvironments``, ``batch:DescribeJobQueues``, ``batch:TerminateJob``, ``batch:DeregisterJobDefinition``
* **ECR**: ``ecr:CreateRepository``, ``ecr:GetAuthorizationToken``, ``ecr:PutLifecyclePolicy``, ``ecr:BatchDeleteImage``
* **EC2**: ``ec2:DescribeSubnets``, ``ec2:DescribeSecurityGroups``
* **CloudWatch Logs**: ``logs:CreateLogGroup``, ``logs:PutRetentionPolicy``, ``logs:GetLogEvents``

Quick-setup managed policies: ``AmazonS3FullAccess``, ``AWSBatchFullAccess``,
``AmazonEC2ContainerRegistryFullAccess``, ``IAMFullAccess``, ``CloudWatchLogsFullAccess``.

2. Batch job role (``{prefix}-job-role``, assumed by ``ecs-tasks.amazonaws.com``)

* ``AmazonECSTaskExecutionRolePolicy`` (managed; covers ECR pulls and CloudWatch logs writes)
* Inline S3 policy: ``s3:GetObject``, ``s3:PutObject``, ``s3:DeleteObject`` on ``arn:aws:s3:::{bucket}/{prefix}/*``

3. EC2 instance role (``{prefix}-instance-role``, assumed by ``ec2.amazonaws.com``)

* ``AmazonEC2ContainerServiceforEC2Role`` (managed; allows EC2 to register with ECS/Batch, pull images, report status)
