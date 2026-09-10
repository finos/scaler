OCI HPC Worker Manager
======================

The OCI HPC worker manager offloads task execution to `OCI Container Instances <https://www.oracle.com/cloud/compute/container-instances/>`_, running each Scaler task as a dedicated containerized job. Unlike the :doc:`OCI Raw worker manager <oci_raw>`, which launches long-running worker processes, the OCI HPC worker manager creates a new Container Instance for every task. Task payloads and results are relayed through OCI Object Storage. Use this worker manager when you need per-task isolation, access to high-memory or specialized hardware shapes, or want to burst large workloads to OCI.

Prerequisites
-------------

* An OCI account with a tenancy
* `OCI CLI <https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm>`_ installed and configured (``oci setup config``)
* `Docker <https://docs.docker.com/get-docker/>`_ installed (for building the worker container image)
* Python packages: ``pip install opengris-scaler[oci]``
* An OCI Object Storage bucket for task payloads and results
* An OCIR repository for the job runner image

Quick Start
-----------

Install OCI CLI and configure credentials:

.. code-block:: bash

   bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)"
   oci setup config

Install Scaler with OCI extras:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[oci]

Provision required OCI resources (Object Storage bucket, Dynamic Group, IAM policies, OCIR repository):

.. code-block:: bash

   python -m scaler.worker_manager.proxy.oci.tools.provision_infrastructure provision \
       --compartment-id ocid1.compartment.oc1..example \
       --region us-ashburn-1 \
       --availability-domain AD-1 \
       --subnet-id ocid1.subnet.oc1..example

This writes a ``.scaler_oci_config.json`` file in the current directory. Build and push the job runner image:

.. code-block:: bash

   python -m scaler.worker_manager.proxy.oci.tools.provision_infrastructure \
       build-image --config .scaler_oci_config.json

Validate all phases with the test harness:

.. code-block:: bash

   python tests/worker_manager/proxy/oci/oci_test_harness.py \
       --config .scaler_oci_config.json

Start all services:

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml
         :caption: config.toml

         [object_storage_server]
         bind_address = "tcp://127.0.0.1:8517"

         [scheduler]
         bind_address = "tcp://127.0.0.1:8516"
         object_storage_address = "tcp://127.0.0.1:8517"

         [[worker_manager]]
         type = "oci_hpc"
         scheduler_address = "tcp://127.0.0.1:8516"
         object_storage_address = "tcp://127.0.0.1:8517"
         worker_manager_id = "wm-oci-hpc"
         object_storage_namespace = "<tenancy-namespace>"
         object_storage_bucket = "scaler-tasks"
         oci_region = "us-ashburn-1"
         compartment_id = "ocid1.compartment.oc1..example"
         availability_domain = "AD-1"
         subnet_id = "ocid1.subnet.oc1..example"
         container_image = "us-ashburn-1.ocir.io/<namespace>/<repo>:latest"
         base_concurrency = 100
         job_timeout_seconds = 3600

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_object_storage_server tcp://127.0.0.1:8517
         scaler_scheduler tcp://0.0.0.0:8516 \
             --object-storage-address tcp://127.0.0.1:8517
         scaler_worker_manager oci_hpc tcp://127.0.0.1:8516 \
             --worker-manager-id wm-oci-hpc \
             --object-storage-address tcp://127.0.0.1:8517 \
             --object-storage-namespace "<tenancy-namespace>" \
             --object-storage-bucket scaler-tasks \
             --oci-region us-ashburn-1 \
             --compartment-id ocid1.compartment.oc1..example \
             --availability-domain AD-1 \
             --subnet-id ocid1.subnet.oc1..example \
             --container-image us-ashburn-1.ocir.io/<namespace>/<repo>:latest \
             --base-concurrency 100 \
             --job-timeout-seconds 3600

.. code-block:: python
   :caption: my_client.py (Terminal 2)

   from scaler import Client

   def heavy_computation(x):
       return x ** 2

   with Client(address="tcp://127.0.0.1:8516") as client:
       futures = client.map(heavy_computation, range(50))
       print([f.result() for f in futures])

Using Existing Infrastructure
-----------------------------

If you already have OCI resources, skip the provisioner and set environment variables directly:

.. code-block:: bash

   export OCI_REGION="us-ashburn-1"
   export OCI_COMPARTMENT_ID="ocid1.compartment.oc1..example"
   export OCI_AVAILABILITY_DOMAIN="AD-1"
   export OCI_SUBNET_ID="ocid1.subnet.oc1..example"
   export OCI_CONTAINER_IMAGE="us-ashburn-1.ocir.io/<namespace>/<repo>:latest"
   export OCI_OBJECT_STORAGE_NAMESPACE="<tenancy-namespace>"
   export OCI_OBJECT_STORAGE_BUCKET="scaler-tasks"

How It Works
------------

1. The worker manager connects to the Scaler scheduler as a worker and receives tasks.
2. Each task is serialized with ``cloudpickle`` and uploaded to OCI Object Storage.
3. A new Container Instance is created for each task. The job runner script inside the container fetches the payload from Object Storage, deserializes and executes the function, and writes the result back to Object Storage.
4. The worker manager polls for container completion, fetches the result from Object Storage, and returns it to the scheduler.
5. A semaphore limits concurrent Container Instances (``base_concurrency``) to prevent exceeding OCI service limits.

Container instances authenticate to Object Storage via OCI Resource Principals — the container does not need credentials embedded in the image.

Configuration Reference
------------------------

OCI HPC Parameters
~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--worker-manager-id`` (``-wmi``, required): Unique identifier for this worker manager instance.
* ``--object-storage-address``: Scaler Object Storage address (used by the worker manager itself).
* ``--object-storage-namespace`` (required): OCI Object Storage tenancy namespace.
* ``--object-storage-bucket`` (required): OCI Object Storage bucket name for task payloads and results.
* ``--object-storage-prefix``: Key prefix for task objects (default: ``scaler-tasks``).
* ``--base-concurrency`` (``-bc``): Maximum number of concurrently running container instances (default: ``100``).
* ``--job-timeout-seconds``: Maximum runtime in seconds for a single container instance (default: ``3600``).

Container Instance Config
~~~~~~~~~~~~~~~~~~~~~~~~~

* ``--oci-region``: OCI region identifier (default: ``us-ashburn-1``).
* ``--compartment-id`` (required): OCI Compartment OCID where container instances are launched.
* ``--availability-domain`` (required): OCI Availability Domain (e.g. ``AD-1`` or ``Uocm:US-ASHBURN-AD-1``).
* ``--subnet-id`` (required): Subnet OCID for container instance network interfaces.
* ``--container-image`` (required): OCIR image URI for the job runner container.
* ``--instance-shape``: Container instance shape (default: ``CI.Standard.E4.Flex``).
* ``--auth-type``: OCI authentication mode — ``config_file`` (default) or ``instance_principal``.
* ``--oci-profile``: OCI config file profile name (default: ``DEFAULT``).

Sizing Parameters
~~~~~~~~~~~~~~~~~

* ``--instance-ocpus``: Number of OCPUs per container instance (default: ``1.0``).
* ``--instance-memory-gb``: Memory in GB per container instance (default: ``6.0``).

Common Parameters
~~~~~~~~~~~~~~~~~

For worker behavior, logging, and event loop options, see :doc:`common_parameters`.

Provisioner Reference
---------------------

The provisioner supports these commands:

.. code-block:: text

   provision      Create all OCI resources and generate config file
   cleanup        Tear down all OCI resources
   build-image    Build and push job runner Docker image

.. list-table:: Provisioner Flags
   :header-rows: 1
   :widths: 30 15 55

   * - Flag
     - Default
     - Description
   * - ``--compartment-id``
     - (required)
     - OCI Compartment OCID
   * - ``--region``
     - ``us-ashburn-1``
     - OCI region identifier
   * - ``--availability-domain``
     - (required)
     - OCI Availability Domain
   * - ``--subnet-id``
     - (required)
     - OCI Subnet OCID
   * - ``--config``
     - ``.scaler_oci_config.json``
     - Path to saved provisioner config (used by ``build-image`` and ``cleanup``)

Architecture
------------

.. code-block:: text

   ┌─────────┐     ┌───────────┐     ┌──────────────────────┐     ┌──────────────────────────┐
   │  Client │────>│ Scheduler │────>│  OCI HPC WorkerProc  │────>│ OCI Container Instances  │
   └─────────┘     └───────────┘     └──────────────────────┘     └────────────┬─────────────┘
                                                │                               │
                                                v                               v
                                       ┌─────────────────┐             ┌──────────────────┐
                                       │ HeartbeatManager│             │  Object Storage  │
                                       └─────────────────┘             └──────────────────┘

.. list-table:: Components
   :header-rows: 1
   :widths: 30 70

   * - Component
     - Description
   * - **Client**
     - Submits tasks to the scheduler using the Scaler API
   * - **Scheduler**
     - Distributes tasks to available workers via ZMQ streaming
   * - **OCI HPC WorkerProc**
     - Connects to the scheduler, receives tasks, submits them as Container Instance jobs, and returns results
   * - **Object Storage**
     - Relays task payloads (serialized with ``cloudpickle``) and results between the worker manager and container instances
   * - **OCI Container Instances**
     - Runs the job runner script per task; authenticates to Object Storage via Resource Principals

Payload Handling
~~~~~~~~~~~~~~~~

Task payloads are serialized with ``cloudpickle`` and uploaded to OCI Object Storage. The container instance job runner downloads the payload, executes the task, and writes the result back to Object Storage. The worker manager fetches the result and returns it to the scheduler.

.. note::
   The provisioner sets a 7-day lifecycle policy on the Object Storage bucket. Ensure this is at least as long as ``job_timeout_seconds`` (default: 1 hour). Objects that expire before the worker manager fetches the result will cause the task to fail silently.

Troubleshooting
---------------

**Container instances created but result never returns:**
Check container logs in OCI Console → Container Instances → your instance → Logs. Verify that the Object Storage bucket lifecycle is at least as long as ``job_timeout_seconds``.

**Permission errors when launching container instances:**
Ensure your IAM user or Dynamic Group has ``manage container-instances`` and ``manage virtual-network-family`` policies in the compartment. The provisioner creates these automatically.

**Resource Principal errors inside the container:**
The job runner uses OCI Resource Principals to access Object Storage. Ensure the container instance's compartment has a Dynamic Group policy that grants ``manage objects`` on the bucket. The provisioner sets this up automatically.

**Jobs stuck in CREATING:**
Check OCI service limits for Container Instances in your region and availability domain. If you have hit the limit, reduce ``base_concurrency`` or request a limit increase.

**Container image pull errors:**
Ensure the OCIR repository is in the same region and that the container instance's subnet can reach the OCIR endpoint. For private repositories, ensure the OCIR pull secret is accessible.
