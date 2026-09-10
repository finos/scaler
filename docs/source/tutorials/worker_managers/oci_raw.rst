OCI Raw Worker Manager
======================

The OCI Raw worker manager provisions Scaler workers as `OCI Container Instances <https://www.oracle.com/cloud/compute/container-instances/>`_. Unlike the :doc:`OCI HPC worker manager <oci_hpc>`, which runs each Scaler *task* as a separate container, the OCI Raw worker manager launches full Scaler *worker processes* inside container instances. Workers connect back to the scheduler and process tasks the same way local workers do, with the scheduler handling load balancing and scaling.

Prerequisites
-------------

* An OCI account with a tenancy
* `OCI CLI <https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm>`_ installed and configured (``oci setup config``)
* Python packages: ``pip install opengris-scaler[oci]``
* A VCN with at least one subnet reachable from the machine running the scheduler
* An OCIR repository and a container image pushed to it (see `Build the Worker Image`_ below)

Quick Start
-----------

Install OCI CLI and configure credentials:

.. code-block:: bash

   bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)"
   oci setup config

Create a virtual environment and install Scaler with OCI extras:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[oci]

Gather your OCI resource identifiers:

.. code-block:: bash

   # Compartment OCID
   oci iam compartment list --query "data[0].id"

   # Availability domain name
   oci iam availability-domain list --query "data[0].name"

   # Subnet OCID (replace VCN_ID with your VCN OCID)
   oci network subnet list --compartment-id <COMPARTMENT_ID> --query "data[0].id"

Copy ``config.toml`` below, replace the placeholder values, then start services:

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml
         :caption: config.toml

         [object_storage_server]
         bind_address = "tcp://127.0.0.1:8517"

         [scheduler]
         bind_address = "tcp://0.0.0.0:8516"
         object_storage_address = "tcp://127.0.0.1:8517"

         [[worker_manager]]
         type = "oci_raw"
         scheduler_address = "tcp://127.0.0.1:8516"
         worker_scheduler_address = "tcp://<PUBLIC_IP>:8516"
         object_storage_address = "tcp://<PUBLIC_IP>:8517"
         worker_manager_id = "wm-oci-raw"
         oci_region = "us-ashburn-1"
         compartment_id = "ocid1.compartment.oc1..example"
         availability_domain = "AD-1"
         subnet_id = "ocid1.subnet.oc1..example"
         container_image = "us-ashburn-1.ocir.io/<namespace>/<repo>:latest"
         python_version = "3.12"
         requirements_txt = """
         opengris-scaler[oci]
         tomli
         pargraph
         """
         instance_ocpus = 4.0
         instance_memory_gb = 30.0

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_object_storage_server tcp://127.0.0.1:8517
         scaler_scheduler tcp://0.0.0.0:8516 \
             --object-storage-address tcp://127.0.0.1:8517 \
             --policy-content "allocate=even_load; scaling=vanilla"
         scaler_worker_manager oci_raw tcp://127.0.0.1:8516 \
             --worker-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --worker-manager-id wm-oci-raw \
             --oci-region us-ashburn-1 \
             --compartment-id ocid1.compartment.oc1..example \
             --availability-domain AD-1 \
             --subnet-id ocid1.subnet.oc1..example \
             --container-image us-ashburn-1.ocir.io/<namespace>/<repo>:latest \
             --python-version 3.12 \
             --requirements-txt "opengris-scaler[oci]" \
             --instance-ocpus 4.0 \
             --instance-memory-gb 30.0

After services are up, use a client to submit tasks to OCI-provisioned workers.

.. code-block:: python
   :caption: my_client.py (Terminal 3)

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<PUBLIC_IP>:8516") as client:
       futures = client.map(compute, range(50))
       print([f.result() for f in futures])

.. _oci_raw_build_worker_image:

Build the Worker Image
----------------------

A ``Dockerfile`` is provided at ``src/scaler/worker_manager/nested/oci/remote/Dockerfile``. It uses a minimal Debian base with ``uv`` for fast, wheel-based installs. The Scaler package and your task dependencies are installed at container startup via ``requirements_txt``, so the base image only needs ``uv`` and Bash.

Build and push to your OCIR repository from the repository root:

.. code-block:: bash

   # Authenticate with OCIR
   docker login us-ashburn-1.ocir.io -u <tenancy-namespace>/<username>

   # Build and push
   docker build \
       -f src/scaler/worker_manager/nested/oci/remote/Dockerfile \
       -t us-ashburn-1.ocir.io/<namespace>/<repo>:latest .
   docker push us-ashburn-1.ocir.io/<namespace>/<repo>:latest

.. note::
   The ``requirements_txt`` field in the worker manager config controls what Python packages are installed in the container when it starts. Include ``opengris-scaler[oci]`` and any packages your tasks depend on.

.. note::
   ``uv`` installs pre-built wheels by default, so no compiler is needed for most packages. If your dependencies include packages that must be compiled from source, add the required build tools to the Dockerfile (e.g. ``gcc``, ``g++``, or other system libraries your packages need):

   .. code-block:: dockerfile

      RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ ... && rm -rf /var/lib/apt/lists/*

How It Works
------------

1. The OCI Raw worker manager connects to the Scaler scheduler and sends periodic heartbeats.
2. On each heartbeat, the scheduler responds with a ``setDesiredTaskConcurrency`` command declaring the target worker count per capability set.
3. The worker manager converges by calling the OCI Container Instances API to launch or stop container instances.
4. Each container instance installs the packages from ``requirements_txt`` at startup, then runs ``scaler_worker_manager baremetal_native`` to spawn one or more worker processes. The number of workers per instance is determined by ``instance_ocpus``.
5. Workers connect back to the scheduler (via ``worker_scheduler_address``) and process tasks like local workers.

Configuration Reference
------------------------

OCI Raw Parameters
~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--worker-manager-id`` (``-wmi``, required): Unique identifier for this worker manager instance.
* ``--worker-scheduler-address``: Scheduler address used by workers inside container instances. Must be reachable from OCI (default: same as ``scheduler_address``).
* ``--object-storage-address``: Object storage address used by workers. Must be reachable from OCI.

Container Instance Config
~~~~~~~~~~~~~~~~~~~~~~~~~

* ``--oci-region`` (``oci_region``): OCI region identifier (default: ``us-ashburn-1``).
* ``--compartment-id`` (``compartment_id``, required): OCI Compartment OCID where container instances are launched.
* ``--availability-domain`` (``availability_domain``, required): OCI Availability Domain (e.g. ``AD-1`` or ``Uocm:US-ASHBURN-AD-1``).
* ``--subnet-id`` (``subnet_id``, required): Subnet OCID for container instance network interfaces.
* ``--container-image`` (``container_image``, required): OCIR image URI (e.g. ``us-ashburn-1.ocir.io/<ns>/<repo>:latest``).
* ``--instance-shape`` (``instance_shape``): Container instance shape (default: ``CI.Standard.E4.Flex``).
* ``--auth-type`` (``auth_type``): OCI authentication mode — ``config_file`` (default) or ``instance_principal``.
* ``--oci-profile`` (``oci_profile``): OCI config file profile name (default: ``DEFAULT``).

Python Worker Environment
~~~~~~~~~~~~~~~~~~~~~~~~~

* ``--python-version`` (``python_version``, required): Python version for workers (e.g. ``3.12``).
* ``--requirements-txt`` (``requirements_txt``, required): Packages to install in the container at startup. Must include ``opengris-scaler[oci]``. Can be an inline newline-separated string or a path to a file.

Sizing Parameters
~~~~~~~~~~~~~~~~~

* ``--instance-ocpus``: Number of OCPUs per container instance (default: ``4.0``). Also determines the number of worker processes started per instance.
* ``--instance-memory-gb``: Memory in GB per container instance (default: ``30.0``).

Common Parameters
~~~~~~~~~~~~~~~~~

For worker behavior, logging, and event loop options, see :doc:`common_parameters`.

Architecture
------------

.. code-block:: text

   ┌─────────┐     ┌───────────┐     ┌──────────────────────┐     ┌──────────────────────────┐
   │  Client │────>│ Scheduler │<───>│ OCI Raw WorkerManager│────>│ OCI Container Instances  │
   └─────────┘     └─────┬─────┘     └──────────────────────┘     └────────────┬─────────────┘
                         │                                                      │
                         │            ┌──────────────────┐                     │
                         └───────────>│  Object Storage  │<────────────────────┘
                                      └──────────────────┘       (scaler_worker_manager
                                                                  baremetal_native runs
                                                                  inside each instance)

1. The scheduler sends a ``setDesiredTaskConcurrency`` command to the worker manager on each heartbeat.
2. The worker manager calls the OCI Container Instances API to launch instances running ``scaler_worker_manager baremetal_native``.
3. Workers inside each instance connect back to the scheduler and process tasks.
4. When workers are no longer needed, the worker manager stops the corresponding container instances.

Troubleshooting
---------------

**Workers can't connect to scheduler:**
Container Instances run inside OCI's network. Ensure ``worker_scheduler_address`` is a public or OCI-internal IP reachable from your subnet, not ``127.0.0.1``. Check your VCN security list to allow inbound TCP on port 8516 from the container instance subnet.

**Container instances fail to start:**
Check OCI Console → Container Instances for error messages. Common causes: invalid subnet or compartment OCID, missing OCIR pull secret, or insufficient IAM permissions. Ensure your user/Dynamic Group has ``manage container-instances`` and ``read virtual-network-family`` policies in the compartment.

**``scaler_worker_manager`` not found in container:**
Ensure ``requirements_txt`` includes ``opengris-scaler[oci]``. The entrypoint installs it at container startup before launching workers.

**Image pull errors:**
Authenticate Docker to your OCIR region and ensure the image URI in ``container_image`` matches the pushed image exactly. For private repositories, confirm the container instance's subnet can reach the OCIR endpoint.
