Kubernetes Worker Manager
=========================

The Kubernetes worker manager (type ``k8s_raw``) provisions Scaler workers as **bare
Kubernetes Pods** --- no Deployment, Job, or StatefulSet controller sits between Scaler
and the pods. This gives Scaler complete, deterministic control over exactly which pods
are alive at any point in time, making scale-up and scale-down as predictable as possible.

Each pod runs the existing ``scaler_worker_manager baremetal_native --mode fixed``
entrypoint --- the same pattern used by other raw worker managers --- so the Kubernetes
layer is purely responsible for pod lifecycle, while the in-pod native manager handles
the actual Scaler workers.

Prerequisites
-------------

* A running Kubernetes cluster (1.28+) with ``kubectl`` access configured.
* `Python package <https://pypi.org/project/kubernetes/>`_ for the Kubernetes client:

  .. code-block:: bash

     pip install opengris-scaler[kubernetes]

* RBAC permissions granting the service account ``get``, ``list``, ``watch``,
  ``create``, and ``delete`` on ``pods`` in the target namespace.
  See `RBAC Example`_ below for a ready-to-apply manifest.

* A container image pushed to a registry reachable from the cluster.  The image must
  include ``uv`` or ``pip`` so that Python requirements can be installed at container
  start-up (the same mechanism used by other raw worker managers).

  The repository includes a ready-made Dockerfile at ``docker/Dockerfile`` that builds
  a minimal Alpine image with ``uv`` pre-installed.  The accompanying
  ``docker/entrypoint.sh`` reads the ``PYTHON_VERSION``, ``PYTHON_REQUIREMENTS``, and
  ``COMMAND`` environment variables that the k8s_raw worker manager injects into each
  pod automatically.  To build and push the image:

  .. code-block:: bash

     docker build -t myregistry.example.com/scaler:latest -f docker/Dockerfile .
     docker push myregistry.example.com/scaler:latest

Quick Start
-----------

Create a virtual environment and install Scaler with the Kubernetes extra:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[kubernetes]

Verify your cluster access:

.. code-block:: bash

   kubectl cluster-info
   kubectl get namespace scaler   # or whichever namespace you plan to use

Copy the ``config.toml`` below, replace the placeholder image URI, then start services:

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml
         :caption: config.toml

         [object_storage_server]
         bind_address = "tcp://0.0.0.0:8517"

         [scheduler]
         bind_address = "tcp://0.0.0.0:8516"
         object_storage_address = "tcp://scheduler-service:8517"

         [[worker_manager]]
         type = "k8s_raw"
         scheduler_address = "tcp://scheduler-service:8516"
         worker_manager_id = "wm-k8s-01"
         max_task_concurrency = 80

         namespace = "scaler"
         pod_image = "myregistry.example.com/scaler:latest"
         workers_per_pod = 4
         delete_grace_period_seconds = 60

         [worker_manager.node_selector]
         nodepool = "compute"

         [worker_manager.resource_requests]
         cpu = "4"
         memory = "16Gi"

         requirements_txt = "opengris-scaler"
         python_version = "3.12.11"

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_object_storage_server tcp://0.0.0.0:8517
         scaler_scheduler tcp://0.0.0.0:8516 \
             --object-storage-address tcp://scheduler-service:8517 \
             --policy-content "allocate=even_load; scaling=vanilla"
         scaler_worker_manager k8s_raw tcp://scheduler-service:8516 \
             --worker-manager-id wm-k8s-01 \
             --max-task-concurrency 80 \
             --namespace scaler \
             --pod-image myregistry.example.com/scaler:latest \
             --workers-per-pod 4 \
             --delete-grace-period-seconds 60

After services are up, use a client to submit tasks to Kubernetes-provisioned workers:

.. code-block:: python
   :caption: my_client.py

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_IP>:8516") as client:
       futures = client.map(compute, range(80))
       print([f.result() for f in futures])

Configuration Reference
-----------------------

Core
~~~~

These fields are part of the embedded ``WorkerManagerConfig`` and appear at the top
level of the ``[[worker_manager]]`` TOML section.

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``scheduler_address``
     - ``str``
     - *(required)*
     - Address of the Scaler scheduler that this worker manager connects to
       (e.g. ``tcp://scheduler-service:8516``).
   * - ``worker_manager_id``
     - ``str``
     - *(required)*
     - Stable, unique identifier for this worker manager instance.
       The scheduler uses it to associate workers with their manager.
   * - ``max_task_concurrency``
     - ``int``
     - CPU count
     - Maximum total number of Scaler worker processes across all pods.
       The number of pods launched is ``ceil(max_task_concurrency / workers_per_pod)``.
       Set to ``-1`` for no limit.

Authentication
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``kubeconfig_path``
     - ``str``
     - ``""``
     - Path to a kubeconfig file. Empty string activates in-cluster authentication
       (reads the service account token mounted at
       ``/var/run/secrets/kubernetes.io/serviceaccount``).
       Set this to a file path when running the worker manager *outside* the cluster.
   * - ``namespace``
     - ``str``
     - ``"default"``
     - Kubernetes namespace in which worker Pods are created and deleted.

Pod Image
~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``pod_image``
     - ``str``
     - *(required)*
     - Container image used for worker Pods
       (e.g. ``myregistry.example.com/scaler-worker:latest``).
       The image must provide the ``scaler_worker_manager`` entry point and a package
       manager (``uv`` or ``pip``) so that runtime requirements can be installed.

Sizing
~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``workers_per_pod``
     - ``int``
     - ``1``
     - Number of Scaler worker processes launched inside each Pod.
       Acts as the divisor when converting ``max_task_concurrency`` to a pod count.
       Must be >= 1.

Pod Spec Fields
~~~~~~~~~~~~~~~

Common Kubernetes pod and container settings are exposed as individual config fields.
These are applied **after** the ``pod_template`` YAML (if any), so they override
template values when both are set.

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``node_selector``
     - ``dict``
     - ``{}``
     - Node selector labels for pod scheduling
       (e.g. ``{nodepool = "compute"}``).
   * - ``service_account_name``
     - ``str``
     - ``""``
     - Kubernetes service account name for worker Pods.
   * - ``image_pull_policy``
     - ``str``
     - ``""``
     - Image pull policy for the worker container
       (``"Always"``, ``"Never"``, or ``"IfNotPresent"``).
   * - ``resource_requests``
     - ``dict``
     - ``{}``
     - Kubernetes resource requests for the worker container
       (e.g. ``{cpu = "2", memory = "8Gi"}``).
   * - ``resource_limits``
     - ``dict``
     - ``{}``
     - Kubernetes resource limits for the worker container
       (e.g. ``{cpu = "4", memory = "16Gi"}``).

Pod Template
~~~~~~~~~~~~

For settings not covered by the fields above --- tolerations, image pull secrets,
init containers, volumes, security contexts, extra labels/annotations, or any other
Kubernetes pod spec option --- use ``pod_template`` to provide a partial pod spec in
YAML format.

**Layering order:**

1. Scaler generates a base pod dict (image, env vars, ``restartPolicy: Never``).
2. ``pod_template`` is parsed and deep-merged into the base.
3. Config fields (``node_selector``, ``resource_requests``, etc.) are applied on
   top, overriding any values set in the template.
4. ``restartPolicy`` is always re-asserted to ``Never`` last; you cannot override it.

Nested dicts merge recursively. Lists *replace* the base list entirely --- no
element-level merge.

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``pod_template``
     - ``str``
     - ``""``
     - Multi-line TOML string containing a partial Kubernetes pod template in YAML
       format. Parsed with ``yaml.safe_load`` and deep-merged into the generated pod
       dict. Use standard camelCase Kubernetes field names.

.. code-block:: toml

   [[worker_manager]]
   type = "k8s_raw"
   namespace = "scaler"
   pod_image = "myregistry.example.com/scaler:latest"
   workers_per_pod = 4

   # Common options:
   service_account_name = "scaler-worker-manager"

   [worker_manager.node_selector]
   nodepool = "compute"

   [worker_manager.resource_requests]
   cpu = "4"
   memory = "16Gi"

   [worker_manager.resource_limits]
   memory = "32Gi"

   # pod_template for everything else:
   pod_template = """
   metadata:
     labels:
       env: prod
     annotations:
       prometheus.io/scrape: "true"
   spec:
     imagePullSecrets:
     - name: registry-credentials
     tolerations:
     - key: dedicated
       operator: Equal
       value: scaler
       effect: NoSchedule
     securityContext:
       runAsNonRoot: true
       runAsUser: 1000
     containers:
     - name: scaler-worker
       securityContext:
         allowPrivilegeEscalation: false
         capabilities:
           drop: ["ALL"]
   """

.. warning::
   If you specify ``env`` in the YAML template's container spec, it **replaces**
   Scaler's injected env vars (``COMMAND``, ``PYTHON_REQUIREMENTS``,
   ``PYTHON_VERSION``) entirely. You must re-include them if you need them.

Lifecycle
~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``delete_grace_period_seconds``
     - ``int``
     - ``30``
     - Grace period in seconds given to a Pod during deletion
       (``gracePeriodSeconds`` on the Kubernetes delete call).
       Allows in-flight tasks to finish cleanly before the container exits.
       Must be >= 0.

Python Environment
~~~~~~~~~~~~~~~~~~

These values are passed as environment variables into each container so the entrypoint
can install the right packages at startup. Shares the same
``PythonWorkerEnvironmentConfig`` fields as other container-based worker managers; they
appear as flat keys in the ``[[worker_manager]]`` TOML section (not a sub-section).

.. list-table::
   :header-rows: 1
   :widths: 30 15 15 40

   * - Field
     - Type
     - Default
     - Description
   * - ``requirements_txt``
     - ``str``
     - ``None``
     - Python packages to install inside the container at startup. Passed as the
       ``PYTHON_REQUIREMENTS`` environment variable. Can be a path to a
       requirements.txt file or an inline string. Include any packages your task
       functions import.
   * - ``python_version``
     - ``str``
     - ``None``
     - Python version string passed as the ``PYTHON_VERSION`` environment variable
       to the container entrypoint.

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behaviour, logging, and event-loop options shared by all
worker managers, see :doc:`common_parameters`.

RBAC Example
------------

Apply the following manifest to grant the ``scaler-worker-manager`` service account the
minimum permissions required in the ``scaler`` namespace.  Adjust ``namespace`` and
``name`` to match your deployment:

.. code-block:: yaml
   :caption: scaler-rbac.yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: scaler-worker-manager
     namespace: scaler
   rules:
     - apiGroups: [""]
       resources: ["pods"]
       verbs: ["get", "create", "delete", "list", "watch"]
   ---
   apiVersion: rbac.authorization.k8s.io/v1
   kind: RoleBinding
   metadata:
     name: scaler-worker-manager
     namespace: scaler
   subjects:
     - kind: ServiceAccount
       name: scaler-worker-manager
       namespace: scaler
   roleRef:
     kind: Role
     name: scaler-worker-manager
     apiGroup: rbac.authorization.k8s.io

Apply it with:

.. code-block:: bash

   kubectl apply -f scaler-rbac.yaml

Then reference it in your config:

.. code-block:: toml

   service_account_name = "scaler-worker-manager"

.. note::
   The manifest above grants permissions scoped to a single namespace using a
   ``Role``/``RoleBinding`` pair.  Do **not** use a ``ClusterRole``/``ClusterRoleBinding``
   unless your deployment genuinely needs cross-namespace pod management.

How It Works
------------

On every scheduler heartbeat, the scheduler sends a ``setDesiredTaskConcurrency``
command.  The worker manager converts this to a pod count
(``ceil(desired_tasks / workers_per_pod)``) and reconciles by creating or deleting
bare Pods via the Kubernetes API.

Each Pod runs ``scaler_worker_manager baremetal_native --mode fixed``, which spawns
``workers_per_pod`` worker processes that connect back to the scheduler and process
tasks like local workers.  When workers are no longer needed, the worker manager
deletes the corresponding Pods, honouring ``delete_grace_period_seconds``.

Every Pod is created with ``restartPolicy: Never``.  Kubernetes will never auto-restart
a crashed pod; instead, the scheduler's worker heartbeat timeout marks the worker dead
and re-queues its tasks.  The next scaling event launches replacement pods.

Troubleshooting
---------------

**Workers can't connect to the scheduler:**
Worker Pods must be able to reach the scheduler address over the network. If the
scheduler runs outside the cluster, ensure the address in ``scheduler_address`` is
reachable from Pod IP space (e.g. a LoadBalancer or NodePort service, or a VPN).
Inside a cluster, use the Kubernetes service name:
``tcp://scheduler-service:8516``.

**Pods stuck in** ``Pending``:
Check Pod events with ``kubectl describe pod <pod-name> -n scaler``. Common causes:
no nodes match the ``node_selector``, tolerations are missing for tainted nodes, or
the cluster has insufficient CPU/memory to satisfy ``resource_requests``.

**Image pull errors or locally-loaded KinD images:**
For images pushed to a private registry, verify the URI in ``pod_image`` is correct
and that any ``imagePullSecrets`` set via ``pod_template`` match valid
``kubernetes.io/dockerconfigjson`` secrets in the same namespace
(``kubectl get secret <name> -n scaler``).

For locally-loaded KinD images that bypass a registry entirely, set
``image_pull_policy = "Never"``.

**Permission denied when creating pods:**
Apply the RBAC manifest in `RBAC Example`_ and confirm ``service_account_name``
matches the deployed service account.

**``scaler_worker_manager`` not found in container:**
Ensure ``python_worker_environment.requirements_txt`` includes
``opengris-scaler[kubernetes]`` (or at minimum ``opengris-scaler``), and that the
container entrypoint installs it before launching workers.

**Pods are not deleted after scale-down:**
Check that the worker manager process has not crashed.  The manager holds the list of
pod names it owns in memory; if it restarts it will no longer track previously created
pods.  You can clean up orphaned pods manually:

.. code-block:: bash

   kubectl delete pods -n scaler -l app=scaler-worker
