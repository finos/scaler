Commands
========

After installing ``opengris-scaler``, the following CLI commands are available:

.. list-table:: Installed commands
   :header-rows: 1

   * - Command
     - Description
   * - :ref:`scaler <cmd-scaler>`
     - Start a full stack from one TOML file (object storage server + scheduler + one or more worker managers).
   * - :ref:`scaler_scheduler <cmd-scaler-scheduler>`
     - Start only the scheduler process (connects to an existing object storage server).
   * - :ref:`scaler_worker_manager <cmd-scaler-worker-manager>`
     - Start one worker manager using a subcommand (``baremetal_native``, ``symphony``, ``aws_raw_ecs``, ``aws_hpc``, ``orb_aws_ec2``).
   * - :ref:`scaler_object_storage_server <cmd-scaler-object-storage-server>`
     - Start only the object storage server.
   * - :ref:`scaler_top <cmd-scaler-top>`
     - Start the terminal monitoring dashboard for a scheduler monitor endpoint.
   * - :ref:`scaler_gui <cmd-scaler-gui>`
     - Start the web monitoring GUI for a scheduler monitor endpoint.


Command Arguments/Config Conventions
------------------------------------

All commands support either command line or ``--config <toml config>``/``-c <toml config>``. In practice, most deployments use TOML files.

- Precedence: ``Command-line flags > TOML settings > built-in defaults``.
- Key naming: long CLI flags converted to snake case (for example, ``--max-task-concurrency`` -> ``max_task_concurrency``).
- Unified launcher: ``scaler`` reads ``[scheduler]``, ``[[worker_manager]]``, ``[object_storage_server]``, ``[top]``, and ``[gui]`` sections.

.. list-table:: Command to TOML mapping
   :header-rows: 1

   * - Command
     - TOML section
   * - ``scaler_scheduler``
     - ``[scheduler]``
   * - ``scaler_object_storage_server``
     - ``[object_storage_server]``
   * - ``scaler_top``
     - ``[top]``
   * - ``scaler_gui``
     - ``[gui]``
   * - ``scaler_worker_manager baremetal_native``
     - ``[[worker_manager]]`` + ``type = "baremetal_native"``
   * - ``scaler_worker_manager symphony``
     - ``[[worker_manager]]`` + ``type = "symphony"``
   * - ``scaler_worker_manager aws_raw_ecs``
     - ``[[worker_manager]]`` + ``type = "aws_raw_ecs"``
   * - ``scaler_worker_manager aws_hpc``
     - ``[[worker_manager]]`` + ``type = "aws_hpc"``
   * - ``scaler_worker_manager orb_aws_ec2``
     - ``[[worker_manager]]`` + ``type = "orb_aws_ec2"``


.. _cmd-scaler:

scaler
------

``scaler`` is the all-in-one launcher. It reads one TOML file and starts any recognized
sections as separate processes.

- ``[scheduler]`` starts the scheduler.
- ``[object_storage_server]`` starts the object storage server.
- ``[[worker_manager]]`` starts one worker manager per table entry.
- ``object_storage_address`` is required in ``[scheduler]`` and points to the object storage server.
- ``advertised_object_storage_address`` is optional, see :ref:`object-storage-addresses`.

.. code-block:: bash

    scaler <toml config file>

Scaler examples
~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [object_storage_server]
            bind_address = "tcp://127.0.0.1:6379"

            [scheduler]
            bind_address = "tcp://127.0.0.1:6378"
            object_storage_address = "tcp://127.0.0.1:6379"
            # optional public endpoint advertised to clients/workers
            # advertised_object_storage_address = "tcp://203.0.113.10:6379"
            monitor_address = "tcp://127.0.0.1:6380"
            policy_engine_type = "simple"
            policy_content = "allocate=even_load; scaling=vanilla"
            logging_level = "INFO"

            [gui]
            monitor_address = "tcp://127.0.0.1:6380"
            gui_address = "127.0.0.1:50001"
            logging_level = "INFO"

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:6378"
            worker_manager_id = "wm-native"
            mode = "dynamic"
            max_task_concurrency = 8
            event_loop = "builtin"
            io_threads = 2

            [[worker_manager]]
            type = "aws_raw_ecs"
            scheduler_address = "tcp://127.0.0.1:6378"
            object_storage_address = "tcp://127.0.0.1:6379"
            worker_manager_id = "wm-ecs"
            ecs_subnets = "subnet-0abc123,subnet-0def456"
            ecs_cluster = "scaler-cluster"
            ecs_task_definition = "scaler-task-definition"
            ecs_task_image = "public.ecr.aws/v4u8j8r6/scaler:latest"
            aws_region = "us-east-1"

            [[worker_manager]]
            type = "aws_hpc"
            scheduler_address = "tcp://127.0.0.1:6378"
            object_storage_address = "tcp://127.0.0.1:6379"
            worker_manager_id = "wm-batch"
            job_queue = "scaler-job-queue"
            job_definition = "scaler-job-definition"
            s3_bucket = "my-scaler-bucket"
            aws_region = "us-east-1"

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler config.toml

Scaler arguments
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Description
   * - ``file``
     - Yes
     - Path to a TOML file that contains ``[scheduler]`` and/or ``[[worker_manager]]`` sections.

If no recognized sections are present, ``scaler`` exits with an error.


.. _cmd-scaler-scheduler:

scaler_scheduler
----------------

``scaler_scheduler`` starts only the scheduler process and requires two addresses:
``<bind_address>`` to bind and ``--object-storage-address`` to connect.
Use ``--advertised-object-storage-address`` when clients/workers should connect through a
different public endpoint.

.. code-block:: bash

    scaler_scheduler [options] <bind_address>

Scheduler examples
~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [scheduler]
            bind_address = "tcp://127.0.0.1:6378"
            object_storage_address = "tcp://127.0.0.1:6379"
            # optional public endpoint advertised to clients/workers
            # advertised_object_storage_address = "tcp://203.0.113.10:6379"
            monitor_address = "tcp://127.0.0.1:6380"
            policy_engine_type = "simple"
            policy_content = "allocate=even_load; scaling=no"
            logging_level = "INFO"

        Run command:

        .. code-block:: bash

            scaler_scheduler --config scheduler.toml tcp://127.0.0.1:6378

    .. group-tab:: command line

        .. code-block:: bash

            scaler_scheduler tcp://127.0.0.1:6378 \
                --object-storage-address tcp://127.0.0.1:6379 \
                --advertised-object-storage-address tcp://203.0.113.10:6379 \
                --monitor-address tcp://127.0.0.1:6380 \
                --policy-engine-type simple \
                --policy-content "allocate=even_load; scaling=no" \
                --logging-level INFO

Scheduler arguments
~~~~~~~~~~~~~~~~~~~

.. list-table:: Core options
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``bind_address``
     - Yes
     - -
     - Scheduler bind address (for example ``tcp://127.0.0.1:6378``).
   * - ``-osa``, ``--object-storage-address``
     - Yes
     - -
     - Object storage address (must be ``tcp://<ip>:<port>``).
   * - ``-aosa``, ``--advertised-object-storage-address``
     - No
     - Same as ``object_storage_address``
     - Public object storage address advertised to clients/workers.
   * - ``-ma``, ``--monitor-address``
     - No
     - Auto from bind address
     - Monitor endpoint, defaults to ``bind_address.port + 2``.
   * - ``-p``, ``--protected``
     - No
     - ``False``
     - Protect scheduler/workers from client shutdown requests.
   * - ``-mt``, ``--max-number-of-tasks-waiting``
     - No
     - ``-1``
     - Max queued tasks while workers are saturated (``-1`` means unlimited).
   * - ``-ct``, ``--client-timeout-seconds``
     - No
     - ``60``
     - Client heartbeat timeout.
   * - ``-wt``, ``--worker-timeout-seconds``
     - No
     - ``60``
     - Worker heartbeat timeout.
   * - ``-ot``, ``--object-retention-seconds``
     - No
     - ``60``
     - Object retention timeout.
   * - ``-ls``, ``--load-balance-seconds``
     - No
     - ``1``
     - Load-balance interval in seconds.
   * - ``-lbt``, ``--load-balance-trigger-times``
     - No
     - ``2``
     - Consecutive identical balance advisories required before rebalance is triggered.
   * - ``-el``, ``--event-loop``
     - No
     - ``builtin``
     - Event loop backend (``builtin`` or ``uvloop``).
   * - ``-it``, ``--io-threads``
     - No
     - ``1``
     - I/O backend thread count.
   * - ``-sri``, ``--status-report-interval-seconds``
     - No
     - ``1``
     - Interval between status reports the scheduler publishes to monitors (``scaler_top``/``scaler_gui``).
   * - ``-orl``, ``--object-report-limit``
     - No
     - ``500``
     - Biggest objects each status report carries, which is how many the web GUI can page through. Each
       one costs the report about 240 bytes.

.. list-table:: Policy options
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``-et``, ``--policy-engine-type``
     - No
     - ``simple``
     - Policy engine type selector.
   * - ``-pc``, ``--policy-content``
     - No
     - ``allocate=even_load; scaling=vanilla``
     - Policy definition string.

.. list-table:: Logging options
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``-ll``, ``--logging-level``
     - No
     - ``INFO``
     - Logging verbosity.
   * - ``-lp``, ``--logging-paths``
     - No
     - ``/dev/stdout``
     - One or more logging outputs.
   * - ``-lcf``, ``--logging-config-file``
     - No
     - ``None``
     - Python logging ``.conf`` file that overrides logging flags.

.. list-table:: Config file option
   :header-rows: 1

   * - Argument
     - Required
     - Description
   * - ``-c``, ``--config``
     - No
     - TOML config file path (uses ``[scheduler]`` section).

Scheduler behavior notes
~~~~~~~~~~~~~~~~~~~~~~~~

.. _protected:

Protected mode
^^^^^^^^^^^^^^

When ``--protected`` is enabled, client shutdown requests cannot stop the scheduler and workers.

.. code-block:: bash

    scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517 --protected

.. _object-storage-addresses:

Object storage addresses
^^^^^^^^^^^^^^^^^^^^^^^^

Each participant connects to the object storage server on the address configured for it. These differ
when a load balancer fronts the server.

- ``[scheduler] object_storage_address``: the address the scheduler connects on.
- ``[scheduler] advertised_object_storage_address``: the address clients outside the cluster connect on.
- ``[[worker_manager]] object_storage_address``: the address its workers connect on, otherwise the advertised address.
- ``Client(object_storage_address=...)``: the address one client connects on, otherwise the advertised address.
- A client opened inside a worker connects on its worker's address, unless it is given either address.

.. code-block:: toml

    [scheduler]
    object_storage_address = "tcp://scaler-object-storage:6379"
    advertised_object_storage_address = "tcp://scaler.example.com:6379"

    [[worker_manager]]
    type = "baremetal_native"
    object_storage_address = "tcp://scaler-object-storage:6379"

Event loop selection
^^^^^^^^^^^^^^^^^^^^

The scheduler uses ``builtin`` event loop by default. You can switch to ``uvloop``.

.. code-block:: bash

    pip install uvloop
    scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517 -el uvloop


.. _cmd-scaler-worker-manager:

scaler_worker_manager
---------------------

``scaler_worker_manager`` is the unified worker-manager entry point. You select a worker manager
with a subcommand and then pass shared and worker-manager-specific options.

.. code-block:: bash

    scaler_worker_manager <subcommand> [options] <scheduler_address>

Available subcommands:

- ``baremetal_native``
- ``symphony``
- ``aws_raw_ecs``
- ``aws_hpc``
- ``orb_aws_ec2``

When ``--config``/``-c`` is supplied, ``scaler_worker_manager`` reads the ``[[worker_manager]]``
array from the TOML file and picks the entry whose ``type`` field matches the subcommand.
It is an error if no entry matches or if more than one entry matches.
Command-line flags always override TOML values.

Arguments (shared by all subcommands)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Shared options
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``scheduler_address``
     - Yes
     - -
     - Scheduler address that the worker manager process connects to.
   * - ``-wsa``, ``--worker-scheduler-address``
     - No
     - Same as ``scheduler_address``
     - Scheduler address advertised to spawned workers; use this when workers need a different/public endpoint.
   * - ``-wmi``, ``--worker-manager-id``
     - Yes
     - -
     - Logical ID for this worker manager.
   * - ``-osa``, ``--object-storage-address``
     - No
     - ``None``
     - Object storage server address (required for some remote worker setups).
   * - ``-mtc``, ``--max-task-concurrency``
     - No
     - ``os.cpu_count() - 1``
     - Max workers/jobs (``-1`` means no limit where supported).
   * - ``-pwc``, ``--per-worker-capabilities``
     - No
     - Empty
     - Capabilities string (for example ``linux,cpu=4,gpu=1``). Each entry is ``name`` or ``name=integer``;
       empty capability names (for example ``=1``) are not allowed.
   * - ``-wtqs``, ``--per-worker-task-queue-size``
     - No
     - ``1000``
     - Per-worker queue size.
   * - ``-his``, ``--heartbeat-interval-seconds``
     - No
     - ``2``
     - Worker heartbeat interval.
   * - ``-tts``, ``--task-timeout-seconds``
     - No
     - ``0``
     - Task timeout (``0`` means no timeout).
   * - ``-dts``, ``--death-timeout-seconds``
     - No
     - ``300``
     - Worker death timeout.
   * - ``-gc``, ``--garbage-collect-interval-seconds``
     - No
     - ``30``
     - Worker GC interval.
   * - ``-tm``, ``--trim-memory-threshold-bytes``
     - No
     - ``1073741824``
     - RSS threshold before memory trim.
   * - ``-hps``, ``--hard-processor-suspend``
     - No
     - ``False``
     - Use OS-level processor suspension (SIGTSTP) for paused tasks.
   * - ``-it``, ``--io-threads``
     - No
     - ``1``
     - I/O thread count per worker.
   * - ``-el``, ``--event-loop``
     - No
     - ``builtin``
     - Event loop backend (``builtin`` or ``uvloop``).
   * - ``--preload``
     - No
     - ``None``
     - Worker preload function spec (for example ``pkg.mod:init()``).
   * - ``-ll``, ``--logging-level``
     - No
     - ``INFO``
     - Logging verbosity.
   * - ``-lp``, ``--logging-paths``
     - No
     - ``/dev/stdout``
     - One or more log outputs.
   * - ``-lcf``, ``--logging-config-file``
     - No
     - ``None``
     - Python logging ``.conf`` file.
   * - ``-c``, ``--config``
     - No
     - ``None``
     - TOML config file path. Config is read from the ``[[worker_manager]]`` entry matching the subcommand.

Worker runtime notes
~~~~~~~~~~~~~~~~~~~~

Preload hook
^^^^^^^^^^^^

Workers can run initialization logic before processing tasks via ``--preload``.

.. code-block:: bash

    scaler_worker_manager baremetal_native tcp://127.0.0.1:6378 --worker-manager-id wm-preload --preload "mypackage.init:setup"
    scaler_worker_manager baremetal_native tcp://127.0.0.1:6378 --worker-manager-id wm-preload --preload "mypackage.init:configure('production', debug=False)"

Death timeout
^^^^^^^^^^^^^

``--death-timeout-seconds`` controls how long a worker can stay disconnected from scheduler before exiting.

.. code-block:: bash

    scaler_worker_manager baremetal_native tcp://127.0.0.1:6378 --worker-manager-id wm-fixed --mode fixed --max-task-concurrency 10 -dts 300

Subcommand: ``baremetal_native``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Local-process worker manager (dynamic auto-scaling or fixed pre-spawned workers).

.. code-block:: bash

    scaler_worker_manager baremetal_native [options] <scheduler_address>

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:6378"
            worker_manager_id = "wm-native"
            mode = "dynamic"
            max_task_concurrency = 8

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_worker_manager baremetal_native tcp://127.0.0.1:6378 \
                --worker-manager-id wm-native \
                --mode dynamic \
                --max-task-concurrency 8

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``--mode``
     - No
     - ``DYNAMIC``
     - ``DYNAMIC`` or ``FIXED`` native worker manager mode.
   * - ``--worker-type``
     - No
     - Auto
     - Worker ID prefix override.
   * - ``-n``, ``--num-of-workers``
     - No
     - -
     - Backward-compatible alias for ``--max-task-concurrency``.

Subcommand: ``symphony``
~~~~~~~~~~~~~~~~~~~~~~~~

IBM Spectrum Symphony worker manager.

.. code-block:: bash

    scaler_worker_manager symphony [options] <scheduler_address>

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [[worker_manager]]
            type = "symphony"
            scheduler_address = "tcp://127.0.0.1:6378"
            worker_manager_id = "wm-symphony"
            service_name = "ScalerService"

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_worker_manager symphony tcp://127.0.0.1:6378 \
                --worker-manager-id wm-symphony \
                --service-name ScalerService

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``-sn``, ``--service-name``
     - Yes
     - -
     - Symphony service name to use for submitted workers.

Subcommand: ``aws_raw_ecs``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

AWS ECS (Fargate) worker manager.

.. code-block:: bash

    scaler_worker_manager aws_raw_ecs [options] <scheduler_address>

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [[worker_manager]]
            type = "aws_raw_ecs"
            scheduler_address = "tcp://127.0.0.1:6378"
            object_storage_address = "tcp://127.0.0.1:6379"
            worker_manager_id = "wm-ecs"
            ecs_subnets = "subnet-0abc123,subnet-0def456"
            ecs_cluster = "scaler-cluster"
            ecs_task_definition = "scaler-task-definition"
            aws_region = "us-east-1"

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_worker_manager aws_raw_ecs tcp://127.0.0.1:6378 \
                --object-storage-address tcp://127.0.0.1:6379 \
                --worker-manager-id wm-ecs \
                --ecs-subnets subnet-0abc123,subnet-0def456 \
                --ecs-cluster scaler-cluster \
                --ecs-task-definition scaler-task-definition \
                --aws-region us-east-1

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``--aws-access-key-id``
     - No
     - ``None``
     - AWS access key ID (or use ``AWS_ACCESS_KEY_ID`` env var).
   * - ``--aws-secret-access-key``
     - No
     - ``None``
     - AWS secret key (or use ``AWS_SECRET_ACCESS_KEY`` env var).
   * - ``--aws-region``
     - No
     - ``us-east-1``
     - AWS region.
   * - ``--ecs-subnets``
     - Yes
     - -
     - Comma-separated subnet IDs for ECS task networking.
   * - ``--ecs-cluster``
     - No
     - ``scaler-cluster``
     - ECS cluster name.
   * - ``--ecs-task-image``
     - No
     - ``public.ecr.aws/v4u8j8r6/scaler:latest``
     - Container image used for workers.
   * - ``--ecs-python-requirements``
     - No
     - ``tomli;pargraph;parfun;pandas``
     - Python dependency string passed to task runtime.
   * - ``--ecs-python-version``
     - No
     - ``3.12.11``
     - Python runtime version for ECS task.
   * - ``--ecs-task-definition``
     - No
     - ``scaler-task-definition``
     - ECS task definition name.
   * - ``--ecs-task-cpu``
     - No
     - ``4``
     - ECS vCPU count.
   * - ``--ecs-task-memory``
     - No
     - ``30``
     - ECS task memory in GB.

Subcommand: ``aws_hpc``
~~~~~~~~~~~~~~~~~~~~~~~

AWS Batch worker manager.

.. code-block:: bash

    scaler_worker_manager aws_hpc [options] <scheduler_address>

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [[worker_manager]]
            type = "aws_hpc"
            scheduler_address = "tcp://127.0.0.1:6378"
            object_storage_address = "tcp://127.0.0.1:6379"
            worker_manager_id = "wm-batch"
            job_queue = "scaler-job-queue"
            job_definition = "scaler-job-definition"
            s3_bucket = "my-scaler-bucket"
            aws_region = "us-east-1"

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_worker_manager aws_hpc tcp://127.0.0.1:6378 \
                --object-storage-address tcp://127.0.0.1:6379 \
                --worker-manager-id wm-batch \
                --job-queue scaler-job-queue \
                --job-definition scaler-job-definition \
                --s3-bucket my-scaler-bucket \
                --aws-region us-east-1

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``-q``, ``--job-queue``
     - Yes
     - -
     - AWS Batch queue name.
   * - ``-d``, ``--job-definition``
     - Yes
     - -
     - AWS Batch job definition name.
   * - ``--s3-bucket``
     - Yes
     - -
     - S3 bucket used for task data.
   * - ``-b``, ``--backend``
     - No
     - ``batch``
     - AWS HPC backend type.
   * - ``-n``, ``--name``
     - No
     - ``None``
     - Worker name override.
   * - ``--aws-region``
     - No
     - ``us-east-1``
     - AWS region.
   * - ``--s3-prefix``
     - No
     - ``scaler-tasks``
     - S3 key prefix for task data.
   * - ``-mcj``, ``--max-concurrent-jobs``
     - No
     - ``100``
     - Maximum concurrently running AWS Batch jobs.
   * - ``--job-timeout-minutes``
     - No
     - ``60``
     - Timeout for each submitted job.

Subcommand: ``orb_aws_ec2``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

ORB (Open Resource Broker) worker manager — dynamically provisions workers on AWS EC2 instances.

.. code-block:: bash

    scaler_worker_manager orb_aws_ec2 [options] <scheduler_address>

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [[worker_manager]]
            type = "orb_aws_ec2"
            scheduler_address = "tcp://127.0.0.1:6378"
            worker_manager_id = "wm-orb"
            object_storage_address = "tcp://127.0.0.1:6379"
            image_id = "ami-0528819f94f4f5fa5"
            instance_type = "t3.medium"
            aws_region = "us-east-1"

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_worker_manager orb_aws_ec2 tcp://127.0.0.1:6378 \
                --worker-manager-id wm-orb \
                --object-storage-address tcp://127.0.0.1:6379 \
                --image-id ami-0528819f94f4f5fa5 \
                --instance-type t3.medium \
                --aws-region us-east-1

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``--image-id``
     - Yes
     - -
     - AMI ID for the worker EC2 instances.
   * - ``--instance-type``
     - No
     - ``t2.micro``
     - EC2 instance type.
   * - ``--aws-region``
     - Yes
     - -
     - AWS region where ORB launches worker instances.
   * - ``--key-name``
     - No
     - ``None``
     - AWS key pair name. A temporary key pair is created if omitted.
   * - ``--subnet-id``
     - No
     - ``None``
     - AWS subnet ID. Defaults to the default subnet in the default VPC.
   * - ``--security-group-ids``
     - No
     - ``[]``
     - Comma-separated AWS security group IDs. A temporary group is created if omitted.

For full details, see :doc:`worker_managers/orb_aws_ec2/index`.


.. _cmd-scaler-object-storage-server:

scaler_object_storage_server
----------------------------

``scaler_object_storage_server`` starts the standalone object storage server.

.. code-block:: bash

    scaler_object_storage_server [options] <bind_address>

Object storage examples
~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [object_storage_server]
            bind_address = "tcp://127.0.0.1:6379"

        Run command:

        .. code-block:: bash

            scaler_object_storage_server --config object_storage.toml tcp://127.0.0.1:6379

    .. group-tab:: command line

        .. code-block:: bash

            scaler_object_storage_server tcp://127.0.0.1:6379

Object storage arguments
~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Description
   * - ``bind_address``
     - Yes
     - Storage bind address in ``tcp://<ip>:<port>`` format.
   * - ``-c``, ``--config``
     - No
     - TOML config file path (uses ``[object_storage_server]`` section).


.. _cmd-scaler-top:

scaler_top
----------

``scaler_top`` starts a terminal dashboard that streams scheduler metrics from a monitor
endpoint.

.. code-block:: bash

    scaler_top [options] <monitor_address>

Top examples
~~~~~~~~~~~~

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [top]
            monitor_address = "tcp://127.0.0.1:6380"
            timeout = 5

        Run command:

        .. code-block:: bash

            scaler_top --config top.toml tcp://127.0.0.1:6380

    .. group-tab:: command line

        .. code-block:: bash

            scaler_top tcp://127.0.0.1:6380 --timeout 5

Top arguments
~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``monitor_address``
     - Yes
     - -
     - Scheduler monitor address to subscribe to.
   * - ``-t``, ``--timeout``
     - No
     - ``5``
     - Subscriber timeout in seconds.
   * - ``-c``, ``--config``
     - No
     - ``None``
     - TOML config file path (uses ``[top]`` section).


.. _cmd-scaler-gui:

scaler_gui
----------

``scaler_gui`` starts the web monitoring GUI and connects it to a scheduler monitor endpoint.

.. code-block:: bash

    scaler_gui [options] <monitor_address>

UI examples
~~~~~~~~~~~

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [gui]
            monitor_address = "tcp://127.0.0.1:6380"
            gui_address = "127.0.0.1:50001"
            logging_level = "INFO"

        Run command:

        .. code-block:: bash

            scaler_gui --config gui.toml tcp://127.0.0.1:6380

    .. group-tab:: command line

        .. code-block:: bash

            scaler_gui tcp://127.0.0.1:6380 --gui-address 127.0.0.1:50001

UI arguments
~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Argument
     - Required
     - Default
     - Description
   * - ``monitor_address``
     - Yes
     - -
     - Scheduler monitor address to subscribe to.
   * - ``--gui-address``
     - No
     - ``0.0.0.0:50001``
     - Host and port for the web server.
   * - ``-bi``, ``--broadcast-interval-seconds``
     - No
     - ``0.5``
     - Interval between updates the web GUI pushes to connected browsers.
   * - ``-tl``, ``--task-log-max-size``
     - No
     - ``50000``
     - Completed tasks and task-log events the GUI keeps. It pages through them server-side, so this
       bounds memory rather than what you can browse.
   * - ``-sri``, ``--status-report-interval-seconds``
     - No
     - ``1``
     - The scheduler's status report interval; set it to match so the scheduler-stale indicator is accurate.
   * - ``-ll``, ``--logging-level``
     - No
     - ``INFO``
     - Logging verbosity.
   * - ``-lp``, ``--logging-paths``
     - No
     - ``/dev/stdout``
     - One or more log outputs.
   * - ``-lcf``, ``--logging-config-file``
     - No
     - ``None``
     - Python logging ``.conf`` file.
   * - ``-c``, ``--config``
     - No
     - ``None``
     - TOML config file path (uses ``[gui]`` section).
