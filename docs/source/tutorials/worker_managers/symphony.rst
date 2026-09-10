IBM Spectrum Symphony Worker Manager
=====================================

The Symphony worker manager integrates Scaler with `IBM Spectrum Symphony <https://www.ibm.com/products/analytics-workload-management>`_, allowing Scaler to offload task execution to a Symphony cluster via the SOAM (Service-Oriented Architecture Middleware) API.

Quick Start
-----------

Prerequisites
~~~~~~~~~~~~~

* An IBM Spectrum Symphony cluster with a registered application and deployed service, which
  ``scripts/symphony/setup_application.py`` creates (see Step 2)
* A Symphony installation on the machine running the worker manager, which is where the ``soamapi`` Python API comes from
* Python 3.10 or 3.12, the versions both Scaler and Symphony 7.3.2 support (see the note in Step 1)
* Python packages: ``pip install opengris-scaler``
* Network connectivity between the machine running the worker manager and both the Scaler scheduler and the Symphony cluster

Step 1: Install Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Scaler comes from PyPI:

.. code-block:: bash

   pip install opengris-scaler

``soamapi`` does not. It is not published on PyPI: it ships with the Symphony product as bytecode, under
``$SOAM_HOME/$SOAM_VERSION/$BINARY_TYPE/lib64/pythonapi_<python-version>``. Put it on the path by sourcing the
Symphony environment in the shell that will run the worker manager:

.. code-block:: bash

   . $SOAM_HOME/conf/profile.soam

That sets all of the variables you need.

.. note::

   **Supported Python versions.** Symphony compiles ``soamapi`` for specific interpreters, one
   ``pythonapi_<python-version>`` directory per version. Only 3.10 and 3.12 are compatible with both
   scaler and symphony.

   You can check which versions of Python your symphony import supports with the following command:

   .. code-block:: bash

      ls -d $SOAM_HOME/$SOAM_VERSION/$BINARY_TYPE/lib64/pythonapi_*

Step 2: Create the Symphony Application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The worker manager submits tasks to a Symphony application whose service runs cloudpickled callables. Create
both with the setup utility, on a host with the Symphony installation, passing the interpreter that will run
the service:

.. code-block:: bash

   python3 scripts/symphony/setup_application.py --python /path/to/python

It packages and deploys the service, generates an application profile with the paths resolved, registers it,
and prints the registered applications. Add ``--dry-run`` to see the profile without changing the cluster, and
``--application``/``--service`` to use names other than ``Scaler``/``ScalerService``.

The interpreter you name needs ``cloudpickle``, ``tblib`` and a matching ``soamapi``; the utility checks all
three by running it, and refuses rather than leaving the failure to appear later as unexplained task
failures. ``pip install opengris-scaler`` provides the first two.

Step 3: Start Scaler
~~~~~~~~~~~~~~~~~~~~

The ``scaler`` launcher starts the object storage server, the scheduler and the worker manager from one
configuration file. Run it on the host with the Symphony installation, in the shell where you sourced the
Symphony environment in Step 1:

.. code-block:: bash

   scaler config.toml

.. code-block:: toml
   :caption: config.toml

   [object_storage_server]
   bind_address = "tcp://127.0.0.1:8517"

   [scheduler]
   bind_address = "tcp://0.0.0.0:8516"
   object_storage_address = "tcp://127.0.0.1:8517"
   policy_engine_type = "simple"
   policy_content = "allocate=even_load; scaling=vanilla"

   [[worker_manager]]
   type = "symphony"
   scheduler_address = "tcp://127.0.0.1:8516"
   worker_manager_id = "wm-symphony"
   service_name = "Scaler"
   max_task_concurrency = 8
   logging_level = "INFO"

The scheduler binds ``0.0.0.0`` so clients on other machines can reach it, while the worker manager connects
over the loopback address alongside it.

``service_name`` takes the Symphony **application** name, not the service name. The worker manager passes it
to ``soamapi.connect()``, which connects to an application. With the application created by
``scripts/symphony/setup_application.py`` this is ``Scaler``, whose service is ``ScalerService``.

To run the parts separately instead, each one takes the same settings on the command line:

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:8517
   scaler_scheduler tcp://0.0.0.0:8516 --object-storage-address tcp://127.0.0.1:8517 \
       --policy-content "allocate=even_load; scaling=vanilla"
   scaler_worker_manager symphony tcp://<SCHEDULER_IP>:8516 \
       --worker-manager-id wm-symphony \
       --service-name Scaler \
       --max-task-concurrency 8

Step 4: Submit Tasks
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_IP>:8516") as client:
       results = client.map(compute, range(50))
       print(results)

``Client.map`` returns the results themselves. ``Client.submit`` is the one that returns a future:

.. code-block:: python

   future = client.submit(compute, 7)
   print(future.result())

How It Works
------------

1. The Symphony worker manager connects to the Scaler scheduler as a worker.
2. It establishes a SOAM connection and session to the configured Symphony service.
3. When the worker manager receives a task from the scheduler, it serializes the function and arguments with ``cloudpickle`` and submits them as a Symphony task via the SOAM API.
4. Symphony schedules the task on its compute hosts. The service calls the function and sends back either its return value or the exception it raised, so a raising function is a completed Symphony task rather than a failed one, and its type, message and traceback reach the client intact.
5. The worker manager deserializes that outcome and returns it to the Scaler scheduler.

The worker manager uses a concurrency semaphore to limit the number of tasks in flight.

Configuration Reference
------------------------

Symphony-Specific Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--service-name`` (``-sn``, required): The name of the Symphony **application** to connect to. Despite the
  option name, this value reaches ``soamapi.connect()``, which takes an application rather than a service.
* ``--max-task-concurrency`` (``-mtc``): Maximum number of concurrent Symphony workers (default: number of CPUs − 1).

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behavior, logging, and event loop options, see :doc:`common_parameters`.
