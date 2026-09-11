.. _installation_options:

Installation
============

The ``opengris-scaler`` package is available on PyPI and can be installed using any compatible package manager. The examples below use `uv <https://docs.astral.sh/uv/getting-started/installation>`_.

Install uv and Create a Virtual Environment
-------------------------------------------

.. note::

    Skip this section if you already have a uv virtual environment prepared.

Install `uv <https://docs.astral.sh/uv/getting-started/installation>`__:

.. tabs::

    .. group-tab:: Linux / macOS

        .. code-block:: bash

            curl -LsSf https://astral.sh/uv/install.sh | sh

    .. group-tab:: Windows

        .. code-block:: powershell

            powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

Restart your shell, then create and activate a virtual environment named ``opengris-scaler-venv``, pinned to Python 3.13, the newest version supported by all optional dependencies (Scaler itself supports Python 3.10 or later). uv downloads this Python version automatically if it is not already installed:

.. tabs::

    .. group-tab:: Linux / macOS

        .. code-block:: bash

            uv venv opengris-scaler-venv --python 3.13
            source opengris-scaler-venv/bin/activate

    .. group-tab:: Windows

        .. code-block:: powershell

            uv venv opengris-scaler-venv --python 3.13
            opengris-scaler-venv\Scripts\activate

Install Scaler
--------------

.. note::

    The commands below assume you are inside an activated virtual environment.

Base installation:

.. code-block:: bash

    uv pip install opengris-scaler

If you use GraphBLAS to solve DAG graph tasks:

.. code-block:: bash

    uv pip install 'opengris-scaler[graphblas]'

If you need all optional dependencies:

.. code-block:: bash

    uv pip install 'opengris-scaler[all]'
