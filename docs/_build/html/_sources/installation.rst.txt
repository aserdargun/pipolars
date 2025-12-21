Installation
============

This guide covers the installation of PIPolars and its dependencies.

Requirements
------------

PIPolars has the following requirements:

**Operating System**
   - Windows 10 or later (required for PI AF SDK)
   - Windows Server 2016 or later

**Python**
   - Python 3.10 or later

**PI System Components**
   - OSIsoft PI AF SDK 2.x (requires .NET Framework 4.8)
   - Access to a PI Data Archive server

.. warning::

   PIPolars requires **Windows** because it uses the OSIsoft PI AF SDK,
   which is a Windows-only .NET library. The library uses ``pythonnet``
   for .NET interop.

Installing the PI AF SDK
------------------------

Before installing PIPolars, you must have the PI AF SDK installed:

1. **Download PI AF Client**

   Download the PI AF Client from the
   `AVEVA OSIsoft Customer Portal <https://customers.osisoft.com>`_.
   You need a valid license and support agreement.

2. **Install PI AF Client**

   Run the installer and select the "PI AF Client" component. This includes:

   - PI AF SDK assemblies
   - PI SDK (legacy, optional)
   - PI Data Archive client connectivity

3. **Verify Installation**

   The PI AF SDK assemblies should be installed in:

   .. code-block:: text

      C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0\

   Key assemblies include:

   - ``OSIsoft.AFSDK.dll``
   - ``OSIsoft.AF.dll``

Installing PIPolars
-------------------

Using uv (Recommended)
~~~~~~~~~~~~~~~~~~~~~~

`uv <https://github.com/astral-sh/uv>`_ is a fast Python package manager from
Astral. It's the recommended way to install PIPolars.

.. code-block:: bash

   # Install uv if you don't have it
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

   # Add PIPolars to your project
   uv add pipolars

   # Or install globally
   uv pip install pipolars

Using pip
~~~~~~~~~

You can also install PIPolars using pip:

.. code-block:: bash

   pip install pipolars

Development Installation
~~~~~~~~~~~~~~~~~~~~~~~~

To install PIPolars for development:

.. code-block:: bash

   # Clone the repository
   git clone https://github.com/pipolars/pipolars.git
   cd pipolars

   # Install with all development dependencies
   uv sync --all-extras

   # Or using pip
   pip install -e ".[dev,docs]"

Verifying the Installation
--------------------------

After installation, verify that PIPolars can find the PI AF SDK:

.. code-block:: python

   from pipolars import PIClient

   # This will raise an error if SDK is not found
   client = PIClient("your-pi-server")

   # Check available servers
   from pipolars.connection.server import PIServerConnection
   servers = PIServerConnection.list_servers()
   print(f"Available PI Servers: {servers}")

Troubleshooting
---------------

PI AF SDK Not Found
~~~~~~~~~~~~~~~~~~~

If you get an error about the PI AF SDK not being found:

1. Verify the SDK is installed in the correct location
2. Check that the ``OSIsoft.AFSDK.dll`` file exists
3. Ensure you're using a 64-bit Python installation

.. code-block:: python

   import sys
   print(f"Python: {sys.version}")
   print(f"Platform: {sys.platform}")
   print(f"64-bit: {sys.maxsize > 2**32}")

pythonnet Import Error
~~~~~~~~~~~~~~~~~~~~~~

If you get errors importing ``clr`` or ``pythonnet``:

.. code-block:: bash

   # Reinstall pythonnet
   pip uninstall pythonnet
   pip install pythonnet>=3.0.3

Connection Refused
~~~~~~~~~~~~~~~~~~

If you cannot connect to the PI Server:

1. Verify the server hostname is correct
2. Check network connectivity (port 5450 by default)
3. Ensure your Windows credentials have access to the PI Server
4. Check the PI Server trust configuration

Optional Dependencies
---------------------

PIPolars has optional dependency groups:

**Development Dependencies**

.. code-block:: bash

   uv sync --extra dev
   # or
   pip install pipolars[dev]

Includes: pytest, mypy, ruff, pre-commit

**Documentation Dependencies**

.. code-block:: bash

   uv sync --extra docs
   # or
   pip install pipolars[docs]

Includes: Sphinx, furo theme, sphinx-copybutton

Environment Variables
---------------------

PIPolars can be configured using environment variables:

.. code-block:: bash

   # PI Server configuration
   set PI_SERVER_HOST=my-pi-server
   set PI_SERVER_PORT=5450
   set PI_SERVER_TIMEOUT=30

   # Cache configuration
   set PIPOLARS_CACHE_BACKEND=sqlite
   set PIPOLARS_CACHE_TTL_HOURS=24

See :doc:`user_guide/configuration` for a complete list of configuration options.

Next Steps
----------

Once installation is complete, proceed to the :doc:`quickstart` guide to
learn how to use PIPolars.
