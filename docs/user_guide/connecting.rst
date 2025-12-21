Connecting to PI Server
=======================

This guide covers the various ways to connect to a PI Data Archive server
using PIPolars.

Basic Connection
----------------

The simplest way to connect is using a hostname:

.. code-block:: python

   from pipolars import PIClient

   # Connect using hostname
   with PIClient("my-pi-server") as client:
       df = client.snapshot("SINUSOID")
       print(df)

The context manager ensures proper connection cleanup. When you exit the
``with`` block, the connection is automatically closed.

Manual Connection Management
----------------------------

You can also manage connections manually:

.. code-block:: python

   client = PIClient("my-pi-server")

   try:
       # Explicitly connect
       client.connect()

       # Use the client
       df = client.snapshot("SINUSOID")
       print(df)

   finally:
       # Always disconnect
       client.disconnect()

Connection Properties
---------------------

Check connection status:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       print(f"Connected: {client.is_connected}")
       print(f"Server: {client.server_name}")

Server Configuration
--------------------

For more control, use ``PIServerConfig``:

.. code-block:: python

   from pipolars import PIClient
   from pipolars.core.config import PIServerConfig

   config = PIServerConfig(
       host="my-pi-server",
       port=5450,          # Default PI port
       timeout=30,         # Connection timeout in seconds
   )

   with PIClient(server=config) as client:
       df = client.snapshot("SINUSOID")

Authentication
--------------

Windows Authentication (Default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, PIPolars uses Windows integrated authentication:

.. code-block:: python

   from pipolars.core.config import AuthMethod, PIServerConfig

   config = PIServerConfig(
       host="my-pi-server",
       auth_method=AuthMethod.WINDOWS,  # Default
   )

This uses your current Windows credentials (NTLM/Kerberos).

Explicit Authentication
~~~~~~~~~~~~~~~~~~~~~~~

For explicit username/password authentication:

.. code-block:: python

   from pipolars.core.config import AuthMethod, PIServerConfig

   config = PIServerConfig(
       host="my-pi-server",
       auth_method=AuthMethod.EXPLICIT,
       username="pi_user",
       password="secret",
   )

.. warning::

   Avoid hardcoding credentials in source code. Use environment variables
   or a secrets manager instead.

Using Environment Variables
---------------------------

PIPolars can read configuration from environment variables:

.. code-block:: bash

   # Windows Command Prompt
   set PI_SERVER_HOST=my-pi-server
   set PI_SERVER_PORT=5450
   set PI_SERVER_TIMEOUT=30

   # PowerShell
   $env:PI_SERVER_HOST = "my-pi-server"
   $env:PI_SERVER_PORT = "5450"

Then in Python:

.. code-block:: python

   from pipolars import PIClient
   from pipolars.core.config import PIServerConfig

   # Configuration is loaded from environment
   config = PIServerConfig()
   with PIClient(server=config) as client:
       df = client.snapshot("SINUSOID")

Discovering Servers
-------------------

List available PI Servers:

.. code-block:: python

   from pipolars.connection.server import PIServerConnection

   # List all known servers
   servers = PIServerConnection.list_servers()
   print(f"Available servers: {servers}")

   # Get the default server
   default = PIServerConnection.get_default_server()
   print(f"Default server: {default}")

Low-Level Connection
--------------------

For advanced use cases, you can use the connection classes directly:

.. code-block:: python

   from pipolars.connection.server import PIServerConnection, pi_connection

   # Using context manager
   with pi_connection("my-pi-server") as conn:
       point = conn.get_point("SINUSOID")
       print(f"Point: {point.Name}")

   # Using connection class
   conn = PIServerConnection("my-pi-server")
   try:
       conn.connect()
       points = conn.search_points("SINU*")
       for p in points:
           print(p.Name)
   finally:
       conn.disconnect()

Connection Pooling
------------------

For high-throughput applications, reuse client connections:

.. code-block:: python

   # Create client once
   client = PIClient("my-pi-server")
   client.connect()

   try:
       # Reuse for multiple queries
       for tag in ["TAG1", "TAG2", "TAG3"]:
           df = client.recorded_values(tag, "*-1h", "*")
           process(df)
   finally:
       client.disconnect()

Error Handling
--------------

Handle connection errors gracefully:

.. code-block:: python

   from pipolars import PIClient
   from pipolars.core.exceptions import PIConnectionError, PIAuthenticationError

   try:
       with PIClient("my-pi-server") as client:
           df = client.snapshot("SINUSOID")

   except PIAuthenticationError as e:
       print(f"Authentication failed: {e}")

   except PIConnectionError as e:
       print(f"Connection failed: {e}")
       print(f"Server: {e.server}")
       print(f"Details: {e.details}")

Next Steps
----------

- :doc:`querying` - Learn how to query data
- :doc:`configuration` - Complete configuration options
- :doc:`../api/client` - PIClient API reference
