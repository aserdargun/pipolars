PIClient
========

.. module:: pipolars.api.client

The ``PIClient`` class is the main entry point for interacting with the
PI System. It provides methods for querying PI Points, extracting data,
and managing connections.

PIClient Class
--------------

.. autoclass:: pipolars.PIClient
   :members:
   :undoc-members:
   :show-inheritance:

   .. rubric:: Connection Methods

   .. automethod:: connect
   .. automethod:: disconnect

   .. rubric:: Snapshot Methods

   .. automethod:: snapshot
   .. automethod:: snapshots

   .. rubric:: Historical Data Methods

   .. automethod:: recorded_values
   .. automethod:: interpolated_values
   .. automethod:: plot_values

   .. rubric:: Summary Methods

   .. automethod:: summary
   .. automethod:: summaries

   .. rubric:: Query Builder

   .. automethod:: query

   .. rubric:: Tag Operations

   .. automethod:: search_tags
   .. automethod:: tag_exists
   .. automethod:: tag_info

   .. rubric:: Convenience Methods

   .. automethod:: last
   .. automethod:: today

   .. rubric:: Cache Management

   .. automethod:: cache_stats
   .. automethod:: clear_cache

   .. rubric:: Properties

   .. autoproperty:: config
   .. autoproperty:: is_connected
   .. autoproperty:: server_name

Usage Examples
--------------

Basic Connection
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient

   # Using context manager (recommended)
   with PIClient("my-pi-server") as client:
       df = client.snapshot("SINUSOID")
       print(df)

   # Manual connection management
   client = PIClient("my-pi-server")
   client.connect()
   try:
       df = client.snapshot("SINUSOID")
   finally:
       client.disconnect()

Historical Data Retrieval
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Recorded values
       df = client.recorded_values(
           "SINUSOID",
           start="*-1d",
           end="*",
           include_quality=True
       )

       # Interpolated values
       df = client.interpolated_values(
           ["TAG1", "TAG2"],
           start="*-1d",
           end="*",
           interval="1h",
           pivot=True
       )

Summary Statistics
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient, SummaryType

   with PIClient("my-pi-server") as client:
       # Overall summary
       df = client.summary(
           "SINUSOID",
           start="*-7d",
           end="*",
           summary_types=[
               SummaryType.AVERAGE,
               SummaryType.MINIMUM,
               SummaryType.MAXIMUM
           ]
       )

       # Interval summaries
       df = client.summaries(
           "SINUSOID",
           start="*-1d",
           end="*",
           interval="1h",
           summary_types=SummaryType.AVERAGE
       )

Configuration
~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient, PIConfig
   from pipolars.core.config import PIServerConfig, CacheConfig, CacheBackend

   config = PIConfig(
       server=PIServerConfig(
           host="my-pi-server",
           timeout=60
       ),
       cache=CacheConfig(
           backend=CacheBackend.SQLITE
       )
   )

   with PIClient(config=config) as client:
       df = client.recorded_values("SINUSOID", "*-1d", "*")

See Also
--------

- :doc:`query` - PIQuery fluent query builder
- :doc:`config` - Configuration options
- :doc:`../user_guide/connecting` - Connection guide
- :doc:`../user_guide/querying` - Querying guide
