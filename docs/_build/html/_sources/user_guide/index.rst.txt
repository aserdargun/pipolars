User Guide
==========

This guide provides comprehensive documentation on using PIPolars for
PI System data extraction and analysis.

Overview
--------

PIPolars is designed around a layered architecture:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Layer
     - Purpose
   * - **API**
     - User-facing interface (PIClient, PIQuery)
   * - **Connection**
     - PI Server and AF Database connectivity
   * - **Extraction**
     - Data retrieval from PI System
   * - **Transform**
     - Data conversion to Polars DataFrames
   * - **Cache**
     - Result caching for performance
   * - **Core**
     - Types, configuration, and exceptions

Typical Workflow
----------------

A typical PIPolars workflow:

1. **Configure** the client with server settings and optional caching
2. **Connect** to the PI Server (automatically or explicitly)
3. **Query** data using direct methods or the fluent query builder
4. **Process** the resulting Polars DataFrame
5. **Disconnect** (automatic with context manager)

.. code-block:: python

   from pipolars import PIClient, PIConfig
   from pipolars.core.config import CacheBackend, CacheConfig, PIServerConfig

   # 1. Configure
   config = PIConfig(
       server=PIServerConfig(host="my-pi-server"),
       cache=CacheConfig(backend=CacheBackend.SQLITE),
   )

   # 2. Connect
   with PIClient(config=config) as client:
       # 3. Query
       df = (
           client.query("SINUSOID")
           .last(hours=24)
           .interpolated(interval="15m")
           .to_dataframe()
       )

       # 4. Process
       result = df.filter(df["value"] > 50)
       print(result)

   # 5. Disconnect (automatic)

Guide Contents
--------------

.. toctree::
   :maxdepth: 2

   connecting
   querying
   time_expressions
   dataframes
   caching
   configuration
   advanced
