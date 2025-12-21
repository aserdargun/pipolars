PIPolars Documentation
======================

**High-performance PI System data extraction library with Polars DataFrames**

PIPolars is a modern Python library for extracting data from OSIsoft PI System
and converting it to `Polars <https://pola.rs/>`_ DataFrames for efficient data
science workflows.

.. note::

   PIPolars requires **Windows** with the **OSIsoft PI AF SDK** installed.
   See :doc:`installation` for detailed requirements.

Features
--------

- **Polars DataFrames**: 10-100x faster than pandas for many operations
- **Modern Python**: Full type hints, Pydantic configuration, Python 3.10+
- **Efficient Bulk Operations**: Native bulk API support for extracting multiple tags
- **Lazy Evaluation**: Polars LazyFrame support for query optimization
- **Caching**: SQLite and Arrow IPC caching for reduced server load
- **Fluent Query API**: Method chaining for readable, declarative queries
- **uv Compatible**: Modern package management with Astral's uv

Quick Example
-------------

.. code-block:: python

   from pipolars import PIClient

   # Connect to PI Server and get data as Polars DataFrame
   with PIClient("my-pi-server") as client:
       # Get last 24 hours of data
       df = client.recorded_values("SINUSOID", start="*-1d", end="*")
       print(f"Retrieved {len(df)} values")

       # Use fluent query builder
       df = (
           client.query(["TAG1", "TAG2"])
           .last(hours=4)
           .interpolated(interval="15m")
           .pivot()
           .to_dataframe()
       )

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   installation
   quickstart

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user_guide/index
   user_guide/connecting
   user_guide/querying
   user_guide/time_expressions
   user_guide/dataframes
   user_guide/caching
   user_guide/configuration
   user_guide/advanced

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index
   api/client
   api/query
   api/types
   api/config
   api/exceptions
   api/cache

.. toctree::
   :maxdepth: 1
   :caption: Additional Information

   changelog
   contributing
   license

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
