PIQuery
=======

.. module:: pipolars.api.query

The ``PIQuery`` class provides a fluent, method-chaining interface for
building PI data queries. It's accessed through ``PIClient.query()``.

PIQuery Class
-------------

.. autoclass:: pipolars.PIQuery
   :members:
   :undoc-members:
   :show-inheritance:

   .. rubric:: Time Range Methods

   .. automethod:: time_range
   .. automethod:: last
   .. automethod:: today
   .. automethod:: yesterday
   .. automethod:: this_week
   .. automethod:: this_month

   .. rubric:: Query Type Methods

   .. automethod:: recorded
   .. automethod:: interpolated
   .. automethod:: plot
   .. automethod:: summary
   .. automethod:: snapshot

   .. rubric:: Option Methods

   .. automethod:: with_quality
   .. automethod:: without_quality
   .. automethod:: boundary
   .. automethod:: filter
   .. automethod:: pivot
   .. automethod:: limit

   .. rubric:: Execution Methods

   .. automethod:: to_dataframe
   .. automethod:: to_lazy_frame

QueryOptions Class
------------------

.. autoclass:: pipolars.api.query.QueryOptions
   :members:
   :undoc-members:

QueryType Enum
--------------

.. autoclass:: pipolars.api.query.QueryType
   :members:
   :undoc-members:

Usage Examples
--------------

Basic Query Building
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient

   with PIClient("my-pi-server") as client:
       # Simple recorded values query
       df = (
           client.query("SINUSOID")
           .time_range("*-1d", "*")
           .recorded()
           .to_dataframe()
       )

       # Using convenience time methods
       df = (
           client.query("SINUSOID")
           .last(hours=24)
           .recorded()
           .to_dataframe()
       )

Interpolated Values
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Hourly interpolation
       df = (
           client.query("SINUSOID")
           .last(days=7)
           .interpolated(interval="1h")
           .to_dataframe()
       )

       # 15-minute intervals with quality
       df = (
           client.query("SINUSOID")
           .time_range("*-4h", "*")
           .interpolated(interval="15m")
           .with_quality()
           .to_dataframe()
       )

Multi-Tag Queries
~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Multiple tags, long format
       df = (
           client.query(["TAG1", "TAG2", "TAG3"])
           .last(hours=4)
           .interpolated(interval="10m")
           .to_dataframe()
       )

       # Pivot to wide format
       df = (
           client.query(["TAG1", "TAG2", "TAG3"])
           .time_range("*-2h", "*")
           .interpolated(interval="10m")
           .pivot()
           .to_dataframe()
       )

Summary Statistics
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient, SummaryType

   with PIClient("my-pi-server") as client:
       # Overall summary
       df = (
           client.query("SINUSOID")
           .last(days=7)
           .summary(
               SummaryType.AVERAGE,
               SummaryType.MINIMUM,
               SummaryType.MAXIMUM,
               SummaryType.STD_DEV
           )
           .to_dataframe()
       )

Lazy Evaluation
~~~~~~~~~~~~~~~

.. code-block:: python

   import polars as pl

   with PIClient("my-pi-server") as client:
       # Get LazyFrame for deferred execution
       lf = (
           client.query("SINUSOID")
           .last(days=30)
           .recorded()
           .to_lazy_frame()
       )

       # Build complex query
       result = (
           lf.filter(pl.col("value") > 50)
           .with_columns(
               pl.col("value").rolling_mean(window_size=100).alias("rolling")
           )
           .collect()  # Execute
       )

Method Chaining Patterns
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # All options at once
       df = (
           client.query(["TAG1", "TAG2"])
           .time_range("*-1d", "*")
           .interpolated(interval="30m")
           .with_quality()
           .pivot()
           .limit(1000)
           .to_dataframe()
       )

See Also
--------

- :doc:`client` - PIClient class
- :doc:`types` - SummaryType and other enums
- :doc:`../user_guide/querying` - Complete querying guide
