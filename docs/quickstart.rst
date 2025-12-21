Quickstart
==========

This guide will help you get started with PIPolars quickly.

Basic Connection
----------------

The simplest way to connect to a PI Server and retrieve data:

.. code-block:: python

   from pipolars import PIClient

   # Connect using hostname
   with PIClient("my-pi-server") as client:
       # Get current value (snapshot)
       df = client.snapshot("SINUSOID")
       print(df)

Output:

.. code-block:: text

   shape: (1, 2)
   +-------------------------+----------+
   | timestamp               | value    |
   | ---                     | ---      |
   | datetime[us, UTC]       | f64      |
   +-------------------------+----------+
   | 2024-01-15 10:30:00 UTC | 50.5     |
   +-------------------------+----------+

Retrieving Historical Data
--------------------------

Recorded Values
~~~~~~~~~~~~~~~

Get actual recorded values from the archive:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Get last 24 hours of data
       df = client.recorded_values(
           "SINUSOID",
           start="*-1d",
           end="*"
       )
       print(f"Retrieved {len(df)} values")
       print(df.head())

Interpolated Values
~~~~~~~~~~~~~~~~~~~

Get values at regular intervals:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Get hourly values
       df = client.interpolated_values(
           "SINUSOID",
           start="*-1d",
           end="*",
           interval="1h"
       )
       print(df)

Working with Multiple Tags
--------------------------

PIPolars efficiently handles multiple tags:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Query multiple tags at once
       df = client.recorded_values(
           ["TAG1", "TAG2", "TAG3"],
           start="*-1h",
           end="*"
       )
       print(df)

       # Pivot to wide format (tags as columns)
       df_pivot = client.interpolated_values(
           ["TAG1", "TAG2", "TAG3"],
           start="*-2h",
           end="*",
           interval="15m",
           pivot=True
       )
       print(df_pivot)

Using the Query Builder
-----------------------

The fluent query builder provides a readable, declarative API:

.. code-block:: python

   from pipolars import PIClient, SummaryType

   with PIClient("my-pi-server") as client:
       # Fluent query building
       df = (
           client.query("SINUSOID")
           .last(hours=24)
           .interpolated(interval="30m")
           .with_quality()
           .to_dataframe()
       )

       # Multi-tag query with pivot
       df = (
           client.query(["TAG1", "TAG2", "TAG3"])
           .time_range("*-4h", "*")
           .interpolated(interval="10m")
           .pivot()
           .to_dataframe()
       )

       # Summary statistics
       df = (
           client.query("SINUSOID")
           .last(days=7)
           .summary(
               SummaryType.AVERAGE,
               SummaryType.MAXIMUM,
               SummaryType.MINIMUM
           )
           .to_dataframe()
       )

Summary Statistics
------------------

Calculate summary statistics over a time range:

.. code-block:: python

   from pipolars import PIClient, SummaryType

   with PIClient("my-pi-server") as client:
       # Get summary for a time range
       df = client.summary(
           "SINUSOID",
           start="*-7d",
           end="*",
           summary_types=[
               SummaryType.AVERAGE,
               SummaryType.MINIMUM,
               SummaryType.MAXIMUM,
               SummaryType.STD_DEV
           ]
       )
       print(df)

       # Get interval summaries (e.g., hourly averages)
       df = client.summaries(
           "SINUSOID",
           start="*-1d",
           end="*",
           interval="1h",
           summary_types=SummaryType.AVERAGE
       )
       print(df)

Convenience Methods
-------------------

PIPolars provides convenience methods for common queries:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Get last N hours/days
       df = client.last("SINUSOID", hours=4)

       # Get today's data
       df = client.today("SINUSOID")

       # Get multiple tags
       df = client.last(["TAG1", "TAG2"], days=7)

Time Expressions
----------------

PIPolars supports PI time expressions:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Expression
     - Description
   * - ``*``
     - Now
   * - ``*-1h``
     - 1 hour ago
   * - ``*-1d``
     - 1 day ago
   * - ``*-7d``
     - 7 days ago
   * - ``t``
     - Today at midnight
   * - ``y``
     - Yesterday at midnight
   * - ``2024-01-15``
     - Absolute date
   * - ``2024-01-15T10:00:00``
     - Absolute datetime

Data Processing with Polars
---------------------------

PIPolars returns Polars DataFrames, enabling fast data processing:

.. code-block:: python

   import polars as pl
   from pipolars import PIClient

   with PIClient("my-pi-server") as client:
       # Get data
       df = client.interpolated_values(
           "SINUSOID",
           start="*-1d",
           end="*",
           interval="5m"
       )

       # Process with Polars
       result = (
           df.with_columns([
               pl.col("value").rolling_mean(window_size=12).alias("rolling_avg"),
               pl.col("value").diff().alias("change"),
               pl.col("value").pct_change().alias("pct_change"),
           ])
           .filter(pl.col("value") > 50)
           .select([
               "timestamp",
               "value",
               "rolling_avg",
               "change",
           ])
       )

       print(result.head(10))

Searching for Tags
------------------

Find tags using wildcard patterns:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Search for tags matching a pattern
       tags = client.search_tags("SINU*")
       print(f"Found {len(tags)} tags: {tags}")

       # Check if a tag exists
       if client.tag_exists("SINUSOID"):
           print("Tag exists!")

       # Get tag metadata
       info = client.tag_info("SINUSOID")
       print(info)

Next Steps
----------

- :doc:`user_guide/connecting` - Learn about connection options
- :doc:`user_guide/querying` - Explore query methods in detail
- :doc:`user_guide/caching` - Set up caching for better performance
- :doc:`api/index` - Complete API reference
