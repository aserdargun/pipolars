Querying Data
=============

PIPolars provides multiple ways to query data from PI System. This guide
covers all the query methods and options.

Query Methods Overview
----------------------

PIPolars offers two approaches to querying:

1. **Direct methods** on ``PIClient`` - Simple, direct calls
2. **Query builder** (``PIQuery``) - Fluent, method-chaining interface

Both produce identical results; choose based on your preference.

Direct Methods
--------------

Snapshot Values
~~~~~~~~~~~~~~~

Get the current value of a tag:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Single tag
       df = client.snapshot("SINUSOID")

       # Multiple tags
       df = client.snapshots(["TAG1", "TAG2", "TAG3"])

Recorded Values
~~~~~~~~~~~~~~~

Get actual archived values:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Basic query
       df = client.recorded_values(
           "SINUSOID",
           start="*-1d",
           end="*"
       )

       # With options
       df = client.recorded_values(
           "SINUSOID",
           start="*-1d",
           end="*",
           max_count=1000,          # Limit values
           include_quality=True,     # Add quality column
       )

       # Multiple tags
       df = client.recorded_values(
           ["TAG1", "TAG2", "TAG3"],
           start="*-1h",
           end="*",
           pivot=True  # Tags become columns
       )

Interpolated Values
~~~~~~~~~~~~~~~~~~~

Get values at regular intervals:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Hourly interpolation
       df = client.interpolated_values(
           "SINUSOID",
           start="*-1d",
           end="*",
           interval="1h"
       )

       # 15-minute intervals
       df = client.interpolated_values(
           "SINUSOID",
           start="*-4h",
           end="*",
           interval="15m"
       )

       # Multiple tags, pivoted
       df = client.interpolated_values(
           ["TAG1", "TAG2"],
           start="*-2h",
           end="*",
           interval="10m",
           pivot=True
       )

Plot Values
~~~~~~~~~~~

Get values optimized for plotting (reduced density):

.. code-block:: python

   with PIClient("my-pi-server") as client:
       df = client.plot_values(
           "SINUSOID",
           start="*-7d",
           end="*",
           intervals=640  # Number of plot points
       )

Summary Values
~~~~~~~~~~~~~~

Calculate statistics over a time range:

.. code-block:: python

   from pipolars import SummaryType

   with PIClient("my-pi-server") as client:
       # Single summary over entire range
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

       # Time-series summaries (e.g., hourly averages)
       df = client.summaries(
           "SINUSOID",
           start="*-1d",
           end="*",
           interval="1h",
           summary_types=SummaryType.AVERAGE
       )

Query Builder (Fluent API)
--------------------------

The query builder provides a readable, chainable interface:

Basic Usage
~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       df = (
           client.query("SINUSOID")
           .time_range("*-1d", "*")
           .recorded()
           .to_dataframe()
       )

Time Range Methods
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Explicit time range
   query.time_range("*-1d", "*")

   # Last N time units
   query.last(hours=24)
   query.last(days=7)
   query.last(minutes=30)
   query.last(hours=2, minutes=30)

   # Preset ranges
   query.today()
   query.yesterday()
   query.this_week()
   query.this_month()

Query Type Methods
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Recorded values
   query.recorded()
   query.recorded(max_count=1000)

   # Interpolated values
   query.interpolated(interval="1h")
   query.interpolated(interval="15m")

   # Plot-optimized
   query.plot(intervals=640)

   # Summary statistics
   query.summary(SummaryType.AVERAGE)
   query.summary(SummaryType.AVERAGE, SummaryType.MAXIMUM)

   # Current snapshot
   query.snapshot()

Option Methods
~~~~~~~~~~~~~~

.. code-block:: python

   # Include quality information
   query.with_quality()

   # Pivot to wide format
   query.pivot()

   # Limit results
   query.limit(1000)

Execution Methods
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Get DataFrame
   df = query.to_dataframe()

   # Get LazyFrame (for deferred execution)
   lf = query.to_lazy_frame()

Complete Examples
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient, SummaryType

   with PIClient("my-pi-server") as client:
       # Example 1: 24 hours of interpolated data with quality
       df = (
           client.query("SINUSOID")
           .last(hours=24)
           .interpolated(interval="15m")
           .with_quality()
           .to_dataframe()
       )

       # Example 2: Multiple tags, pivoted
       df = (
           client.query(["TAG1", "TAG2", "TAG3"])
           .time_range("*-4h", "*")
           .interpolated(interval="10m")
           .pivot()
           .to_dataframe()
       )

       # Example 3: Weekly summary statistics
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

       # Example 4: Today's snapshots
       df = (
           client.query(["TAG1", "TAG2"])
           .today()
           .recorded()
           .to_dataframe()
       )

Convenience Methods
-------------------

PIPolars provides shortcut methods for common queries:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Get last N hours/days/minutes
       df = client.last("SINUSOID", hours=24)
       df = client.last("SINUSOID", days=7)
       df = client.last(["TAG1", "TAG2"], hours=4)

       # Get today's data
       df = client.today("SINUSOID")
       df = client.today(["TAG1", "TAG2"])

Tag Operations
--------------

Searching for Tags
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Wildcard search
       tags = client.search_tags("SINU*")
       tags = client.search_tags("*TEMP*")
       tags = client.search_tags("UNIT1.*")

       # Limit results
       tags = client.search_tags("*", max_results=100)

Checking Tag Existence
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       if client.tag_exists("SINUSOID"):
           df = client.snapshot("SINUSOID")

Tag Information
~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient("my-pi-server") as client:
       info = client.tag_info("SINUSOID")
       print(info)
       # {
       #     'name': 'SINUSOID',
       #     'point_id': 1234,
       #     'point_type': 'float64',
       #     'description': 'Demo sinusoid tag',
       #     'engineering_units': '',
       #     'zero': 0.0,
       #     'span': 100.0
       # }

Output Format Options
---------------------

Wide vs Long Format
~~~~~~~~~~~~~~~~~~~

By default, multi-tag queries return data in "long" format:

.. code-block:: text

   shape: (100, 3)
   +-------------------------+----------+-------+
   | timestamp               | tag      | value |
   | ---                     | ---      | ---   |
   | datetime[us, UTC]       | str      | f64   |
   +-------------------------+----------+-------+
   | 2024-01-15 10:00:00 UTC | TAG1     | 50.5  |
   | 2024-01-15 10:00:00 UTC | TAG2     | 75.2  |
   | 2024-01-15 10:15:00 UTC | TAG1     | 51.3  |
   ...

Use ``pivot=True`` for "wide" format:

.. code-block:: text

   shape: (50, 3)
   +-------------------------+-------+-------+
   | timestamp               | TAG1  | TAG2  |
   | ---                     | ---   | ---   |
   | datetime[us, UTC]       | f64   | f64   |
   +-------------------------+-------+-------+
   | 2024-01-15 10:00:00 UTC | 50.5  | 75.2  |
   | 2024-01-15 10:15:00 UTC | 51.3  | 76.1  |
   ...

Quality Information
~~~~~~~~~~~~~~~~~~~

Include quality flags with ``include_quality=True``:

.. code-block:: python

   df = client.recorded_values(
       "SINUSOID",
       start="*-1h",
       end="*",
       include_quality=True
   )

Output:

.. code-block:: text

   shape: (100, 3)
   +-------------------------+-------+---------+
   | timestamp               | value | quality |
   | ---                     | ---   | ---     |
   | datetime[us, UTC]       | f64   | i8      |
   +-------------------------+-------+---------+
   | 2024-01-15 10:00:00 UTC | 50.5  | 0       |
   | 2024-01-15 10:01:00 UTC | 51.2  | 0       |
   ...

Quality values:

- 0: Good
- 1: Substituted
- 2: Questionable
- 3: Bad
- 4: No Data
- 5: Calculation Failed

Next Steps
----------

- :doc:`time_expressions` - Detailed time expression syntax
- :doc:`dataframes` - Working with Polars DataFrames
- :doc:`../api/query` - PIQuery API reference
