Working with Polars DataFrames
==============================

PIPolars returns data as `Polars <https://pola.rs/>`_ DataFrames, which offer
significant performance advantages over pandas for many operations.

Why Polars?
-----------

Polars provides:

- **Speed**: 10-100x faster than pandas for many operations
- **Memory efficiency**: Zero-copy operations and memory-mapped I/O
- **Lazy evaluation**: Query optimization before execution
- **Parallel execution**: Automatic multi-core utilization
- **Type safety**: Strict column types and better null handling
- **Modern API**: Consistent, expression-based interface

DataFrame Structure
-------------------

Single Tag Output
~~~~~~~~~~~~~~~~~

For single-tag queries, the DataFrame has these columns:

.. code-block:: text

   shape: (1000, 2)
   +-------------------------+----------+
   | timestamp               | value    |
   | ---                     | ---      |
   | datetime[us, UTC]       | f64      |
   +-------------------------+----------+
   | 2024-01-15 10:00:00 UTC | 50.5     |
   | 2024-01-15 10:01:00 UTC | 51.2     |
   | 2024-01-15 10:02:00 UTC | 49.8     |
   ...

With quality information:

.. code-block:: text

   shape: (1000, 3)
   +-------------------------+----------+---------+
   | timestamp               | value    | quality |
   | ---                     | ---      | ---     |
   | datetime[us, UTC]       | f64      | i8      |
   +-------------------------+----------+---------+
   | 2024-01-15 10:00:00 UTC | 50.5     | 0       |
   | 2024-01-15 10:01:00 UTC | 51.2     | 0       |
   ...

Multi-Tag Output (Long Format)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For multi-tag queries, the default is "long" format:

.. code-block:: text

   shape: (3000, 3)
   +-------------------------+----------+-------+
   | timestamp               | tag      | value |
   | ---                     | ---      | ---   |
   | datetime[us, UTC]       | str      | f64   |
   +-------------------------+----------+-------+
   | 2024-01-15 10:00:00 UTC | TAG1     | 50.5  |
   | 2024-01-15 10:00:00 UTC | TAG2     | 75.2  |
   | 2024-01-15 10:00:00 UTC | TAG3     | 22.1  |
   | 2024-01-15 10:01:00 UTC | TAG1     | 51.3  |
   ...

Multi-Tag Output (Wide Format)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With ``pivot=True``, tags become columns:

.. code-block:: text

   shape: (1000, 4)
   +-------------------------+-------+-------+-------+
   | timestamp               | TAG1  | TAG2  | TAG3  |
   | ---                     | ---   | ---   | ---   |
   | datetime[us, UTC]       | f64   | f64   | f64   |
   +-------------------------+-------+-------+-------+
   | 2024-01-15 10:00:00 UTC | 50.5  | 75.2  | 22.1  |
   | 2024-01-15 10:01:00 UTC | 51.3  | 76.1  | 21.8  |
   ...

Basic Operations
----------------

Viewing Data
~~~~~~~~~~~~

.. code-block:: python

   import polars as pl

   # View first/last rows
   print(df.head(10))
   print(df.tail(10))

   # View schema
   print(df.schema)

   # Summary statistics
   print(df.describe())

   # Shape
   print(f"Rows: {len(df)}, Columns: {df.width}")

Selecting Columns
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Select columns
   df.select("timestamp", "value")
   df.select(pl.col("timestamp"), pl.col("value"))

   # Exclude columns
   df.select(pl.all().exclude("quality"))

Filtering Data
~~~~~~~~~~~~~~

.. code-block:: python

   # Simple filter
   df.filter(pl.col("value") > 50)

   # Multiple conditions
   df.filter(
       (pl.col("value") > 50) &
       (pl.col("value") < 100)
   )

   # Filter by time
   df.filter(
       pl.col("timestamp") > datetime(2024, 1, 15, 12, 0)
   )

   # Filter good quality only
   df.filter(pl.col("quality") == 0)

Transformations
---------------

Adding Calculated Columns
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   df = df.with_columns([
       # Rolling average
       pl.col("value").rolling_mean(window_size=12).alias("rolling_avg"),

       # Difference from previous
       pl.col("value").diff().alias("change"),

       # Percent change
       pl.col("value").pct_change().alias("pct_change"),

       # Absolute value
       pl.col("value").abs().alias("abs_value"),

       # Lag values
       pl.col("value").shift(1).alias("prev_value"),
   ])

Time-Based Operations
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   df = df.with_columns([
       # Extract date components
       pl.col("timestamp").dt.hour().alias("hour"),
       pl.col("timestamp").dt.weekday().alias("day_of_week"),
       pl.col("timestamp").dt.date().alias("date"),

       # Time since start
       (pl.col("timestamp") - pl.col("timestamp").first()).alias("elapsed"),
   ])

Aggregations
~~~~~~~~~~~~

.. code-block:: python

   # Group by time period
   hourly = (
       df.group_by(pl.col("timestamp").dt.truncate("1h"))
       .agg([
           pl.col("value").mean().alias("avg"),
           pl.col("value").min().alias("min"),
           pl.col("value").max().alias("max"),
           pl.col("value").std().alias("std"),
           pl.col("value").count().alias("count"),
       ])
   )

   # Group by tag (for multi-tag data)
   by_tag = (
       df.group_by("tag")
       .agg([
           pl.col("value").mean().alias("avg"),
           pl.col("value").std().alias("std"),
       ])
   )

Common Data Science Patterns
----------------------------

Anomaly Detection
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Z-score based anomaly detection
   df = df.with_columns([
       ((pl.col("value") - pl.col("value").mean()) /
        pl.col("value").std()).alias("z_score")
   ])

   anomalies = df.filter(pl.col("z_score").abs() > 3)

   # IQR-based detection
   q1 = df["value"].quantile(0.25)
   q3 = df["value"].quantile(0.75)
   iqr = q3 - q1

   outliers = df.filter(
       (pl.col("value") < q1 - 1.5 * iqr) |
       (pl.col("value") > q3 + 1.5 * iqr)
   )

Resampling
~~~~~~~~~~

.. code-block:: python

   # Resample to hourly
   hourly = (
       df.sort("timestamp")
       .group_by_dynamic("timestamp", every="1h")
       .agg([
           pl.col("value").mean().alias("value"),
       ])
   )

   # Resample with multiple aggregations
   daily = (
       df.sort("timestamp")
       .group_by_dynamic("timestamp", every="1d")
       .agg([
           pl.col("value").first().alias("open"),
           pl.col("value").max().alias("high"),
           pl.col("value").min().alias("low"),
           pl.col("value").last().alias("close"),
           pl.col("value").mean().alias("avg"),
       ])
   )

Gap Detection
~~~~~~~~~~~~~

.. code-block:: python

   # Find gaps in data
   df = df.with_columns([
       (pl.col("timestamp") - pl.col("timestamp").shift(1)).alias("time_diff")
   ])

   # Gaps larger than expected interval
   gaps = df.filter(pl.col("time_diff") > timedelta(minutes=5))

Correlation Analysis
~~~~~~~~~~~~~~~~~~~~

For multi-tag pivoted data:

.. code-block:: python

   # Get pivoted data
   df = client.interpolated_values(
       ["TAG1", "TAG2", "TAG3"],
       "*-1d", "*", interval="15m", pivot=True
   )

   # Calculate correlation
   correlation = df.select(["TAG1", "TAG2", "TAG3"]).corr()
   print(correlation)

LazyFrame for Large Data
------------------------

For large datasets, use LazyFrame for query optimization:

.. code-block:: python

   # Get LazyFrame
   lf = (
       client.query("SINUSOID")
       .last(days=30)
       .recorded()
       .to_lazy_frame()
   )

   # Build query (not executed yet)
   result = (
       lf.filter(pl.col("value") > 50)
       .with_columns(pl.col("value").rolling_mean(window_size=100).alias("rolling"))
       .group_by(pl.col("timestamp").dt.date())
       .agg(pl.col("value").mean())
   )

   # Execute query
   df = result.collect()

Converting to Other Formats
---------------------------

To pandas
~~~~~~~~~

.. code-block:: python

   pandas_df = df.to_pandas()

To Arrow
~~~~~~~~

.. code-block:: python

   arrow_table = df.to_arrow()

To CSV
~~~~~~

.. code-block:: python

   df.write_csv("data.csv")

To Parquet
~~~~~~~~~~

.. code-block:: python

   df.write_parquet("data.parquet")

To JSON
~~~~~~~

.. code-block:: python

   df.write_json("data.json")

Performance Tips
----------------

1. **Use LazyFrame** for complex queries:

   .. code-block:: python

      lf = df.lazy()
      result = lf.filter(...).with_columns(...).collect()

2. **Avoid loops** - use vectorized operations:

   .. code-block:: python

      # Bad
      for i in range(len(df)):
          process(df[i])

      # Good
      df.with_columns(pl.col("value").map_elements(process))

3. **Select only needed columns**:

   .. code-block:: python

      df.select(["timestamp", "value"])  # Faster than using all columns

4. **Use appropriate data types**:

   .. code-block:: python

      df = df.cast({"value": pl.Float32})  # Save memory if precision not needed

5. **Leverage parallel processing**:

   .. code-block:: python

      import polars as pl
      pl.set_random_seed(0)
      pl.Config.set_streaming_chunk_size(100_000)

Next Steps
----------

- `Polars User Guide <https://docs.pola.rs/>`_
- :doc:`caching` - Cache results for better performance
- :doc:`advanced` - Advanced usage patterns
