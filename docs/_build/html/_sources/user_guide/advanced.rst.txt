Advanced Usage
==============

This guide covers advanced PIPolars usage patterns for complex scenarios.

Bulk Operations
---------------

For large-scale data extraction, PIPolars optimizes bulk operations:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Extract many tags efficiently
       tags = client.search_tags("PLANT1.*")  # May return hundreds of tags

       # Bulk extraction with parallelization
       df = client.recorded_values(
           tags[:100],  # First 100 tags
           start="*-1d",
           end="*",
           pivot=True
       )

Parallel requests are configured via ``QueryConfig``:

.. code-block:: python

   from pipolars.core.config import QueryConfig

   config = PIConfig(
       server=PIServerConfig(host="my-server"),
       query=QueryConfig(
           parallel_requests=8,  # 8 concurrent requests
       ),
   )

Lazy Evaluation
---------------

Use LazyFrame for query optimization on large datasets:

.. code-block:: python

   with PIClient("my-pi-server") as client:
       # Get LazyFrame instead of DataFrame
       lf = (
           client.query("SINUSOID")
           .last(days=30)
           .recorded()
           .to_lazy_frame()
       )

       # Build query (not executed)
       result = (
           lf.filter(pl.col("value") > 50)
           .with_columns(
               pl.col("value").rolling_mean(window_size=100).alias("rolling")
           )
           .group_by(pl.col("timestamp").dt.date())
           .agg(pl.col("value").mean())
           .sort("timestamp")
       )

       # Execute optimized query
       df = result.collect()

Error Handling
--------------

Comprehensive error handling for production systems:

.. code-block:: python

   from pipolars import PIClient
   from pipolars.core.exceptions import (
       PIPolarsError,
       PIConnectionError,
       PIAuthenticationError,
       PIDataError,
       PIPointNotFoundError,
       PIQueryError,
       PITimeParseError,
       PIBulkOperationError,
   )

   def safe_query(client, tag, start, end):
       """Query with comprehensive error handling."""
       try:
           return client.recorded_values(tag, start, end)

       except PIPointNotFoundError as e:
           print(f"Tag not found: {e.tag}")
           return None

       except PITimeParseError as e:
           print(f"Invalid time expression: {e}")
           return None

       except PIConnectionError as e:
           print(f"Connection failed to {e.server}: {e.message}")
           raise

       except PIPolarsError as e:
           print(f"PI error: {e.message}")
           print(f"Details: {e.details}")
           raise

Handling Bulk Operation Failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars.core.exceptions import PIBulkOperationError

   try:
       df = client.recorded_values(
           ["TAG1", "TAG2", "INVALID_TAG", "TAG3"],
           start="*-1h",
           end="*"
       )
   except PIBulkOperationError as e:
       print(f"Succeeded: {e.succeeded}")
       print(f"Failed: {e.failed}")

       # Process successful results
       for tag in e.succeeded:
           print(f"Got data for {tag}")

Streaming Large Datasets
------------------------

For very large time ranges, process data in chunks:

.. code-block:: python

   from datetime import datetime, timedelta

   def stream_data(client, tag, start, end, chunk_days=7):
       """Stream data in chunks to avoid memory issues."""
       current = start

       while current < end:
           chunk_end = min(current + timedelta(days=chunk_days), end)

           df = client.recorded_values(
               tag,
               start=current.isoformat(),
               end=chunk_end.isoformat()
           )

           yield df

           current = chunk_end

   # Usage
   with PIClient("my-pi-server") as client:
       start = datetime(2024, 1, 1)
       end = datetime(2024, 12, 31)

       for chunk_df in stream_data(client, "SINUSOID", start, end):
           # Process each chunk
           process(chunk_df)

Custom Data Processing Pipeline
-------------------------------

Build reusable processing pipelines:

.. code-block:: python

   import polars as pl

   class PIDataPipeline:
       """Reusable data processing pipeline."""

       def __init__(self, client):
           self.client = client
           self.steps = []

       def add_step(self, step_fn):
           self.steps.append(step_fn)
           return self

       def execute(self, tags, start, end, **kwargs):
           df = self.client.recorded_values(tags, start, end, **kwargs)

           for step in self.steps:
               df = step(df)

           return df

   # Define processing steps
   def add_rolling_stats(df):
       return df.with_columns([
           pl.col("value").rolling_mean(window_size=12).alias("rolling_avg"),
           pl.col("value").rolling_std(window_size=12).alias("rolling_std"),
       ])

   def filter_outliers(df, z_threshold=3):
       mean = df["value"].mean()
       std = df["value"].std()
       return df.filter(
           ((pl.col("value") - mean) / std).abs() < z_threshold
       )

   def add_time_features(df):
       return df.with_columns([
           pl.col("timestamp").dt.hour().alias("hour"),
           pl.col("timestamp").dt.weekday().alias("day_of_week"),
       ])

   # Build and execute pipeline
   with PIClient("my-pi-server") as client:
       pipeline = (
           PIDataPipeline(client)
           .add_step(add_rolling_stats)
           .add_step(filter_outliers)
           .add_step(add_time_features)
       )

       df = pipeline.execute("SINUSOID", "*-7d", "*")

Logging and Monitoring
----------------------

Configure logging for debugging:

.. code-block:: python

   import logging

   # Enable debug logging
   logging.basicConfig(level=logging.DEBUG)
   logger = logging.getLogger("pipolars")
   logger.setLevel(logging.DEBUG)

   # Or via configuration
   config = PIConfig(
       server=PIServerConfig(host="my-server"),
       debug=True,
       log_level="DEBUG",
   )

Query Performance Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time

   class TimingClient:
       """Client wrapper with timing instrumentation."""

       def __init__(self, client):
           self.client = client
           self.timings = []

       def recorded_values(self, *args, **kwargs):
           start = time.perf_counter()
           result = self.client.recorded_values(*args, **kwargs)
           elapsed = time.perf_counter() - start

           self.timings.append({
               "method": "recorded_values",
               "elapsed": elapsed,
               "rows": len(result),
           })

           return result

       def report(self):
           import polars as pl
           return pl.DataFrame(self.timings).describe()

Connection Pooling Pattern
--------------------------

For long-running applications:

.. code-block:: python

   import threading
   from queue import Queue

   class PIConnectionPool:
       """Simple connection pool for PIPolars."""

       def __init__(self, server, size=4):
           self.server = server
           self.size = size
           self.pool = Queue(maxsize=size)
           self._lock = threading.Lock()

           # Pre-create connections
           for _ in range(size):
               client = PIClient(server)
               client.connect()
               self.pool.put(client)

       def acquire(self):
           return self.pool.get()

       def release(self, client):
           self.pool.put(client)

       def close_all(self):
           while not self.pool.empty():
               client = self.pool.get()
               client.disconnect()

   # Usage
   pool = PIConnectionPool("my-pi-server", size=4)

   try:
       client = pool.acquire()
       df = client.recorded_values("SINUSOID", "*-1h", "*")
       pool.release(client)
   finally:
       pool.close_all()

Context manager version:

.. code-block:: python

   from contextlib import contextmanager

   class PIConnectionPool:
       # ... previous implementation ...

       @contextmanager
       def connection(self):
           client = self.acquire()
           try:
               yield client
           finally:
               self.release(client)

   # Usage
   pool = PIConnectionPool("my-pi-server")

   with pool.connection() as client:
       df = client.recorded_values("SINUSOID", "*-1h", "*")

Working with AF Data
--------------------

Access AF elements and attributes:

.. code-block:: python

   from pipolars.connection.af_database import AFDatabaseConnection

   # Connect to AF Database
   af_conn = AFDatabaseConnection(
       server="my-af-server",
       database="MyDatabase"
   )
   af_conn.connect()

   # Navigate AF hierarchy
   root = af_conn.get_root_element()
   elements = af_conn.search_elements("Plant1|*")

   for element in elements:
       attributes = element.Attributes
       for attr in attributes:
           print(f"{element.Name}.{attr.Name}")

uv Script Dependencies
----------------------

Run standalone scripts with uv inline dependencies:

.. code-block:: python

   #!/usr/bin/env python3
   # /// script
   # requires-python = ">=3.10"
   # dependencies = ["pipolars"]
   # ///

   from pipolars import PIClient, SummaryType

   def main():
       with PIClient("my-pi-server") as client:
           df = (
               client.query("SINUSOID")
               .last(days=7)
               .summary(SummaryType.AVERAGE, SummaryType.MAXIMUM)
               .to_dataframe()
           )
           print(df)

   if __name__ == "__main__":
       main()

Run with:

.. code-block:: bash

   uv run my_script.py

Integration with Other Libraries
--------------------------------

With pandas
~~~~~~~~~~~

.. code-block:: python

   # Convert to pandas
   pandas_df = df.to_pandas()

   # Use pandas functionality
   pandas_df.to_excel("output.xlsx")

With NumPy
~~~~~~~~~~

.. code-block:: python

   import numpy as np

   # Get numpy array
   values = df["value"].to_numpy()

   # Perform numpy operations
   fft_result = np.fft.fft(values)

With scikit-learn
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sklearn.preprocessing import StandardScaler
   from sklearn.cluster import KMeans

   # Prepare features
   X = df.select(["value", "rolling_avg"]).to_numpy()

   # Scale and cluster
   scaler = StandardScaler()
   X_scaled = scaler.fit_transform(X)

   kmeans = KMeans(n_clusters=3)
   clusters = kmeans.fit_predict(X_scaled)

   # Add back to DataFrame
   df = df.with_columns(pl.Series("cluster", clusters))

With Plotly
~~~~~~~~~~~

.. code-block:: python

   import plotly.express as px

   # Convert to pandas for Plotly
   pandas_df = df.to_pandas()

   fig = px.line(
       pandas_df,
       x="timestamp",
       y="value",
       title="PI Data"
   )
   fig.show()

Next Steps
----------

- :doc:`../api/index` - Complete API reference
- :doc:`caching` - Caching configuration
- :doc:`configuration` - Full configuration options
