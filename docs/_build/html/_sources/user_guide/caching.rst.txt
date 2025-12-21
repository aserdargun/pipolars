Caching
=======

PIPolars includes a flexible caching system to reduce server load and improve
query performance for repeated requests.

Overview
--------

The caching system supports multiple backends:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Backend
     - Description
   * - ``none``
     - No caching (default)
   * - ``memory``
     - In-memory LRU cache (fast, lost on restart)
   * - ``sqlite``
     - SQLite database cache (persistent)
   * - ``arrow``
     - Arrow IPC file cache (optimal for Polars)

Enabling Caching
----------------

Basic Configuration
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pipolars import PIClient, PIConfig
   from pipolars.core.config import CacheBackend, CacheConfig, PIServerConfig

   config = PIConfig(
       server=PIServerConfig(host="my-pi-server"),
       cache=CacheConfig(
           backend=CacheBackend.SQLITE,  # Enable SQLite cache
       ),
   )

   with PIClient(config=config) as client:
       # First query - fetches from PI Server
       df1 = client.recorded_values("SINUSOID", "*-1h", "*")

       # Second query - served from cache (faster)
       df2 = client.recorded_values("SINUSOID", "*-1h", "*")

Cache Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pathlib import Path

   cache_config = CacheConfig(
       backend=CacheBackend.SQLITE,
       path=Path("~/.pipolars/cache").expanduser(),  # Cache directory
       max_size_mb=1024,  # Maximum cache size (1 GB)
       ttl_hours=24,      # Time-to-live (24 hours)
       compression=True,  # Enable compression
   )

Cache Backends
--------------

Memory Cache
~~~~~~~~~~~~

Fast but non-persistent. Best for short-running scripts:

.. code-block:: python

   cache_config = CacheConfig(
       backend=CacheBackend.MEMORY,
   )

Features:

- LRU (Least Recently Used) eviction
- Thread-safe
- Automatic TTL expiration
- No disk I/O

SQLite Cache
~~~~~~~~~~~~

Persistent SQLite database. Good general-purpose choice:

.. code-block:: python

   cache_config = CacheConfig(
       backend=CacheBackend.SQLITE,
       path=Path("~/.pipolars/cache").expanduser(),
       max_size_mb=2048,
       ttl_hours=48,
   )

Features:

- Persistent across restarts
- Automatic size management
- TTL-based expiration
- Compressed storage
- Thread-safe

Arrow Cache
~~~~~~~~~~~

Arrow IPC files. Optimal for Polars integration:

.. code-block:: python

   cache_config = CacheConfig(
       backend=CacheBackend.ARROW,
       path=Path("/data/pipolars_cache"),
       max_size_mb=4096,
   )

Features:

- Native Polars format
- Zero-copy reads
- Fast serialization
- Best for large DataFrames

Cache Management
----------------

Checking Cache Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient(config=config) as client:
       # Make some queries...
       df = client.recorded_values("SINUSOID", "*-1d", "*")

       # Check cache stats
       stats = client.cache_stats()
       print(stats)
       # {
       #     'type': 'sqlite',
       #     'items': 42,
       #     'size_mb': 125.5,
       #     'max_size_mb': 1024,
       #     'hits': 156,
       #     'misses': 42,
       #     'hit_rate': 0.788
       # }

Clearing the Cache
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with PIClient(config=config) as client:
       # Clear all cached data
       client.clear_cache()

Disabling Cache for Specific Queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a client without caching:

.. code-block:: python

   # Caching disabled
   client = PIClient("my-pi-server", enable_cache=False)

Cache Key Generation
--------------------

Cache keys are generated from query parameters:

- Tag name
- Start time
- End time
- Query type (recorded, interpolated, etc.)
- Additional parameters (interval, summary types, etc.)

.. code-block:: python

   from pipolars.cache.storage import CacheBackendBase

   # Generate cache key manually
   key = CacheBackendBase.generate_key(
       tag="SINUSOID",
       start="*-1d",
       end="*",
       query_type="recorded"
   )
   print(key)  # e.g., "a7b3c9d1e5f2..."

Environment Variables
---------------------

Configure caching via environment variables:

.. code-block:: bash

   # Windows Command Prompt
   set PIPOLARS_CACHE_BACKEND=sqlite
   set PIPOLARS_CACHE_PATH=C:\Users\me\.pipolars\cache
   set PIPOLARS_CACHE_MAX_SIZE_MB=2048
   set PIPOLARS_CACHE_TTL_HOURS=48

   # PowerShell
   $env:PIPOLARS_CACHE_BACKEND = "sqlite"
   $env:PIPOLARS_CACHE_TTL_HOURS = "48"

Best Practices
--------------

1. **Choose the right backend**:

   - Development: Use ``memory`` or ``none``
   - Production scripts: Use ``sqlite``
   - Data pipelines: Use ``arrow``

2. **Set appropriate TTL**:

   - Real-time data: Short TTL (1-4 hours)
   - Historical analysis: Longer TTL (24-168 hours)
   - Static data: Very long TTL or manual invalidation

3. **Size the cache appropriately**:

   .. code-block:: python

      # Estimate cache size needed
      # Typical PI data: ~100 bytes per value
      # 1 million values ≈ 100 MB
      cache_config = CacheConfig(
          backend=CacheBackend.SQLITE,
          max_size_mb=1024,  # 1 GB for ~10M cached values
      )

4. **Consider data freshness**:

   .. code-block:: python

      # For real-time dashboards, disable caching
      client = PIClient(config, enable_cache=False)

      # Or use short TTL
      cache_config = CacheConfig(
          backend=CacheBackend.MEMORY,
          ttl_hours=1,
      )

5. **Use consistent time expressions**:

   .. code-block:: python

      # These cache as different queries (different keys)
      client.recorded_values("TAG", "*-1h", "*")  # Time changes each call
      client.recorded_values("TAG", "*-1h", "*")  # Different cache key!

      # For caching to work, use absolute times for historical data
      client.recorded_values("TAG", "2024-01-01", "2024-01-02")
      client.recorded_values("TAG", "2024-01-01", "2024-01-02")  # Cache hit!

Advanced: Custom Cache Backend
------------------------------

Implement custom cache backends by extending ``CacheBackendBase``:

.. code-block:: python

   from pipolars.cache.storage import CacheBackendBase
   import polars as pl

   class RedisCache(CacheBackendBase):
       def __init__(self, redis_url: str):
           self.redis = redis.from_url(redis_url)

       def get(self, key: str) -> pl.DataFrame | None:
           data = self.redis.get(key)
           if data:
               return pl.read_ipc(data)
           return None

       def set(self, key: str, data: pl.DataFrame, ttl=None):
           buffer = data.write_ipc(None)
           self.redis.set(key, buffer, ex=ttl.total_seconds() if ttl else None)

       def delete(self, key: str) -> bool:
           return self.redis.delete(key) > 0

       def exists(self, key: str) -> bool:
           return self.redis.exists(key) > 0

       def clear(self) -> None:
           self.redis.flushdb()

       def get_stats(self) -> dict:
           info = self.redis.info()
           return {"type": "redis", "keys": info["db0"]["keys"]}

Troubleshooting
---------------

Cache Not Working
~~~~~~~~~~~~~~~~~

1. Verify caching is enabled:

   .. code-block:: python

      print(client.config.cache.backend)  # Should not be CacheBackend.NONE

2. Check cache directory permissions:

   .. code-block:: python

      print(client.config.cache.path)
      # Ensure this directory is writable

3. Verify TTL hasn't expired:

   .. code-block:: python

      stats = client.cache_stats()
      print(f"Items: {stats.get('items', 0)}")

Cache Too Large
~~~~~~~~~~~~~~~

.. code-block:: python

   # Reduce max size
   cache_config = CacheConfig(
       backend=CacheBackend.SQLITE,
       max_size_mb=512,  # Smaller limit
   )

   # Or clear the cache
   client.clear_cache()

Next Steps
----------

- :doc:`configuration` - Full configuration reference
- :doc:`advanced` - Advanced usage patterns
- :doc:`../api/cache` - Cache API reference
