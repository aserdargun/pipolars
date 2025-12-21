Cache
=====

.. module:: pipolars.cache

This module provides caching backends and strategies for PIPolars.

Storage Module
--------------

.. module:: pipolars.cache.storage

Cache Backends
~~~~~~~~~~~~~~

CacheBackendBase
^^^^^^^^^^^^^^^^

.. autoclass:: pipolars.cache.storage.CacheBackendBase
   :members:
   :undoc-members:
   :show-inheritance:

   Abstract base class for cache backends.

   All cache backends implement these methods:

   - ``get(key)`` - Retrieve cached data
   - ``set(key, data, ttl)`` - Store data in cache
   - ``delete(key)`` - Delete cached data
   - ``exists(key)`` - Check if key exists
   - ``clear()`` - Clear all cached data
   - ``get_stats()`` - Get cache statistics

MemoryCache
^^^^^^^^^^^

.. autoclass:: pipolars.cache.storage.MemoryCache
   :members:
   :undoc-members:
   :show-inheritance:

   In-memory LRU cache backend.

   Features:

   - Fast access
   - Thread-safe
   - LRU eviction
   - Automatic TTL expiration
   - Data lost on process exit

Usage:

.. code-block:: python

   from pipolars.cache.storage import MemoryCache

   cache = MemoryCache(max_items=1000)

   # Store data
   cache.set("key", df, ttl=timedelta(hours=1))

   # Retrieve data
   df = cache.get("key")

   # Check stats
   print(cache.get_stats())

SQLiteCache
^^^^^^^^^^^

.. autoclass:: pipolars.cache.storage.SQLiteCache
   :members:
   :undoc-members:
   :show-inheritance:

   SQLite-based persistent cache backend.

   Features:

   - Persistent storage
   - Automatic size management
   - TTL-based expiration
   - Compressed storage (Arrow IPC format)
   - Thread-safe

Usage:

.. code-block:: python

   from pipolars.cache.storage import SQLiteCache
   from pathlib import Path

   cache = SQLiteCache(
       path=Path("~/.pipolars/cache").expanduser(),
       max_size_mb=1024
   )

   # Store data
   cache.set("key", df, ttl=timedelta(hours=24))

   # Retrieve data
   df = cache.get("key")

ArrowCache
^^^^^^^^^^

.. autoclass:: pipolars.cache.storage.ArrowCache
   :members:
   :undoc-members:
   :show-inheritance:

   Arrow IPC file-based cache backend.

   Features:

   - Native Polars format
   - Zero-copy reads
   - Fast serialization
   - Optimal for large DataFrames

Usage:

.. code-block:: python

   from pipolars.cache.storage import ArrowCache
   from pathlib import Path

   cache = ArrowCache(
       path=Path("/data/pipolars_cache"),
       max_size_mb=4096
   )

Factory Function
~~~~~~~~~~~~~~~~

.. autofunction:: pipolars.cache.storage.get_cache_backend

   Factory function to create a cache backend from configuration.

   Args:
       config: CacheConfig instance

   Returns:
       Cache backend instance or None if caching is disabled

Usage:

.. code-block:: python

   from pipolars.cache.storage import get_cache_backend
   from pipolars.core.config import CacheConfig, CacheBackend

   config = CacheConfig(
       backend=CacheBackend.SQLITE,
       max_size_mb=1024
   )

   cache = get_cache_backend(config)

Strategies Module
-----------------

.. module:: pipolars.cache.strategies

TTLStrategy
~~~~~~~~~~~

.. autoclass:: pipolars.cache.strategies.TTLStrategy
   :members:
   :undoc-members:
   :show-inheritance:

   Time-to-live caching strategy.

Cache Key Generation
--------------------

Cache keys are generated from query parameters using SHA-256 hashing:

.. code-block:: python

   from pipolars.cache.storage import CacheBackendBase

   key = CacheBackendBase.generate_key(
       tag="SINUSOID",
       start="*-1d",
       end="*",
       query_type="recorded",
       interval="1h"  # Optional additional parameters
   )

   print(key)  # e.g., "a7b3c9d1e5f2a8b4..."

The key includes:

- Tag name
- Start time (string representation)
- End time (string representation)
- Query type
- Additional parameters (JSON-serialized)

Cache Statistics
----------------

All backends provide statistics via ``get_stats()``:

.. code-block:: python

   stats = cache.get_stats()

   # Memory cache stats
   {
       "type": "memory",
       "items": 42,
       "max_items": 1000,
       "hits": 156,
       "misses": 42,
       "hit_rate": 0.788
   }

   # SQLite cache stats
   {
       "type": "sqlite",
       "items": 42,
       "size_bytes": 131457024,
       "size_mb": 125.5,
       "max_size_mb": 1024,
       "hits": 156,
       "misses": 42,
       "hit_rate": 0.788
   }

   # Arrow cache stats
   {
       "type": "arrow",
       "items": 42,
       "size_bytes": 131457024,
       "size_mb": 125.5,
       "max_size_mb": 4096,
       "hits": 156,
       "misses": 42,
       "hit_rate": 0.788
   }

Integration with PIClient
-------------------------

Caching is configured via ``PIConfig``:

.. code-block:: python

   from pipolars import PIClient, PIConfig
   from pipolars.core.config import CacheConfig, CacheBackend, PIServerConfig

   config = PIConfig(
       server=PIServerConfig(host="my-pi-server"),
       cache=CacheConfig(
           backend=CacheBackend.SQLITE,
           ttl_hours=24,
       ),
   )

   with PIClient(config=config) as client:
       # First query - cache miss
       df = client.recorded_values("SINUSOID", "*-1h", "*")

       # Second query - cache hit
       df = client.recorded_values("SINUSOID", "*-1h", "*")

       # Check stats
       print(client.cache_stats())

       # Clear cache
       client.clear_cache()

Custom Cache Backend
--------------------

Implement custom backends by extending ``CacheBackendBase``:

.. code-block:: python

   from pipolars.cache.storage import CacheBackendBase
   import polars as pl
   from datetime import timedelta

   class RedisCache(CacheBackendBase):
       def __init__(self, redis_url: str):
           import redis
           self.redis = redis.from_url(redis_url)
           self._hits = 0
           self._misses = 0

       def get(self, key: str) -> pl.DataFrame | None:
           data = self.redis.get(key)
           if data:
               self._hits += 1
               return pl.read_ipc(data)
           self._misses += 1
           return None

       def set(
           self,
           key: str,
           data: pl.DataFrame,
           ttl: timedelta | None = None
       ) -> None:
           buffer = data.write_ipc(None)
           ex = int(ttl.total_seconds()) if ttl else None
           self.redis.set(key, buffer, ex=ex)

       def delete(self, key: str) -> bool:
           return self.redis.delete(key) > 0

       def exists(self, key: str) -> bool:
           return self.redis.exists(key) > 0

       def clear(self) -> None:
           self.redis.flushdb()

       def get_stats(self) -> dict:
           info = self.redis.info()
           total = self._hits + self._misses
           return {
               "type": "redis",
               "keys": info.get("db0", {}).get("keys", 0),
               "hits": self._hits,
               "misses": self._misses,
               "hit_rate": self._hits / total if total > 0 else 0,
           }

See Also
--------

- :doc:`../user_guide/caching` - Caching guide
- :doc:`config` - CacheConfig options
