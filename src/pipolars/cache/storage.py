"""Cache storage backends for PI data.

This module provides different storage backends for caching
PI data locally, including in-memory, SQLite, and Arrow file-based caches.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc

from pipolars.core.config import CacheBackend, CacheConfig
from pipolars.core.exceptions import PICacheError

logger = logging.getLogger(__name__)


class CacheBackendBase(ABC):
    """Abstract base class for cache backends.

    Cache backends are responsible for storing and retrieving
    Polars DataFrames with associated metadata.
    """

    @abstractmethod
    def get(self, key: str) -> pl.DataFrame | None:
        """Retrieve data from the cache.

        Args:
            key: Cache key

        Returns:
            Cached DataFrame or None if not found
        """
        pass

    @abstractmethod
    def set(
        self,
        key: str,
        data: pl.DataFrame,
        ttl: timedelta | None = None,
    ) -> None:
        """Store data in the cache.

        Args:
            key: Cache key
            data: DataFrame to cache
            ttl: Optional time-to-live
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete data from the cache.

        Args:
            key: Cache key

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: Cache key

        Returns:
            True if exists
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all cached data."""
        pass

    @abstractmethod
    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        pass

    @staticmethod
    def generate_key(
        tag: str,
        start: datetime | str,
        end: datetime | str,
        query_type: str = "recorded",
        **kwargs: Any,
    ) -> str:
        """Generate a cache key from query parameters.

        Args:
            tag: Tag name
            start: Start time
            end: End time
            query_type: Type of query
            **kwargs: Additional parameters

        Returns:
            Cache key string
        """
        key_parts = [
            tag,
            str(start),
            str(end),
            query_type,
        ]

        if kwargs:
            key_parts.append(json.dumps(kwargs, sort_keys=True))

        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]


class MemoryCache(CacheBackendBase):
    """In-memory cache backend using an LRU cache.

    This cache is fast but data is lost when the process ends.
    Uses an LRU (Least Recently Used) eviction policy.
    """

    def __init__(self, max_items: int = 1000) -> None:
        """Initialize the memory cache.

        Args:
            max_items: Maximum number of items to cache
        """
        self._cache: OrderedDict[str, tuple[pl.DataFrame, datetime | None]] = (
            OrderedDict()
        )
        self._max_items = max_items
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> pl.DataFrame | None:
        """Retrieve data from the cache."""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            data, expires_at = self._cache[key]

            # Check TTL
            if expires_at and datetime.now() > expires_at:
                del self._cache[key]
                self._misses += 1
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            self._hits += 1
            return data

    def set(
        self,
        key: str,
        data: pl.DataFrame,
        ttl: timedelta | None = None,
    ) -> None:
        """Store data in the cache."""
        with self._lock:
            expires_at = datetime.now() + ttl if ttl else None

            # Evict oldest items if at capacity
            while len(self._cache) >= self._max_items:
                self._cache.popitem(last=False)

            self._cache[key] = (data, expires_at)
            self._cache.move_to_end(key)

    def delete(self, key: str) -> bool:
        """Delete data from the cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        with self._lock:
            if key not in self._cache:
                return False

            _, expires_at = self._cache[key]
            if expires_at and datetime.now() > expires_at:
                del self._cache[key]
                return False

            return True

    def clear(self) -> None:
        """Clear all cached data."""
        with self._lock:
            self._cache.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0

            return {
                "type": "memory",
                "items": len(self._cache),
                "max_items": self._max_items,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
            }


class SQLiteCache(CacheBackendBase):
    """SQLite-based cache backend.

    Provides persistent caching using SQLite database with
    DataFrame serialization via Apache Arrow IPC format.
    """

    def __init__(
        self,
        path: Path | str,
        max_size_mb: int = 1024,
    ) -> None:
        """Initialize the SQLite cache.

        Args:
            path: Path to the cache directory
            max_size_mb: Maximum cache size in MB
        """
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._db_path = self._path / "cache.db"
        self._max_size_mb = max_size_mb
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    data BLOB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    size_bytes INTEGER
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires_at ON cache(expires_at)
            """)
            conn.commit()

    def _serialize_df(self, df: pl.DataFrame) -> bytes:
        """Serialize a DataFrame to bytes."""
        arrow_table = df.to_arrow()
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)
        return sink.getvalue().to_pybytes()

    def _deserialize_df(self, data: bytes) -> pl.DataFrame:
        """Deserialize bytes to a DataFrame."""
        reader = ipc.open_stream(data)
        arrow_table = reader.read_all()
        return pl.from_arrow(arrow_table)

    def get(self, key: str) -> pl.DataFrame | None:
        """Retrieve data from the cache."""
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    """
                    SELECT data, expires_at FROM cache WHERE key = ?
                    """,
                    (key,),
                )
                row = cursor.fetchone()

                if row is None:
                    self._misses += 1
                    return None

                data, expires_at = row

                # Check TTL
                if expires_at:
                    expires_dt = datetime.fromisoformat(expires_at)
                    if datetime.now() > expires_dt:
                        conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                        conn.commit()
                        self._misses += 1
                        return None

                self._hits += 1
                return self._deserialize_df(data)

    def set(
        self,
        key: str,
        data: pl.DataFrame,
        ttl: timedelta | None = None,
    ) -> None:
        """Store data in the cache."""
        with self._lock:
            serialized = self._serialize_df(data)
            size_bytes = len(serialized)

            # Check if we need to evict
            self._maybe_evict(size_bytes)

            expires_at = (
                (datetime.now() + ttl).isoformat() if ttl else None
            )

            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO cache (key, data, expires_at, size_bytes)
                    VALUES (?, ?, ?, ?)
                    """,
                    (key, serialized, expires_at, size_bytes),
                )
                conn.commit()

    def _maybe_evict(self, new_size: int) -> None:
        """Evict old entries if cache is too large."""
        max_bytes = self._max_size_mb * 1024 * 1024

        with sqlite3.connect(self._db_path) as conn:
            # Get current size
            cursor = conn.execute("SELECT SUM(size_bytes) FROM cache")
            current_size = cursor.fetchone()[0] or 0

            if current_size + new_size > max_bytes:
                # Delete expired entries first
                conn.execute(
                    "DELETE FROM cache WHERE expires_at < ?",
                    (datetime.now().isoformat(),),
                )

                # If still too large, delete oldest entries
                cursor = conn.execute("SELECT SUM(size_bytes) FROM cache")
                current_size = cursor.fetchone()[0] or 0

                while current_size + new_size > max_bytes:
                    conn.execute("""
                        DELETE FROM cache WHERE key IN (
                            SELECT key FROM cache ORDER BY created_at LIMIT 10
                        )
                    """)
                    cursor = conn.execute("SELECT SUM(size_bytes) FROM cache")
                    new_current = cursor.fetchone()[0] or 0
                    if new_current >= current_size:
                        break
                    current_size = new_current

                conn.commit()

    def delete(self, key: str) -> bool:
        """Delete data from the cache."""
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    "DELETE FROM cache WHERE key = ?", (key,)
                )
                conn.commit()
                return cursor.rowcount > 0

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    """
                    SELECT 1 FROM cache
                    WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)
                    """,
                    (key, datetime.now().isoformat()),
                )
                return cursor.fetchone() is not None

    def clear(self) -> None:
        """Clear all cached data."""
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute("DELETE FROM cache")
                conn.commit()

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*), SUM(size_bytes) FROM cache"
                )
                count, total_bytes = cursor.fetchone()

                total = self._hits + self._misses
                hit_rate = self._hits / total if total > 0 else 0.0

                return {
                    "type": "sqlite",
                    "items": count or 0,
                    "size_bytes": total_bytes or 0,
                    "size_mb": (total_bytes or 0) / (1024 * 1024),
                    "max_size_mb": self._max_size_mb,
                    "hits": self._hits,
                    "misses": self._misses,
                    "hit_rate": hit_rate,
                }


class ArrowCache(CacheBackendBase):
    """Arrow IPC file-based cache backend.

    Stores each cached DataFrame as a separate Arrow IPC file
    for optimal I/O performance with Polars.
    """

    def __init__(
        self,
        path: Path | str,
        max_size_mb: int = 1024,
    ) -> None:
        """Initialize the Arrow cache.

        Args:
            path: Path to the cache directory
            max_size_mb: Maximum cache size in MB
        """
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._data_path = self._path / "data"
        self._data_path.mkdir(exist_ok=True)
        self._meta_path = self._path / "metadata.json"
        self._max_size_mb = max_size_mb
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

        self._load_metadata()

    def _load_metadata(self) -> None:
        """Load cache metadata from disk."""
        if self._meta_path.exists():
            with open(self._meta_path) as f:
                self._metadata = json.load(f)
        else:
            self._metadata = {"entries": {}}

    def _save_metadata(self) -> None:
        """Save cache metadata to disk."""
        with open(self._meta_path, "w") as f:
            json.dump(self._metadata, f)

    def _get_file_path(self, key: str) -> Path:
        """Get the file path for a cache key."""
        return self._data_path / f"{key}.arrow"

    def get(self, key: str) -> pl.DataFrame | None:
        """Retrieve data from the cache."""
        with self._lock:
            if key not in self._metadata["entries"]:
                self._misses += 1
                return None

            entry = self._metadata["entries"][key]

            # Check TTL
            if entry.get("expires_at"):
                expires_dt = datetime.fromisoformat(entry["expires_at"])
                if datetime.now() > expires_dt:
                    self._delete_entry(key)
                    self._misses += 1
                    return None

            file_path = self._get_file_path(key)
            if not file_path.exists():
                del self._metadata["entries"][key]
                self._save_metadata()
                self._misses += 1
                return None

            self._hits += 1
            return pl.read_ipc(file_path)

    def set(
        self,
        key: str,
        data: pl.DataFrame,
        ttl: timedelta | None = None,
    ) -> None:
        """Store data in the cache."""
        with self._lock:
            file_path = self._get_file_path(key)

            # Write data
            data.write_ipc(file_path)

            # Update metadata
            self._metadata["entries"][key] = {
                "created_at": datetime.now().isoformat(),
                "expires_at": (datetime.now() + ttl).isoformat() if ttl else None,
                "size_bytes": file_path.stat().st_size,
            }
            self._save_metadata()

            # Maybe evict
            self._maybe_evict()

    def _delete_entry(self, key: str) -> None:
        """Delete a cache entry."""
        file_path = self._get_file_path(key)
        if file_path.exists():
            file_path.unlink()
        if key in self._metadata["entries"]:
            del self._metadata["entries"][key]
            self._save_metadata()

    def _maybe_evict(self) -> None:
        """Evict entries if cache is too large."""
        max_bytes = self._max_size_mb * 1024 * 1024

        # Calculate current size
        total_size = sum(
            e.get("size_bytes", 0)
            for e in self._metadata["entries"].values()
        )

        if total_size <= max_bytes:
            return

        # Delete expired first
        now = datetime.now()
        expired = [
            k for k, v in self._metadata["entries"].items()
            if v.get("expires_at") and datetime.fromisoformat(v["expires_at"]) < now
        ]
        for key in expired:
            self._delete_entry(key)

        # Delete oldest if still too large
        entries = sorted(
            self._metadata["entries"].items(),
            key=lambda x: x[1].get("created_at", ""),
        )

        for key, _ in entries:
            total_size = sum(
                e.get("size_bytes", 0)
                for e in self._metadata["entries"].values()
            )
            if total_size <= max_bytes:
                break
            self._delete_entry(key)

    def delete(self, key: str) -> bool:
        """Delete data from the cache."""
        with self._lock:
            if key in self._metadata["entries"]:
                self._delete_entry(key)
                return True
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        with self._lock:
            if key not in self._metadata["entries"]:
                return False

            entry = self._metadata["entries"][key]
            if entry.get("expires_at"):
                expires_dt = datetime.fromisoformat(entry["expires_at"])
                if datetime.now() > expires_dt:
                    return False

            return self._get_file_path(key).exists()

    def clear(self) -> None:
        """Clear all cached data."""
        with self._lock:
            for key in list(self._metadata["entries"].keys()):
                self._delete_entry(key)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_bytes = sum(
                e.get("size_bytes", 0)
                for e in self._metadata["entries"].values()
            )

            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0

            return {
                "type": "arrow",
                "items": len(self._metadata["entries"]),
                "size_bytes": total_bytes,
                "size_mb": total_bytes / (1024 * 1024),
                "max_size_mb": self._max_size_mb,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
            }


def get_cache_backend(config: CacheConfig) -> CacheBackendBase | None:
    """Factory function to create a cache backend from configuration.

    Args:
        config: Cache configuration

    Returns:
        Cache backend instance or None if caching is disabled
    """
    if config.backend == CacheBackend.NONE:
        return None
    elif config.backend == CacheBackend.MEMORY:
        return MemoryCache()
    elif config.backend == CacheBackend.SQLITE:
        return SQLiteCache(config.path, config.max_size_mb)
    elif config.backend == CacheBackend.ARROW:
        return ArrowCache(config.path, config.max_size_mb)
    else:
        raise PICacheError(f"Unknown cache backend: {config.backend}")
