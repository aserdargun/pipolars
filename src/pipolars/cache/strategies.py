"""Caching strategies for PI data.

This module provides different caching strategies for managing
how PI data is cached and retrieved.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable

import polars as pl

from pipolars.cache.storage import CacheBackendBase
from pipolars.core.types import PITimestamp, TimeRange

if TYPE_CHECKING:
    pass


class CacheStrategy(ABC):
    """Abstract base class for cache strategies.

    Cache strategies determine how data is cached and when
    cached data should be used vs. fetched from the server.
    """

    def __init__(self, backend: CacheBackendBase) -> None:
        """Initialize the strategy.

        Args:
            backend: Cache backend to use
        """
        self._backend = backend

    @property
    def backend(self) -> CacheBackendBase:
        """Get the cache backend."""
        return self._backend

    @abstractmethod
    def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable[[], pl.DataFrame],
    ) -> pl.DataFrame:
        """Get data from cache or fetch from source.

        Args:
            key: Cache key
            fetch_func: Function to fetch data if not cached

        Returns:
            DataFrame from cache or freshly fetched
        """
        pass

    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry.

        Args:
            key: Cache key

        Returns:
            True if invalidated
        """
        return self._backend.delete(key)

    def clear_all(self) -> None:
        """Clear all cached data."""
        self._backend.clear()


class TTLStrategy(CacheStrategy):
    """Time-to-live caching strategy.

    Cached data expires after a specified duration. Once expired,
    data is re-fetched from the source.
    """

    def __init__(
        self,
        backend: CacheBackendBase,
        ttl: timedelta = timedelta(hours=24),
    ) -> None:
        """Initialize the TTL strategy.

        Args:
            backend: Cache backend
            ttl: Time-to-live for cached data
        """
        super().__init__(backend)
        self._ttl = ttl

    @property
    def ttl(self) -> timedelta:
        """Get the TTL duration."""
        return self._ttl

    def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable[[], pl.DataFrame],
    ) -> pl.DataFrame:
        """Get data from cache or fetch with TTL."""
        cached = self._backend.get(key)

        if cached is not None:
            return cached

        data = fetch_func()
        self._backend.set(key, data, self._ttl)
        return data

    def set_with_ttl(
        self,
        key: str,
        data: pl.DataFrame,
        ttl: timedelta | None = None,
    ) -> None:
        """Set data with custom TTL.

        Args:
            key: Cache key
            data: Data to cache
            ttl: Custom TTL (uses default if None)
        """
        self._backend.set(key, data, ttl or self._ttl)


class SlidingWindowStrategy(CacheStrategy):
    """Sliding window caching strategy for time-series data.

    This strategy is optimized for time-series queries where
    the time range slides forward over time. It maintains
    overlapping cached data to minimize re-fetching.
    """

    def __init__(
        self,
        backend: CacheBackendBase,
        window_size: timedelta = timedelta(hours=24),
        overlap: timedelta = timedelta(hours=1),
    ) -> None:
        """Initialize the sliding window strategy.

        Args:
            backend: Cache backend
            window_size: Size of each cache window
            overlap: Overlap between windows
        """
        super().__init__(backend)
        self._window_size = window_size
        self._overlap = overlap

    def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable[[], pl.DataFrame],
    ) -> pl.DataFrame:
        """Get data using sliding window logic."""
        cached = self._backend.get(key)

        if cached is not None:
            return cached

        data = fetch_func()
        self._backend.set(key, data, self._window_size)
        return data

    def get_time_range_data(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        fetch_func: Callable[[datetime, datetime], pl.DataFrame],
        timestamp_col: str = "timestamp",
    ) -> pl.DataFrame:
        """Get data for a time range, using cached windows.

        Args:
            tag: Tag name
            start: Start time
            end: End time
            fetch_func: Function to fetch data for a time range
            timestamp_col: Name of the timestamp column

        Returns:
            Combined DataFrame for the full time range
        """
        # Calculate window boundaries
        windows = self._calculate_windows(start, end)
        all_data = []

        for window_start, window_end in windows:
            key = self._backend.generate_key(
                tag,
                window_start.isoformat(),
                window_end.isoformat(),
                "window",
            )

            cached = self._backend.get(key)

            if cached is not None:
                # Filter to requested range
                filtered = cached.filter(
                    (pl.col(timestamp_col) >= start)
                    & (pl.col(timestamp_col) <= end)
                )
                all_data.append(filtered)
            else:
                # Fetch and cache the full window
                data = fetch_func(window_start, window_end)
                self._backend.set(key, data, self._window_size)

                # Filter to requested range
                filtered = data.filter(
                    (pl.col(timestamp_col) >= start)
                    & (pl.col(timestamp_col) <= end)
                )
                all_data.append(filtered)

        if not all_data:
            return pl.DataFrame()

        # Combine and deduplicate
        combined = pl.concat(all_data)
        return combined.unique(subset=[timestamp_col]).sort(timestamp_col)

    def _calculate_windows(
        self,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        """Calculate cache window boundaries.

        Args:
            start: Query start time
            end: Query end time

        Returns:
            List of (window_start, window_end) tuples
        """
        windows = []
        current_start = self._align_to_window(start)

        while current_start < end:
            window_end = current_start + self._window_size
            windows.append((current_start, window_end))
            current_start = window_end - self._overlap

        return windows

    def _align_to_window(self, dt: datetime) -> datetime:
        """Align a datetime to a window boundary.

        Args:
            dt: Datetime to align

        Returns:
            Aligned datetime
        """
        # Round down to the nearest window boundary
        window_seconds = self._window_size.total_seconds()
        timestamp = dt.timestamp()
        aligned_timestamp = (timestamp // window_seconds) * window_seconds
        return datetime.fromtimestamp(aligned_timestamp, tz=dt.tzinfo)


class SmartCacheStrategy(CacheStrategy):
    """Smart caching strategy that adapts based on query patterns.

    This strategy analyzes query patterns and adjusts caching
    behavior for optimal performance.
    """

    def __init__(
        self,
        backend: CacheBackendBase,
        default_ttl: timedelta = timedelta(hours=24),
        hot_data_ttl: timedelta = timedelta(minutes=5),
        historical_ttl: timedelta = timedelta(days=30),
        hot_data_threshold: timedelta = timedelta(hours=1),
    ) -> None:
        """Initialize the smart cache strategy.

        Args:
            backend: Cache backend
            default_ttl: Default TTL for normal data
            hot_data_ttl: TTL for recent/hot data
            historical_ttl: TTL for historical data
            hot_data_threshold: Threshold for considering data "hot"
        """
        super().__init__(backend)
        self._default_ttl = default_ttl
        self._hot_data_ttl = hot_data_ttl
        self._historical_ttl = historical_ttl
        self._hot_data_threshold = hot_data_threshold
        self._query_history: list[dict[str, Any]] = []

    def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable[[], pl.DataFrame],
    ) -> pl.DataFrame:
        """Get data with smart TTL selection."""
        cached = self._backend.get(key)

        if cached is not None:
            return cached

        data = fetch_func()
        ttl = self._select_ttl(key)
        self._backend.set(key, data, ttl)
        return data

    def get_with_time_range(
        self,
        key: str,
        start: datetime,
        end: datetime,
        fetch_func: Callable[[], pl.DataFrame],
    ) -> pl.DataFrame:
        """Get data with time-range-aware TTL.

        Args:
            key: Cache key
            start: Query start time
            end: Query end time
            fetch_func: Function to fetch data

        Returns:
            DataFrame
        """
        cached = self._backend.get(key)

        if cached is not None:
            return cached

        data = fetch_func()
        ttl = self._select_ttl_for_range(start, end)
        self._backend.set(key, data, ttl)

        # Record query for pattern analysis
        self._record_query(key, start, end)

        return data

    def _select_ttl(self, key: str) -> timedelta:
        """Select appropriate TTL based on key patterns."""
        return self._default_ttl

    def _select_ttl_for_range(
        self,
        start: datetime,
        end: datetime,
    ) -> timedelta:
        """Select TTL based on time range.

        Recent data gets shorter TTL as it may still be updating.
        Historical data gets longer TTL as it's unlikely to change.
        """
        now = datetime.now(tz=start.tzinfo)

        # If end time is recent (hot data)
        if now - end < self._hot_data_threshold:
            return self._hot_data_ttl

        # If data is old (historical)
        if now - end > timedelta(days=7):
            return self._historical_ttl

        return self._default_ttl

    def _record_query(
        self,
        key: str,
        start: datetime,
        end: datetime,
    ) -> None:
        """Record a query for pattern analysis."""
        self._query_history.append({
            "key": key,
            "start": start,
            "end": end,
            "timestamp": datetime.now(),
        })

        # Keep only recent history
        if len(self._query_history) > 1000:
            self._query_history = self._query_history[-500:]

    def get_popular_patterns(self) -> list[dict[str, Any]]:
        """Analyze query history for popular patterns.

        Returns:
            List of popular query patterns
        """
        # Simple frequency analysis
        from collections import Counter

        key_counts = Counter(q["key"] for q in self._query_history)
        return [
            {"key": key, "count": count}
            for key, count in key_counts.most_common(10)
        ]

    def prefetch_popular(
        self,
        fetch_func: Callable[[str], pl.DataFrame],
    ) -> int:
        """Prefetch data for popular query patterns.

        Args:
            fetch_func: Function to fetch data by key

        Returns:
            Number of patterns prefetched
        """
        patterns = self.get_popular_patterns()
        prefetched = 0

        for pattern in patterns:
            key = pattern["key"]
            if not self._backend.exists(key):
                try:
                    data = fetch_func(key)
                    self._backend.set(key, data, self._default_ttl)
                    prefetched += 1
                except Exception:
                    pass

        return prefetched
