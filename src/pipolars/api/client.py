"""Main PIPolars client.

This module provides the primary interface for extracting PI System
data as Polars DataFrames.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence

import polars as pl

from pipolars.api.query import PIQuery
from pipolars.cache.storage import CacheBackendBase, get_cache_backend
from pipolars.cache.strategies import TTLStrategy
from pipolars.connection.af_database import AFDatabaseConnection
from pipolars.connection.server import PIServerConnection
from pipolars.core.config import CacheConfig, PIConfig, PIServerConfig
from pipolars.core.types import PITimestamp, SummaryType, TimeRange
from pipolars.extraction.attributes import AFAttributeExtractor
from pipolars.extraction.bulk import BulkExtractor
from pipolars.extraction.elements import AFElementExtractor
from pipolars.extraction.events import EventFrameExtractor
from pipolars.extraction.points import PIPointExtractor
from pipolars.transform.converters import PIToPolarsConverter

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PIClient:
    """Main client for extracting PI System data as Polars DataFrames.

    This is the primary interface for the PIPolars library. It provides
    a high-level API for querying PI Points, AF Attributes, and Event
    Frames, returning data as Polars DataFrames.

    Example:
        >>> # Connect to PI Server
        >>> with PIClient("my-pi-server") as client:
        ...     # Get recorded values as DataFrame
        ...     df = client.recorded_values("SINUSOID", start="*-1d", end="*")
        ...     print(df)
        ...
        ...     # Get multiple tags at once
        ...     df = client.recorded_values(
        ...         ["TAG1", "TAG2", "TAG3"],
        ...         start="*-1h",
        ...         end="*"
        ...     )
        ...
        ...     # Get summary statistics
        ...     summaries = client.summaries(
        ...         "SINUSOID",
        ...         start="*-7d",
        ...         end="*",
        ...         summary_types=[SummaryType.AVERAGE, SummaryType.MAXIMUM]
        ...     )

        >>> # Using configuration
        >>> config = PIConfig(
        ...     server=PIServerConfig(host="my-pi-server"),
        ...     cache=CacheConfig(backend=CacheBackend.SQLITE),
        ... )
        >>> client = PIClient(config=config)
    """

    def __init__(
        self,
        server: str | PIServerConfig | None = None,
        config: PIConfig | None = None,
        enable_cache: bool = True,
    ) -> None:
        """Initialize the PI client.

        Args:
            server: PI Server hostname or configuration
            config: Full PIPolars configuration
            enable_cache: Whether to enable caching
        """
        # Build configuration
        if config:
            self._config = config
        elif server:
            if isinstance(server, str):
                server_config = PIServerConfig(host=server)
            else:
                server_config = server
            self._config = PIConfig(server=server_config)
        else:
            self._config = PIConfig()

        # Initialize connections
        self._pi_connection: PIServerConnection | None = None
        self._af_connection: AFDatabaseConnection | None = None

        # Initialize cache
        self._cache: CacheBackendBase | None = None
        self._cache_strategy: TTLStrategy | None = None
        if enable_cache and self._config.cache.backend.value != "none":
            self._cache = get_cache_backend(self._config.cache)
            if self._cache:
                self._cache_strategy = TTLStrategy(
                    self._cache,
                    ttl=self._config.cache.ttl,
                )

        # Initialize converters
        self._converter = PIToPolarsConverter(self._config.polars)

        # Lazy-loaded extractors
        self._point_extractor: PIPointExtractor | None = None
        self._bulk_extractor: BulkExtractor | None = None
        self._attribute_extractor: AFAttributeExtractor | None = None
        self._element_extractor: AFElementExtractor | None = None
        self._event_extractor: EventFrameExtractor | None = None

    @property
    def config(self) -> PIConfig:
        """Get the client configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Check if connected to PI Server."""
        return self._pi_connection is not None and self._pi_connection.is_connected

    @property
    def server_name(self) -> str | None:
        """Get the connected server name."""
        if self._pi_connection:
            return self._pi_connection.name
        return None

    def connect(self) -> PIClient:
        """Connect to the PI Server.

        Returns:
            Self for method chaining
        """
        if self._pi_connection is None:
            self._pi_connection = PIServerConnection(self._config.server)

        self._pi_connection.connect()
        logger.info(f"Connected to PI Server: {self._pi_connection.name}")

        return self

    def disconnect(self) -> None:
        """Disconnect from the PI Server."""
        if self._pi_connection:
            self._pi_connection.disconnect()

        if self._af_connection:
            self._af_connection.disconnect()

    def __enter__(self) -> PIClient:
        """Context manager entry."""
        return self.connect()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()

    def _ensure_connected(self) -> None:
        """Ensure we're connected to the server."""
        if not self.is_connected:
            self.connect()

    def _get_point_extractor(self) -> PIPointExtractor:
        """Get or create the point extractor."""
        self._ensure_connected()
        if self._point_extractor is None:
            self._point_extractor = PIPointExtractor(self._pi_connection)  # type: ignore
        return self._point_extractor

    def _get_bulk_extractor(self) -> BulkExtractor:
        """Get or create the bulk extractor."""
        self._ensure_connected()
        if self._bulk_extractor is None:
            self._bulk_extractor = BulkExtractor(
                self._pi_connection,  # type: ignore
                max_parallel=self._config.query.parallel_requests,
            )
        return self._bulk_extractor

    # -------------------------------------------------------------------------
    # Query Builder
    # -------------------------------------------------------------------------

    def query(self, tags: str | list[str]) -> PIQuery:
        """Create a query builder for the specified tags.

        Args:
            tags: Single tag or list of tags to query

        Returns:
            PIQuery builder for method chaining

        Example:
            >>> df = client.query("SINUSOID") \\
            ...     .time_range("*-1d", "*") \\
            ...     .recorded() \\
            ...     .to_dataframe()
        """
        self._ensure_connected()
        return PIQuery(
            client=self,
            tags=tags if isinstance(tags, list) else [tags],
        )

    # -------------------------------------------------------------------------
    # Snapshot Values
    # -------------------------------------------------------------------------

    def snapshot(self, tag: str) -> pl.DataFrame:
        """Get the current snapshot value for a tag.

        Args:
            tag: PI Point name

        Returns:
            DataFrame with single row containing current value
        """
        extractor = self._get_point_extractor()
        value = extractor.snapshot(tag)
        return self._converter.values_to_dataframe([value])

    def snapshots(self, tags: list[str]) -> pl.DataFrame:
        """Get current snapshot values for multiple tags.

        Args:
            tags: List of PI Point names

        Returns:
            DataFrame with current values for all tags
        """
        extractor = self._get_bulk_extractor()
        values = extractor.snapshots(tags)
        return self._converter.multi_tag_to_dataframe(
            {tag: [value] for tag, value in values.items()}
        )

    # -------------------------------------------------------------------------
    # Recorded Values
    # -------------------------------------------------------------------------

    def recorded_values(
        self,
        tags: str | list[str],
        start: PITimestamp,
        end: PITimestamp,
        max_count: int = 0,
        include_quality: bool = False,
        pivot: bool = False,
    ) -> pl.DataFrame:
        """Get recorded values for one or more tags.

        Args:
            tags: Single tag or list of tags
            start: Start time
            end: End time
            max_count: Maximum values per tag (0 = no limit)
            include_quality: Include quality column
            pivot: Pivot to wide format (tags as columns)

        Returns:
            DataFrame with recorded values
        """
        tags_list = [tags] if isinstance(tags, str) else tags

        if len(tags_list) == 1:
            # Single tag
            point_extractor = self._get_point_extractor()
            values = point_extractor.recorded_values(tags_list[0], start, end)
            return self._converter.values_to_dataframe(values, include_quality)
        else:
            # Multiple tags
            bulk_extractor = self._get_bulk_extractor()
            tag_values = bulk_extractor.recorded_values(tags_list, start, end, max_count)
            return self._converter.multi_tag_to_dataframe(
                tag_values, include_quality, pivot
            )

    # -------------------------------------------------------------------------
    # Interpolated Values
    # -------------------------------------------------------------------------

    def interpolated_values(
        self,
        tags: str | list[str],
        start: PITimestamp,
        end: PITimestamp,
        interval: str = "1h",
        include_quality: bool = False,
        pivot: bool = False,
    ) -> pl.DataFrame:
        """Get interpolated values at regular intervals.

        Args:
            tags: Single tag or list of tags
            start: Start time
            end: End time
            interval: Time interval (e.g., "1h", "15m", "1d")
            include_quality: Include quality column
            pivot: Pivot to wide format

        Returns:
            DataFrame with interpolated values
        """
        tags_list = [tags] if isinstance(tags, str) else tags

        if len(tags_list) == 1:
            point_extractor = self._get_point_extractor()
            values = point_extractor.interpolated_values(
                tags_list[0], start, end, interval
            )
            return self._converter.values_to_dataframe(values, include_quality)
        else:
            bulk_extractor = self._get_bulk_extractor()
            tag_values = bulk_extractor.interpolated_values(
                tags_list, start, end, interval
            )
            return self._converter.multi_tag_to_dataframe(
                tag_values, include_quality, pivot
            )

    # -------------------------------------------------------------------------
    # Plot Values
    # -------------------------------------------------------------------------

    def plot_values(
        self,
        tag: str,
        start: PITimestamp,
        end: PITimestamp,
        intervals: int = 640,
        include_quality: bool = False,
    ) -> pl.DataFrame:
        """Get values optimized for plotting.

        Args:
            tag: PI Point name
            start: Start time
            end: End time
            intervals: Number of intervals
            include_quality: Include quality column

        Returns:
            DataFrame with plot-optimized values
        """
        extractor = self._get_point_extractor()
        values = extractor.plot_values(tag, start, end, intervals)
        return self._converter.values_to_dataframe(values, include_quality)

    # -------------------------------------------------------------------------
    # Summary Values
    # -------------------------------------------------------------------------

    def summary(
        self,
        tags: str | list[str],
        start: PITimestamp,
        end: PITimestamp,
        summary_types: SummaryType | list[SummaryType] = SummaryType.AVERAGE,
    ) -> pl.DataFrame:
        """Get summary statistics for one or more tags.

        Args:
            tags: Single tag or list of tags
            start: Start time
            end: End time
            summary_types: Summary type(s) to calculate

        Returns:
            DataFrame with summary statistics
        """
        tags_list = [tags] if isinstance(tags, str) else tags

        if len(tags_list) == 1:
            point_extractor = self._get_point_extractor()
            tag_summaries = point_extractor.summary(tags_list[0], start, end, summary_types)
            return self._converter.summaries_to_dataframe(
                {tags_list[0]: tag_summaries}
            )
        else:
            bulk_extractor = self._get_bulk_extractor()
            all_summaries = bulk_extractor.summaries(tags_list, start, end, summary_types)
            return self._converter.summaries_to_dataframe(all_summaries)

    def summaries(
        self,
        tag: str,
        start: PITimestamp,
        end: PITimestamp,
        interval: str,
        summary_types: SummaryType | list[SummaryType] = SummaryType.AVERAGE,
    ) -> pl.DataFrame:
        """Get summary statistics over multiple intervals.

        Args:
            tag: PI Point name
            start: Start time
            end: End time
            interval: Interval for each summary
            summary_types: Summary type(s) to calculate

        Returns:
            DataFrame with time-series summary statistics
        """
        extractor = self._get_point_extractor()
        intervals = extractor.summaries(tag, start, end, interval, summary_types)
        return self._converter.time_series_summaries_to_dataframe(
            {tag: intervals}
        )

    # -------------------------------------------------------------------------
    # Tag Search and Info
    # -------------------------------------------------------------------------

    def search_tags(
        self,
        pattern: str,
        max_results: int = 1000,
    ) -> list[str]:
        """Search for PI Points matching a pattern.

        Args:
            pattern: Search pattern (supports wildcards)
            max_results: Maximum results

        Returns:
            List of matching tag names
        """
        self._ensure_connected()
        points = self._pi_connection.search_points(pattern, max_results)  # type: ignore
        return [str(p.Name) for p in points]

    def tag_exists(self, tag: str) -> bool:
        """Check if a tag exists.

        Args:
            tag: PI Point name

        Returns:
            True if the tag exists
        """
        self._ensure_connected()
        return self._pi_connection.point_exists(tag)  # type: ignore

    def tag_info(self, tag: str) -> dict[str, Any]:
        """Get metadata for a tag.

        Args:
            tag: PI Point name

        Returns:
            Dictionary with tag metadata
        """
        extractor = self._get_point_extractor()
        config = extractor.get_point_config(tag)
        return {
            "name": config.name,
            "point_id": config.point_id,
            "point_type": config.point_type.value,
            "description": config.description,
            "engineering_units": config.engineering_units,
            "zero": config.zero,
            "span": config.span,
        }

    # -------------------------------------------------------------------------
    # Cache Management
    # -------------------------------------------------------------------------

    def cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        if self._cache:
            return self._cache.get_stats()
        return {"enabled": False}

    def clear_cache(self) -> None:
        """Clear all cached data."""
        if self._cache:
            self._cache.clear()

    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------

    def last(
        self,
        tags: str | list[str],
        hours: int = 0,
        days: int = 0,
        minutes: int = 0,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Get recorded values for the last N hours/days/minutes.

        Args:
            tags: Single tag or list of tags
            hours: Number of hours
            days: Number of days
            minutes: Number of minutes
            **kwargs: Additional arguments for recorded_values

        Returns:
            DataFrame with recorded values

        Example:
            >>> df = client.last("SINUSOID", hours=24)
            >>> df = client.last(["TAG1", "TAG2"], days=7)
        """
        time_range = TimeRange.last(hours=hours, days=days, minutes=minutes)
        return self.recorded_values(
            tags,
            start=time_range.start,
            end=time_range.end,
            **kwargs,
        )

    def today(
        self,
        tags: str | list[str],
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Get recorded values for today.

        Args:
            tags: Single tag or list of tags
            **kwargs: Additional arguments for recorded_values

        Returns:
            DataFrame with today's values
        """
        time_range = TimeRange.today()
        return self.recorded_values(
            tags,
            start=time_range.start,
            end=time_range.end,
            **kwargs,
        )
