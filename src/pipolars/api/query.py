"""Fluent query builder for PI data.

This module provides a fluent interface for building PI data queries
with method chaining.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import polars as pl

from pipolars.core.types import (
    AFTime,
    BoundaryType,
    PITimestamp,
    SummaryType,
    TimeRange,
)

if TYPE_CHECKING:
    from pipolars.api.client import PIClient


class QueryType(Enum):
    """Types of PI data queries."""

    RECORDED = auto()
    INTERPOLATED = auto()
    PLOT = auto()
    SUMMARY = auto()
    SNAPSHOT = auto()


@dataclass
class QueryOptions:
    """Options for a PI query."""

    query_type: QueryType = QueryType.RECORDED
    start: PITimestamp | None = None
    end: PITimestamp | None = None
    interval: str | None = None
    max_count: int = 0
    include_quality: bool = False
    boundary_type: BoundaryType = BoundaryType.INSIDE
    filter_expression: str | None = None
    summary_types: list[SummaryType] = field(default_factory=list)
    plot_intervals: int = 640
    pivot: bool = False


class PIQuery:
    """Fluent query builder for PI data.

    This class provides a method-chaining interface for building
    PI data queries in a readable, declarative style.

    Example:
        >>> df = client.query("SINUSOID") \\
        ...     .time_range("*-1d", "*") \\
        ...     .recorded() \\
        ...     .with_quality() \\
        ...     .to_dataframe()

        >>> df = client.query(["TAG1", "TAG2"]) \\
        ...     .last(hours=24) \\
        ...     .interpolated(interval="15m") \\
        ...     .pivot() \\
        ...     .to_dataframe()

        >>> df = client.query("SINUSOID") \\
        ...     .time_range("*-7d", "*") \\
        ...     .summary(SummaryType.AVERAGE, SummaryType.MAXIMUM) \\
        ...     .to_dataframe()
    """

    def __init__(self, client: PIClient, tags: list[str]) -> None:
        """Initialize the query builder.

        Args:
            client: PIClient instance
            tags: List of tags to query
        """
        self._client = client
        self._tags = tags
        self._options = QueryOptions()

    # -------------------------------------------------------------------------
    # Time Range Methods
    # -------------------------------------------------------------------------

    def time_range(
        self,
        start: PITimestamp,
        end: PITimestamp,
    ) -> PIQuery:
        """Set the time range for the query.

        Args:
            start: Start time (datetime, string, or AFTime)
            end: End time

        Returns:
            Self for method chaining
        """
        self._options.start = start
        self._options.end = end
        return self

    def last(
        self,
        hours: int = 0,
        days: int = 0,
        minutes: int = 0,
        seconds: int = 0,
    ) -> PIQuery:
        """Set time range to the last N time units.

        Args:
            hours: Number of hours
            days: Number of days
            minutes: Number of minutes
            seconds: Number of seconds

        Returns:
            Self for method chaining
        """
        self._options.start = AFTime.ago(
            hours=hours, days=days, minutes=minutes, seconds=seconds
        )
        self._options.end = AFTime.now()
        return self

    def today(self) -> PIQuery:
        """Set time range to today.

        Returns:
            Self for method chaining
        """
        time_range = TimeRange.today()
        self._options.start = time_range.start
        self._options.end = time_range.end
        return self

    def yesterday(self) -> PIQuery:
        """Set time range to yesterday.

        Returns:
            Self for method chaining
        """
        self._options.start = AFTime.yesterday()
        self._options.end = AFTime.today()
        return self

    def this_week(self) -> PIQuery:
        """Set time range to this week.

        Returns:
            Self for method chaining
        """
        self._options.start = AFTime("*-7d")
        self._options.end = AFTime.now()
        return self

    def this_month(self) -> PIQuery:
        """Set time range to this month.

        Returns:
            Self for method chaining
        """
        self._options.start = AFTime("*-30d")
        self._options.end = AFTime.now()
        return self

    # -------------------------------------------------------------------------
    # Query Type Methods
    # -------------------------------------------------------------------------

    def recorded(self, max_count: int = 0) -> PIQuery:
        """Query for recorded values.

        Args:
            max_count: Maximum values to return (0 = no limit)

        Returns:
            Self for method chaining
        """
        self._options.query_type = QueryType.RECORDED
        self._options.max_count = max_count
        return self

    def interpolated(self, interval: str = "1h") -> PIQuery:
        """Query for interpolated values.

        Args:
            interval: Time interval (e.g., "1h", "15m", "1d")

        Returns:
            Self for method chaining
        """
        self._options.query_type = QueryType.INTERPOLATED
        self._options.interval = interval
        return self

    def plot(self, intervals: int = 640) -> PIQuery:
        """Query for plot-optimized values.

        Args:
            intervals: Number of intervals

        Returns:
            Self for method chaining
        """
        self._options.query_type = QueryType.PLOT
        self._options.plot_intervals = intervals
        return self

    def summary(self, *summary_types: SummaryType) -> PIQuery:
        """Query for summary statistics.

        Args:
            *summary_types: Summary types to calculate

        Returns:
            Self for method chaining
        """
        self._options.query_type = QueryType.SUMMARY
        self._options.summary_types = list(summary_types) if summary_types else [SummaryType.AVERAGE]
        return self

    def snapshot(self) -> PIQuery:
        """Query for current snapshot values.

        Returns:
            Self for method chaining
        """
        self._options.query_type = QueryType.SNAPSHOT
        return self

    # -------------------------------------------------------------------------
    # Option Methods
    # -------------------------------------------------------------------------

    def with_quality(self) -> PIQuery:
        """Include quality information in results.

        Returns:
            Self for method chaining
        """
        self._options.include_quality = True
        return self

    def without_quality(self) -> PIQuery:
        """Exclude quality information from results.

        Returns:
            Self for method chaining
        """
        self._options.include_quality = False
        return self

    def boundary(self, boundary_type: BoundaryType) -> PIQuery:
        """Set the boundary type for recorded values.

        Args:
            boundary_type: Boundary type to use

        Returns:
            Self for method chaining
        """
        self._options.boundary_type = boundary_type
        return self

    def filter(self, expression: str) -> PIQuery:
        """Set a filter expression.

        Args:
            expression: PI filter expression

        Returns:
            Self for method chaining
        """
        self._options.filter_expression = expression
        return self

    def pivot(self) -> PIQuery:
        """Pivot results to wide format (tags as columns).

        Returns:
            Self for method chaining
        """
        self._options.pivot = True
        return self

    def limit(self, max_count: int) -> PIQuery:
        """Limit the number of values returned.

        Args:
            max_count: Maximum values per tag

        Returns:
            Self for method chaining
        """
        self._options.max_count = max_count
        return self

    # -------------------------------------------------------------------------
    # Execution Methods
    # -------------------------------------------------------------------------

    def to_dataframe(self) -> pl.DataFrame:
        """Execute the query and return a Polars DataFrame.

        Returns:
            DataFrame with query results
        """
        self._validate()

        if self._options.query_type == QueryType.SNAPSHOT:
            return self._execute_snapshot()
        elif self._options.query_type == QueryType.RECORDED:
            return self._execute_recorded()
        elif self._options.query_type == QueryType.INTERPOLATED:
            return self._execute_interpolated()
        elif self._options.query_type == QueryType.PLOT:
            return self._execute_plot()
        elif self._options.query_type == QueryType.SUMMARY:
            return self._execute_summary()
        else:
            raise ValueError(f"Unknown query type: {self._options.query_type}")

    def to_lazy_frame(self) -> pl.LazyFrame:
        """Execute the query and return a Polars LazyFrame.

        Returns:
            LazyFrame for deferred execution
        """
        return self.to_dataframe().lazy()

    def _validate(self) -> None:
        """Validate the query configuration."""
        if self._options.query_type != QueryType.SNAPSHOT:
            if self._options.start is None or self._options.end is None:
                raise ValueError(
                    "Time range must be set. Use .time_range() or .last()"
                )

    def _execute_snapshot(self) -> pl.DataFrame:
        """Execute a snapshot query."""
        if len(self._tags) == 1:
            return self._client.snapshot(self._tags[0])
        else:
            return self._client.snapshots(self._tags)

    def _execute_recorded(self) -> pl.DataFrame:
        """Execute a recorded values query."""
        return self._client.recorded_values(
            self._tags,
            start=self._options.start,  # type: ignore
            end=self._options.end,  # type: ignore
            max_count=self._options.max_count,
            include_quality=self._options.include_quality,
            pivot=self._options.pivot,
        )

    def _execute_interpolated(self) -> pl.DataFrame:
        """Execute an interpolated values query."""
        return self._client.interpolated_values(
            self._tags,
            start=self._options.start,  # type: ignore
            end=self._options.end,  # type: ignore
            interval=self._options.interval or "1h",
            include_quality=self._options.include_quality,
            pivot=self._options.pivot,
        )

    def _execute_plot(self) -> pl.DataFrame:
        """Execute a plot values query."""
        if len(self._tags) > 1:
            raise ValueError("Plot values only supports single tag queries")

        return self._client.plot_values(
            self._tags[0],
            start=self._options.start,  # type: ignore
            end=self._options.end,  # type: ignore
            intervals=self._options.plot_intervals,
            include_quality=self._options.include_quality,
        )

    def _execute_summary(self) -> pl.DataFrame:
        """Execute a summary query."""
        return self._client.summary(
            self._tags,
            start=self._options.start,  # type: ignore
            end=self._options.end,  # type: ignore
            summary_types=self._options.summary_types,
        )

    # -------------------------------------------------------------------------
    # String Representation
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        """String representation of the query."""
        return (
            f"PIQuery(tags={self._tags}, "
            f"type={self._options.query_type.name}, "
            f"start={self._options.start}, "
            f"end={self._options.end})"
        )
