"""Timestamp handling utilities for PI data.

This module provides utilities for handling and converting
timestamps between PI System and Polars formats.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

import polars as pl

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.exceptions import PITimeParseError
from pipolars.core.types import AFTime, PITimestamp


class TimestampHandler:
    """Handles timestamp conversion between PI System and Python/Polars.

    This class provides utilities for:
    - Parsing PI time expressions (like "*-1d", "t", "y")
    - Converting between timezones
    - Creating proper Polars datetime columns

    Example:
        >>> handler = TimestampHandler(timezone="America/New_York")
        >>> dt = handler.parse("*-1d")  # 1 day ago
        >>> af_time = handler.to_af_time("2024-01-15T10:00:00")
    """

    # PI time expression patterns
    RELATIVE_PATTERNS = {
        "*": "now",
        "t": "today",
        "y": "yesterday",
    }

    def __init__(self, timezone_str: str = "UTC") -> None:
        """Initialize the timestamp handler.

        Args:
            timezone_str: Default timezone for conversions
        """
        self._timezone_str = timezone_str
        self._timezone = ZoneInfo(timezone_str)
        self._sdk = get_sdk_manager()

    @property
    def timezone(self) -> ZoneInfo:
        """Get the configured timezone."""
        return self._timezone

    def parse(self, time_expr: PITimestamp) -> datetime:
        """Parse a PI time expression to a Python datetime.

        Args:
            time_expr: Time expression (datetime, string, or AFTime)

        Returns:
            Python datetime with timezone

        Raises:
            PITimeParseError: If the expression cannot be parsed
        """
        if isinstance(time_expr, datetime):
            if time_expr.tzinfo is None:
                return time_expr.replace(tzinfo=self._timezone)
            return time_expr

        if isinstance(time_expr, AFTime):
            time_expr = time_expr.expression

        # Use AF SDK to parse the expression
        try:
            AFTime_class = self._sdk.af_time_class
            af_time = AFTime_class(str(time_expr))
            local_time = af_time.LocalTime

            # Convert .NET DateTime to Python datetime
            dt = datetime(
                local_time.Year,
                local_time.Month,
                local_time.Day,
                local_time.Hour,
                local_time.Minute,
                local_time.Second,
                local_time.Millisecond * 1000,
            )

            return dt.replace(tzinfo=self._timezone)

        except Exception as e:
            raise PITimeParseError(str(time_expr), reason=str(e)) from e

    def to_af_time(self, time_expr: PITimestamp) -> Any:
        """Convert a timestamp to an AFTime object.

        Args:
            time_expr: Time expression to convert

        Returns:
            AFTime object from the SDK
        """
        AFTime_class = self._sdk.af_time_class

        if isinstance(time_expr, datetime):
            return AFTime_class(time_expr.isoformat())
        elif isinstance(time_expr, AFTime):
            return AFTime_class(time_expr.expression)
        else:
            return AFTime_class(str(time_expr))

    def to_af_time_range(
        self,
        start: PITimestamp,
        end: PITimestamp,
    ) -> Any:
        """Create an AFTimeRange from start and end times.

        Args:
            start: Start time
            end: End time

        Returns:
            AFTimeRange object from the SDK
        """
        AFTimeRange = self._sdk.af_time_range_class
        start_time = self.to_af_time(start)
        end_time = self.to_af_time(end)
        return AFTimeRange(start_time, end_time)

    def localize(self, dt: datetime) -> datetime:
        """Localize a naive datetime to the configured timezone.

        Args:
            dt: Datetime to localize

        Returns:
            Localized datetime
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=self._timezone)
        return dt.astimezone(self._timezone)

    def to_utc(self, dt: datetime) -> datetime:
        """Convert a datetime to UTC.

        Args:
            dt: Datetime to convert

        Returns:
            Datetime in UTC
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self._timezone)
        return dt.astimezone(timezone.utc)

    def format_for_pi(self, dt: datetime) -> str:
        """Format a datetime for PI System queries.

        Args:
            dt: Datetime to format

        Returns:
            Formatted string for PI queries
        """
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def parse_interval(interval: str) -> timedelta:
        """Parse a PI interval string to a timedelta.

        Args:
            interval: Interval string (e.g., "1h", "30m", "1d")

        Returns:
            Python timedelta

        Raises:
            PITimeParseError: If the interval cannot be parsed
        """
        unit_map = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
            "w": "weeks",
        }

        try:
            # Extract number and unit
            value = ""
            unit = ""

            for char in interval:
                if char.isdigit() or char == ".":
                    value += char
                else:
                    unit += char

            if not value or not unit:
                raise ValueError("Invalid format")

            unit = unit.lower().strip()
            if unit not in unit_map:
                raise ValueError(f"Unknown unit: {unit}")

            kwargs = {unit_map[unit]: float(value)}
            return timedelta(**kwargs)

        except Exception as e:
            raise PITimeParseError(
                interval, reason=f"Invalid interval format: {e}"
            ) from e

    @staticmethod
    def generate_time_range(
        start: datetime,
        end: datetime,
        interval: timedelta,
    ) -> list[datetime]:
        """Generate a list of timestamps at regular intervals.

        Args:
            start: Start time
            end: End time
            interval: Time interval

        Returns:
            List of datetime objects
        """
        timestamps = []
        current = start

        while current <= end:
            timestamps.append(current)
            current += interval

        return timestamps

    def to_polars_datetime(
        self,
        timestamps: list[datetime],
        time_unit: Literal["ns", "us", "ms"] = "us",
    ) -> pl.Series:
        """Convert timestamps to a Polars datetime Series.

        Args:
            timestamps: List of datetime objects
            time_unit: Polars time unit ("ns", "us", "ms")

        Returns:
            Polars Series with datetime type
        """
        series = pl.Series("timestamp", timestamps)
        return series.cast(pl.Datetime(time_unit, self._timezone_str))

    @staticmethod
    def resample_timestamps(
        df: pl.DataFrame,
        timestamp_col: str,
        interval: str,
        aggregation: str = "mean",
    ) -> pl.DataFrame:
        """Resample a DataFrame to a new time interval.

        Args:
            df: Input DataFrame
            timestamp_col: Name of the timestamp column
            interval: New interval (e.g., "1h", "1d")
            aggregation: Aggregation method ("mean", "sum", "min", "max")

        Returns:
            Resampled DataFrame
        """
        agg_funcs = {
            "mean": pl.mean,
            "sum": pl.sum,
            "min": pl.min,
            "max": pl.max,
            "first": pl.first,
            "last": pl.last,
            "count": pl.count,
        }

        if aggregation not in agg_funcs:
            raise ValueError(f"Unknown aggregation: {aggregation}")

        # Use Polars group_by_dynamic for resampling
        numeric_cols = [
            c for c in df.columns if c != timestamp_col and df[c].dtype.is_numeric()
        ]

        agg_exprs = [agg_funcs[aggregation](c).alias(c) for c in numeric_cols]

        return df.group_by_dynamic(timestamp_col, every=interval).agg(agg_exprs)
