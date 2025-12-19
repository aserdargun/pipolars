"""Type definitions for PIPolars library.

This module contains all the type definitions, enums, and data classes used
throughout the library for type safety and better IDE support.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, IntEnum, auto
from typing import Any, Literal, TypeAlias, Union

import polars as pl


# Type aliases for common types
PITimestamp: TypeAlias = Union[datetime, str, "AFTime"]
TagName: TypeAlias = str
TagPath: TypeAlias = str


class RetrievalMode(str, Enum):
    """Data retrieval modes for PI Point queries.

    These modes determine how data is retrieved from the PI Data Archive.
    """

    RECORDED = "recorded"
    """Return actual recorded values as stored in the archive."""

    INTERPOLATED = "interpolated"
    """Return interpolated values at regular intervals."""

    PLOT = "plot"
    """Return values optimized for plotting (reduced data density)."""

    SUMMARY = "summary"
    """Return summary statistics (min, max, avg, etc.)."""

    COMPRESSED = "compressed"
    """Return compressed data using exception/compression settings."""


class SummaryType(IntEnum):
    """Summary calculation types for PI data.

    These correspond to OSIsoft AF SDK AFSummaryTypes enumeration.
    """

    NONE = 0
    TOTAL = 1
    AVERAGE = 2
    MINIMUM = 4
    MAXIMUM = 8
    RANGE = 16
    STD_DEV = 32
    POP_STD_DEV = 64
    COUNT = 128
    PERCENT_GOOD = 8192
    TOTAL_WITH_UOM = 16384
    ALL = 24831
    ALL_FOR_NON_NUMERIC = 8320


class TimestampMode(str, Enum):
    """Timestamp handling modes for summary calculations."""

    AUTO = "auto"
    """Automatically determine timestamp placement."""

    START = "start"
    """Use interval start time."""

    END = "end"
    """Use interval end time."""

    MIDDLE = "middle"
    """Use interval midpoint."""


class DataQuality(IntEnum):
    """PI data quality flags.

    These flags indicate the quality and reliability of PI values.
    """

    GOOD = 0
    """Value is good and reliable."""

    SUBSTITUTED = 1
    """Value was manually substituted."""

    QUESTIONABLE = 2
    """Value quality is questionable."""

    BAD = 3
    """Value is bad or unreliable."""

    NO_DATA = 4
    """No data available for the requested time."""

    CALC_FAILED = 5
    """Calculation failed to produce a value."""


class DigitalState(str, Enum):
    """Common PI digital states."""

    NO_DATA = "No Data"
    BAD_INPUT = "Bad Input"
    CALC_OFF = "Calc Off"
    COMM_FAIL = "Comm Fail"
    CONFIGURE = "Configure"
    I_O_TIMEOUT = "I/O Timeout"
    NO_SAMPLE = "No Sample"
    SHUTDOWN = "Shutdown"
    SCAN_OFF = "Scan Off"
    OVER_RANGE = "Over Range"
    UNDER_RANGE = "Under Range"


class PointType(str, Enum):
    """PI Point data types."""

    FLOAT16 = "float16"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INT16 = "int16"
    INT32 = "int32"
    DIGITAL = "digital"
    TIMESTAMP = "timestamp"
    STRING = "string"
    BLOB = "blob"


class BoundaryType(str, Enum):
    """Boundary handling for time range queries."""

    INSIDE = "inside"
    """Only include values strictly inside the time range."""

    OUTSIDE = "outside"
    """Include boundary values outside the range."""

    INTERPOLATED = "interpolated"
    """Interpolate values at boundaries."""


@dataclass(frozen=True, slots=True)
class AFTime:
    """Represents a PI AF Time specification.

    Supports both absolute timestamps and relative time expressions
    like "*" (now), "*-1d" (1 day ago), "t" (today), etc.

    Examples:
        >>> AFTime("*")  # Now
        >>> AFTime("*-1h")  # 1 hour ago
        >>> AFTime("2024-01-01")  # Absolute date
        >>> AFTime("t")  # Today at midnight
        >>> AFTime("y")  # Yesterday at midnight
    """

    expression: str
    """The time expression string."""

    def __str__(self) -> str:
        return self.expression

    @classmethod
    def now(cls) -> AFTime:
        """Create an AFTime representing the current time."""
        return cls("*")

    @classmethod
    def today(cls) -> AFTime:
        """Create an AFTime representing today at midnight."""
        return cls("t")

    @classmethod
    def yesterday(cls) -> AFTime:
        """Create an AFTime representing yesterday at midnight."""
        return cls("y")

    @classmethod
    def ago(cls, **kwargs: int) -> AFTime:
        """Create an AFTime relative to now.

        Args:
            **kwargs: Time units (days, hours, minutes, seconds)

        Returns:
            AFTime representing the relative time.

        Example:
            >>> AFTime.ago(days=1, hours=2)  # 1 day and 2 hours ago
        """
        parts = []
        if days := kwargs.get("days"):
            parts.append(f"{days}d")
        if hours := kwargs.get("hours"):
            parts.append(f"{hours}h")
        if minutes := kwargs.get("minutes"):
            parts.append(f"{minutes}m")
        if seconds := kwargs.get("seconds"):
            parts.append(f"{seconds}s")

        offset = "".join(parts) if parts else "0s"
        return cls(f"*-{offset}")

    @classmethod
    def from_datetime(cls, dt: datetime) -> AFTime:
        """Create an AFTime from a Python datetime object."""
        return cls(dt.isoformat())


@dataclass(slots=True)
class PIValue:
    """Represents a single PI value with timestamp and quality.

    This is the fundamental data unit returned from PI queries.
    """

    timestamp: datetime
    """The timestamp of the value."""

    value: Any
    """The actual value (can be numeric, string, or digital state)."""

    quality: DataQuality = DataQuality.GOOD
    """The quality flag for this value."""

    is_good: bool = field(init=False)
    """Convenience property indicating if the value is good quality."""

    def __post_init__(self) -> None:
        self.is_good = self.quality == DataQuality.GOOD

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for DataFrame construction."""
        return {
            "timestamp": self.timestamp,
            "value": self.value,
            "quality": self.quality.value,
        }


@dataclass(frozen=True, slots=True)
class TimeRange:
    """Represents a time range for PI queries.

    Attributes:
        start: Start time of the range
        end: End time of the range
    """

    start: PITimestamp
    end: PITimestamp

    @classmethod
    def last(cls, **kwargs: int) -> TimeRange:
        """Create a time range from now to the past.

        Example:
            >>> TimeRange.last(days=7)  # Last 7 days
            >>> TimeRange.last(hours=24)  # Last 24 hours
        """
        return cls(
            start=AFTime.ago(**kwargs),
            end=AFTime.now(),
        )

    @classmethod
    def today(cls) -> TimeRange:
        """Create a time range for today."""
        return cls(start=AFTime.today(), end=AFTime.now())


@dataclass(frozen=True, slots=True)
class PointConfig:
    """Configuration for a PI Point (tag).

    Contains metadata about a PI Point retrieved from the server.
    """

    name: str
    """The PI Point name (tag name)."""

    point_id: int
    """The unique point ID in the PI Data Archive."""

    point_type: PointType
    """The data type of the point."""

    description: str = ""
    """Description of the point."""

    engineering_units: str = ""
    """Engineering units for the point."""

    zero: float = 0.0
    """Zero value for scaling."""

    span: float = 100.0
    """Span value for scaling."""

    display_digits: int = -5
    """Number of display digits."""

    typical_value: float | None = None
    """Typical value for this point."""


@dataclass(frozen=True, slots=True)
class SummaryResult:
    """Result of a summary calculation.

    Contains the calculated summary values for a time range.
    """

    tag: str
    """The PI Point name."""

    start: datetime
    """Start time of the summary period."""

    end: datetime
    """End time of the summary period."""

    average: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    total: float | None = None
    count: int | None = None
    std_dev: float | None = None
    range: float | None = None
    percent_good: float | None = None


# Polars schema definitions for PI data
PI_VALUE_SCHEMA: dict[str, pl.DataType] = {
    "timestamp": pl.Datetime("us", "UTC"),
    "value": pl.Float64,
    "quality": pl.Int8,
}

PI_VALUE_WITH_TAG_SCHEMA: dict[str, pl.DataType] = {
    "tag": pl.Utf8,
    "timestamp": pl.Datetime("us", "UTC"),
    "value": pl.Float64,
    "quality": pl.Int8,
}

SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "tag": pl.Utf8,
    "start": pl.Datetime("us", "UTC"),
    "end": pl.Datetime("us", "UTC"),
    "average": pl.Float64,
    "minimum": pl.Float64,
    "maximum": pl.Float64,
    "total": pl.Float64,
    "count": pl.Int64,
    "std_dev": pl.Float64,
    "percent_good": pl.Float64,
}
