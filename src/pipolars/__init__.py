"""
PIPolars - High-performance PI System data extraction with Polars DataFrames.

A modern Python library for extracting data from OSIsoft PI System and converting
it to Polars DataFrames for efficient data science workflows.

Example:
    >>> from pipolars import PIClient
    >>> with PIClient("PI-SERVER") as client:
    ...     df = client.points["SINUSOID"].recorded_values(
    ...         start="-1d",
    ...         end="*"
    ...     )
    ...     print(df)
"""

from pipolars.api.client import PIClient
from pipolars.api.query import PIQuery
from pipolars.core.config import PIConfig
from pipolars.core.exceptions import (
    PIConnectionError,
    PIDataError,
    PIPolarsError,
    PIQueryError,
)
from pipolars.core.types import (
    AFTime,
    DataQuality,
    PIValue,
    RetrievalMode,
    SummaryType,
    TimestampMode,
)

__version__ = "0.1.0"
__all__ = [
    # Main client
    "PIClient",
    "PIQuery",
    # Configuration
    "PIConfig",
    # Types
    "AFTime",
    "PIValue",
    "RetrievalMode",
    "SummaryType",
    "TimestampMode",
    "DataQuality",
    # Exceptions
    "PIPolarsError",
    "PIConnectionError",
    "PIDataError",
    "PIQueryError",
]
