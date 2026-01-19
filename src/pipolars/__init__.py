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
    AnalysisInfo,
    AnalysisStatus,
    DataQuality,
    PIValue,
    PointConfig,
    RetrievalMode,
    SummaryType,
    TimestampMode,
)

try:
    from pipolars._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"
__all__ = [
    # Main client
    "PIClient",
    "PIQuery",
    # Configuration
    "PIConfig",
    # Types
    "AFTime",
    "AnalysisInfo",
    "AnalysisStatus",
    "DataQuality",
    "PIValue",
    "PointConfig",
    "RetrievalMode",
    "SummaryType",
    "TimestampMode",
    # Exceptions
    "PIPolarsError",
    "PIConnectionError",
    "PIDataError",
    "PIQueryError",
]
