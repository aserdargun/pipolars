"""PI Point data extraction.

This module provides functionality for extracting time-series data
from PI Points (tags) in the PI Data Archive.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.exceptions import PIDataError, PIPointNotFoundError
from pipolars.core.types import (
    AFTime,
    BoundaryType,
    DataQuality,
    PITimestamp,
    PIValue,
    PointConfig,
    PointType,
    SummaryType,
    TimeRange,
)

if TYPE_CHECKING:
    from pipolars.connection.server import PIServerConnection

logger = logging.getLogger(__name__)


@dataclass
class RecordedValuesOptions:
    """Options for recorded values retrieval."""

    boundary_type: BoundaryType = BoundaryType.INSIDE
    filter_expression: str | None = None
    include_filtered_values: bool = False
    max_count: int = 0  # 0 = no limit


@dataclass
class InterpolatedValuesOptions:
    """Options for interpolated values retrieval."""

    interval: str = "1h"
    filter_expression: str | None = None
    include_filtered_values: bool = False


@dataclass
class PlotValuesOptions:
    """Options for plot values retrieval."""

    intervals: int = 640
    include_min_max: bool = True


class PIPointExtractor:
    """Extracts time-series data from PI Points.

    This class provides methods for retrieving various types of data
    from PI Points including recorded, interpolated, and summary values.

    Example:
        >>> extractor = PIPointExtractor(connection)
        >>> values = extractor.recorded_values(
        ...     "SINUSOID",
        ...     start="*-1d",
        ...     end="*"
        ... )
        >>> for v in values:
        ...     print(f"{v.timestamp}: {v.value}")
    """

    def __init__(self, connection: PIServerConnection) -> None:
        """Initialize the extractor.

        Args:
            connection: Active PI Server connection
        """
        self._connection = connection
        self._sdk = get_sdk_manager()

    def _parse_time(self, time: PITimestamp) -> Any:
        """Convert a timestamp to AFTime.

        Args:
            time: Timestamp in various formats

        Returns:
            AFTime object
        """
        AFTime_class = self._sdk.af_time_class

        if isinstance(time, datetime):
            return AFTime_class(time.isoformat())
        elif isinstance(time, AFTime):
            return AFTime_class(time.expression)
        else:
            return AFTime_class(str(time))

    def _create_time_range(
        self,
        start: PITimestamp,
        end: PITimestamp,
    ) -> Any:
        """Create an AFTimeRange from start and end times.

        Args:
            start: Start time
            end: End time

        Returns:
            AFTimeRange object
        """
        AFTimeRange = self._sdk.af_time_range_class
        start_time = self._parse_time(start)
        end_time = self._parse_time(end)
        return AFTimeRange(start_time, end_time)

    def _convert_value(self, af_value: Any) -> PIValue:
        """Convert an AFValue to PIValue.

        Args:
            af_value: The AFValue from PI SDK

        Returns:
            PIValue object
        """
        # Get timestamp
        timestamp = af_value.Timestamp.LocalTime

        # Get value - handle digital states and errors
        value = af_value.Value
        if hasattr(value, "Name"):
            # Digital state
            value = str(value.Name)
        elif hasattr(value, "ToString"):
            # Try to get numeric value
            try:
                value = float(value)
            except (ValueError, TypeError):
                value = str(value.ToString())

        # Get quality
        quality = DataQuality.GOOD
        if af_value.IsGood is False:
            quality = DataQuality.BAD
        elif hasattr(af_value, "Substituted") and af_value.Substituted:
            quality = DataQuality.SUBSTITUTED

        return PIValue(
            timestamp=timestamp,
            value=value,
            quality=quality,
        )

    def get_point_config(self, tag_name: str) -> PointConfig:
        """Get configuration/metadata for a PI Point.

        Args:
            tag_name: The PI Point name

        Returns:
            PointConfig with point metadata
        """
        point = self._connection.get_point(tag_name)

        # Get point attributes
        attrs = point.GetAttributes([
            "pointid",
            "pointtype",
            "descriptor",
            "engunits",
            "zero",
            "span",
            "displaydigits",
            "typicalvalue",
        ])

        # Map point type
        point_type_map = {
            "Float16": PointType.FLOAT16,
            "Float32": PointType.FLOAT32,
            "Float64": PointType.FLOAT64,
            "Int16": PointType.INT16,
            "Int32": PointType.INT32,
            "Digital": PointType.DIGITAL,
            "Timestamp": PointType.TIMESTAMP,
            "String": PointType.STRING,
            "Blob": PointType.BLOB,
        }

        point_type_str = str(attrs.get("pointtype", "Float32"))
        point_type = point_type_map.get(point_type_str, PointType.FLOAT64)

        return PointConfig(
            name=tag_name,
            point_id=int(attrs.get("pointid", 0)),
            point_type=point_type,
            description=str(attrs.get("descriptor", "")),
            engineering_units=str(attrs.get("engunits", "")),
            zero=float(attrs.get("zero", 0.0)),
            span=float(attrs.get("span", 100.0)),
            display_digits=int(attrs.get("displaydigits", -5)),
            typical_value=float(attrs.get("typicalvalue")) if attrs.get("typicalvalue") else None,
        )

    def snapshot(self, tag_name: str) -> PIValue:
        """Get the current snapshot value for a PI Point.

        Args:
            tag_name: The PI Point name

        Returns:
            Current PIValue
        """
        point = self._connection.get_point(tag_name)
        af_value = point.CurrentValue()
        return self._convert_value(af_value)

    def snapshots(self, tag_names: list[str]) -> dict[str, PIValue]:
        """Get current snapshot values for multiple PI Points.

        Args:
            tag_names: List of PI Point names

        Returns:
            Dictionary mapping tag names to PIValues
        """
        PIPointList = self._sdk.pi_point_list_class
        point_list = PIPointList()

        for tag_name in tag_names:
            point = self._connection.get_point(tag_name)
            point_list.Add(point)

        # Get all snapshots at once
        af_values = point_list.CurrentValue()

        result = {}
        for i, tag_name in enumerate(tag_names):
            result[tag_name] = self._convert_value(af_values[i])

        return result

    def recorded_values(
        self,
        tag_name: str,
        start: PITimestamp,
        end: PITimestamp,
        options: RecordedValuesOptions | None = None,
    ) -> list[PIValue]:
        """Get recorded values for a PI Point.

        Args:
            tag_name: The PI Point name
            start: Start time
            end: End time
            options: Optional retrieval options

        Returns:
            List of PIValue objects
        """
        options = options or RecordedValuesOptions()
        point = self._connection.get_point(tag_name)
        time_range = self._create_time_range(start, end)

        # Get boundary type enum
        AFBoundaryType = self._sdk.get_type("OSIsoft.AF.Data", "AFBoundaryType")
        boundary_map = {
            BoundaryType.INSIDE: AFBoundaryType.Inside,
            BoundaryType.OUTSIDE: AFBoundaryType.Outside,
            BoundaryType.INTERPOLATED: AFBoundaryType.Interpolated,
        }
        boundary = boundary_map.get(options.boundary_type, AFBoundaryType.Inside)

        # Call RecordedValues
        af_values = point.RecordedValues(
            time_range,
            boundary,
            options.filter_expression,
            options.include_filtered_values,
            options.max_count,
        )

        return [self._convert_value(v) for v in af_values]

    def recorded_values_iterator(
        self,
        tag_name: str,
        start: PITimestamp,
        end: PITimestamp,
        page_size: int = 10000,
    ) -> Iterator[PIValue]:
        """Iterate over recorded values with pagination.

        Args:
            tag_name: The PI Point name
            start: Start time
            end: End time
            page_size: Number of values per page

        Yields:
            PIValue objects
        """
        point = self._connection.get_point(tag_name)
        time_range = self._create_time_range(start, end)

        AFBoundaryType = self._sdk.get_type("OSIsoft.AF.Data", "AFBoundaryType")
        PIPagingConfiguration = self._sdk.get_type(
            "OSIsoft.AF.PI", "PIPagingConfiguration"
        )

        # Configure paging
        paging_config = PIPagingConfiguration(
            PIPagingConfiguration.PageType.EventCount,
            page_size,
        )

        # Get paginated results
        af_values = point.RecordedValues(
            time_range,
            AFBoundaryType.Inside,
            None,  # filter expression
            False,  # include filtered
            paging_config,
        )

        for af_value in af_values:
            yield self._convert_value(af_value)

    def interpolated_values(
        self,
        tag_name: str,
        start: PITimestamp,
        end: PITimestamp,
        interval: str = "1h",
        options: InterpolatedValuesOptions | None = None,
    ) -> list[PIValue]:
        """Get interpolated values for a PI Point.

        Args:
            tag_name: The PI Point name
            start: Start time
            end: End time
            interval: Time interval (e.g., "1h", "15m", "1d")
            options: Optional retrieval options

        Returns:
            List of PIValue objects at regular intervals
        """
        options = options or InterpolatedValuesOptions()
        point = self._connection.get_point(tag_name)
        time_range = self._create_time_range(start, end)

        # Parse interval
        AFTimeSpan = self._sdk.get_type("OSIsoft.AF.Time", "AFTimeSpan")
        time_interval = AFTimeSpan.Parse(interval)

        # Call InterpolatedValues
        af_values = point.InterpolatedValues(
            time_range,
            time_interval,
            options.filter_expression,
            options.include_filtered_values,
        )

        return [self._convert_value(v) for v in af_values]

    def plot_values(
        self,
        tag_name: str,
        start: PITimestamp,
        end: PITimestamp,
        intervals: int = 640,
    ) -> list[PIValue]:
        """Get plot values for a PI Point.

        Plot values are optimized for graphing, returning a reduced
        set of values that preserve the visual appearance of the data.

        Args:
            tag_name: The PI Point name
            start: Start time
            end: End time
            intervals: Number of intervals for the plot

        Returns:
            List of PIValue objects optimized for plotting
        """
        point = self._connection.get_point(tag_name)
        time_range = self._create_time_range(start, end)

        af_values = point.PlotValues(time_range, intervals)

        return [self._convert_value(v) for v in af_values]

    def summary(
        self,
        tag_name: str,
        start: PITimestamp,
        end: PITimestamp,
        summary_types: SummaryType | list[SummaryType] = SummaryType.AVERAGE,
    ) -> dict[str, Any]:
        """Get summary values for a PI Point.

        Args:
            tag_name: The PI Point name
            start: Start time
            end: End time
            summary_types: Summary type(s) to calculate

        Returns:
            Dictionary with summary values
        """
        point = self._connection.get_point(tag_name)
        time_range = self._create_time_range(start, end)

        # Convert summary types to SDK enum
        AFSummaryTypes = self._sdk.get_type("OSIsoft.AF.Data", "AFSummaryTypes")

        if isinstance(summary_types, list):
            sdk_summary = AFSummaryTypes.None_
            for st in summary_types:
                sdk_summary |= AFSummaryTypes(st.value)
        else:
            sdk_summary = AFSummaryTypes(summary_types.value)

        # Get summary
        AFCalculationBasis = self._sdk.get_type("OSIsoft.AF.Data", "AFCalculationBasis")
        AFTimestampCalculation = self._sdk.get_type(
            "OSIsoft.AF.Data", "AFTimestampCalculation"
        )

        summaries = point.Summary(
            time_range,
            sdk_summary,
            AFCalculationBasis.TimeWeighted,
            AFTimestampCalculation.Auto,
        )

        # Convert to dictionary
        result = {}
        summary_name_map = {
            1: "total",
            2: "average",
            4: "minimum",
            8: "maximum",
            16: "range",
            32: "std_dev",
            64: "pop_std_dev",
            128: "count",
            8192: "percent_good",
        }

        for summary in summaries:
            summary_type_value = int(summary.SummaryType)
            name = summary_name_map.get(summary_type_value, str(summary_type_value))
            result[name] = summary.Value.Value

        return result

    def summaries(
        self,
        tag_name: str,
        start: PITimestamp,
        end: PITimestamp,
        interval: str,
        summary_types: SummaryType | list[SummaryType] = SummaryType.AVERAGE,
    ) -> list[dict[str, Any]]:
        """Get summary values over multiple intervals.

        Args:
            tag_name: The PI Point name
            start: Start time
            end: End time
            interval: Time interval for each summary
            summary_types: Summary type(s) to calculate

        Returns:
            List of dictionaries with summary values per interval
        """
        point = self._connection.get_point(tag_name)
        time_range = self._create_time_range(start, end)

        AFSummaryTypes = self._sdk.get_type("OSIsoft.AF.Data", "AFSummaryTypes")
        AFTimeSpan = self._sdk.get_type("OSIsoft.AF.Time", "AFTimeSpan")
        AFCalculationBasis = self._sdk.get_type("OSIsoft.AF.Data", "AFCalculationBasis")
        AFTimestampCalculation = self._sdk.get_type(
            "OSIsoft.AF.Data", "AFTimestampCalculation"
        )

        time_interval = AFTimeSpan.Parse(interval)

        if isinstance(summary_types, list):
            sdk_summary = AFSummaryTypes.None_
            for st in summary_types:
                sdk_summary |= AFSummaryTypes(st.value)
        else:
            sdk_summary = AFSummaryTypes(summary_types.value)

        summaries = point.Summaries(
            time_range,
            time_interval,
            sdk_summary,
            AFCalculationBasis.TimeWeighted,
            AFTimestampCalculation.Auto,
        )

        # Convert to list of dictionaries
        results = []
        for summary_dict in summaries.Values:
            interval_results = {
                "timestamp": summary_dict.Key.LocalTime,
            }
            for summary in summary_dict.Value:
                summary_type_value = int(summary.SummaryType)
                name = self._get_summary_name(summary_type_value)
                interval_results[name] = summary.Value.Value
            results.append(interval_results)

        return results

    def _get_summary_name(self, summary_type_value: int) -> str:
        """Get the name for a summary type value."""
        summary_name_map = {
            1: "total",
            2: "average",
            4: "minimum",
            8: "maximum",
            16: "range",
            32: "std_dev",
            64: "pop_std_dev",
            128: "count",
            8192: "percent_good",
        }
        return summary_name_map.get(summary_type_value, str(summary_type_value))

    def value_at(self, tag_name: str, time: PITimestamp) -> PIValue:
        """Get the value at a specific time.

        Args:
            tag_name: The PI Point name
            time: The time to get the value for

        Returns:
            PIValue at the specified time
        """
        point = self._connection.get_point(tag_name)
        af_time = self._parse_time(time)

        AFRetrievalMode = self._sdk.get_type("OSIsoft.AF.Data", "AFRetrievalMode")
        af_value = point.RecordedValue(af_time, AFRetrievalMode.AtOrBefore)

        return self._convert_value(af_value)
