"""PI Point data extraction.

This module provides functionality for extracting time-series data
from PI Points (tags) in the PI Data Archive.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.types import (
    AFTime,
    BoundaryType,
    DataQuality,
    PITimestamp,
    PIValue,
    PointConfig,
    PointType,
    SummaryType,
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

    def _convert_net_datetime(self, net_datetime: Any) -> datetime:
        """Convert a .NET DateTime to Python datetime.

        Args:
            net_datetime: .NET DateTime object

        Returns:
            Python datetime object
        """
        return datetime(
            net_datetime.Year,
            net_datetime.Month,
            net_datetime.Day,
            net_datetime.Hour,
            net_datetime.Minute,
            net_datetime.Second,
            net_datetime.Millisecond * 1000,  # Convert ms to us
        )

    def _convert_value(self, af_value: Any) -> PIValue:
        """Convert an AFValue to PIValue.

        Args:
            af_value: The AFValue from PI SDK

        Returns:
            PIValue object
        """
        # Get timestamp and convert from .NET DateTime to Python datetime
        net_timestamp = af_value.Timestamp.LocalTime
        timestamp = self._convert_net_datetime(net_timestamp)

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

    def _get_attr(self, attrs: Any, key: str, default: Any = None) -> Any:
        """Safely get a value from a .NET IDictionary.

        Args:
            attrs: .NET IDictionary object
            key: Key to look up
            default: Default value if key not found

        Returns:
            Value from dictionary or default
        """
        try:
            if attrs.ContainsKey(key):
                return attrs[key]
            return default
        except Exception:
            return default

    def get_point_config(self, tag_name: str) -> PointConfig:
        """Get configuration/metadata for a PI Point.

        Args:
            tag_name: The PI Point name

        Returns:
            PointConfig with point metadata
        """
        point = self._connection.get_point(tag_name)

        # Get point attributes - including additional attributes
        attrs = point.GetAttributes([
            # Core attributes
            "pointid",
            "pointtype",
            "descriptor",
            "engunits",
            "zero",
            "span",
            "displaydigits",
            "typicalvalue",
            # Alarm thresholds
            "valuehighalarm",
            "valuelowalarm",
            "valuehighwarning",
            "valuelowwarning",
            # Rate of change limits
            "rocinghighvalue",
            "rocinglowvalue",
            # Interface information
            "interfaceid",
            "interfacename",
            # Scan and source information
            "scantime",
            "srcptid",
            "srcptname",
            # Additional metadata
            "convers",
            "devname",
            "alias",
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

        point_type_str = str(self._get_attr(attrs, "pointtype", "Float32"))
        point_type = point_type_map.get(point_type_str, PointType.FLOAT64)

        # Helper to safely convert to float
        def safe_float(val: Any) -> float | None:
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Helper to safely convert to int
        def safe_int(val: Any) -> int | None:
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        return PointConfig(
            name=tag_name,
            point_id=int(self._get_attr(attrs, "pointid", 0)),
            point_type=point_type,
            description=str(self._get_attr(attrs, "descriptor", "")),
            engineering_units=str(self._get_attr(attrs, "engunits", "")),
            zero=float(self._get_attr(attrs, "zero", 0.0)),
            span=float(self._get_attr(attrs, "span", 100.0)),
            display_digits=int(self._get_attr(attrs, "displaydigits", -5)),
            typical_value=safe_float(self._get_attr(attrs, "typicalvalue")),
            # Alarm thresholds
            value_high_alarm=safe_float(self._get_attr(attrs, "valuehighalarm")),
            value_low_alarm=safe_float(self._get_attr(attrs, "valuelowalarm")),
            value_high_warning=safe_float(self._get_attr(attrs, "valuehighwarning")),
            value_low_warning=safe_float(self._get_attr(attrs, "valuelowwarning")),
            # Rate of change limits
            roc_high_value=safe_float(self._get_attr(attrs, "rocinghighvalue")),
            roc_low_value=safe_float(self._get_attr(attrs, "rocinglowvalue")),
            # Interface information
            interface_id=safe_int(self._get_attr(attrs, "interfaceid")),
            interface_name=str(self._get_attr(attrs, "interfacename", "")),
            # Scan and source information
            scan_time=str(self._get_attr(attrs, "scantime", "")),
            source_point_id=safe_int(self._get_attr(attrs, "srcptid")),
            source_point_name=str(self._get_attr(attrs, "srcptname", "")),
            # Additional metadata
            conversion_factor=safe_float(self._get_attr(attrs, "convers")),
            device_name=str(self._get_attr(attrs, "devname", "")),
            alias=str(self._get_attr(attrs, "alias", "")),
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
            # Start with first type, then OR the rest
            sdk_summary = AFSummaryTypes(summary_types[0].value)
            for st in summary_types[1:]:
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
        # PI SDK returns IDictionary<AFSummaryTypes, AFValue>
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

        # Iterate over dictionary keys
        for key in summaries.Keys:
            summary_type_value = int(key)
            name = summary_name_map.get(summary_type_value, str(summary_type_value))
            af_value = summaries[key]
            # Extract the actual value
            value = af_value.Value
            if hasattr(value, "Value"):
                value = value.Value
            try:
                result[name] = float(value)
            except (ValueError, TypeError):
                result[name] = value

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
            # Start with first type, then OR the rest
            sdk_summary = AFSummaryTypes(summary_types[0].value)
            for st in summary_types[1:]:
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
        # PI SDK returns IDictionary<AFSummaryTypes, AFValues>
        # AFValues is a collection of timestamped values
        # We need to pivot this into a list of {timestamp, summary1, summary2, ...}

        # First, collect all data by timestamp
        timestamp_data: dict[datetime, dict[str, Any]] = {}

        for summary_type_key in summaries.Keys:
            summary_type_value = int(summary_type_key)
            name = self._get_summary_name(summary_type_value)
            af_values = summaries[summary_type_key]

            # AFValues is iterable
            for af_value in af_values:
                net_timestamp = af_value.Timestamp.LocalTime
                py_timestamp = self._convert_net_datetime(net_timestamp)

                if py_timestamp not in timestamp_data:
                    timestamp_data[py_timestamp] = {"timestamp": py_timestamp}

                value = af_value.Value
                if hasattr(value, "Value"):
                    value = value.Value
                try:
                    timestamp_data[py_timestamp][name] = float(value)
                except (ValueError, TypeError):
                    timestamp_data[py_timestamp][name] = value

        # Convert to sorted list
        results = list(timestamp_data.values())
        results.sort(key=lambda x: x["timestamp"])

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
