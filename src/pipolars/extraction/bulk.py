"""Bulk data extraction operations.

This module provides high-performance bulk data extraction for
multiple PI Points and AF Attributes simultaneously.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.types import (
    AFTime,
    DataQuality,
    PITimestamp,
    PIValue,
    SummaryType,
    TimeRange,
)

if TYPE_CHECKING:
    from pipolars.connection.server import PIServerConnection

logger = logging.getLogger(__name__)


@dataclass
class BulkResult:
    """Result of a bulk extraction operation."""

    tag: str
    values: list[PIValue]
    success: bool
    error: str | None = None


@dataclass
class BulkSummaryResult:
    """Result of a bulk summary operation."""

    tag: str
    summaries: dict[str, Any]
    success: bool
    error: str | None = None


class BulkExtractor:
    """High-performance bulk data extraction.

    This class provides efficient methods for extracting data from
    multiple PI Points simultaneously using parallel operations
    and the PI bulk data API.

    Example:
        >>> extractor = BulkExtractor(connection)
        >>> results = extractor.recorded_values(
        ...     tags=["TAG1", "TAG2", "TAG3"],
        ...     start="*-1d",
        ...     end="*"
        ... )
        >>> for tag, values in results.items():
        ...     print(f"{tag}: {len(values)} values")
    """

    def __init__(
        self,
        connection: PIServerConnection,
        max_parallel: int = 4,
    ) -> None:
        """Initialize the bulk extractor.

        Args:
            connection: Active PI Server connection
            max_parallel: Maximum parallel operations
        """
        self._connection = connection
        self._sdk = get_sdk_manager()
        self._max_parallel = max_parallel

    def _parse_time(self, time: PITimestamp) -> Any:
        """Convert a timestamp to AFTime."""
        AFTime_class = self._sdk.af_time_class

        if isinstance(time, datetime):
            return AFTime_class(time.isoformat())
        elif isinstance(time, AFTime):
            return AFTime_class(time.expression)
        else:
            return AFTime_class(str(time))

    def _create_time_range(self, start: PITimestamp, end: PITimestamp) -> Any:
        """Create an AFTimeRange."""
        AFTimeRange = self._sdk.af_time_range_class
        start_time = self._parse_time(start)
        end_time = self._parse_time(end)
        return AFTimeRange(start_time, end_time)

    def _convert_net_datetime(self, net_datetime: Any) -> datetime:
        """Convert a .NET DateTime to Python datetime."""
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
        """Convert an AFValue to PIValue."""
        net_timestamp = af_value.Timestamp.LocalTime
        timestamp = self._convert_net_datetime(net_timestamp)

        value = af_value.Value
        if hasattr(value, "Name"):
            value = str(value.Name)
        elif hasattr(value, "ToString"):
            try:
                value = float(value)
            except (ValueError, TypeError):
                value = str(value.ToString())

        quality = DataQuality.GOOD
        if af_value.IsGood is False:
            quality = DataQuality.BAD

        return PIValue(timestamp=timestamp, value=value, quality=quality)

    def snapshots(self, tags: list[str]) -> dict[str, PIValue]:
        """Get current snapshot values for multiple tags.

        Uses the bulk API for efficient retrieval.

        Args:
            tags: List of PI Point names

        Returns:
            Dictionary mapping tag names to PIValues
        """
        PIPointList = self._sdk.pi_point_list_class
        point_list = PIPointList()

        for tag in tags:
            point = self._connection.get_point(tag)
            point_list.Add(point)

        # Get all snapshots at once using bulk API
        af_values = point_list.CurrentValue()

        result = {}
        for i, tag in enumerate(tags):
            result[tag] = self._convert_value(af_values[i])

        return result

    def recorded_values(
        self,
        tags: list[str],
        start: PITimestamp,
        end: PITimestamp,
        max_count: int = 0,
        parallel: bool = True,
    ) -> dict[str, list[PIValue]]:
        """Get recorded values for multiple tags.

        Args:
            tags: List of PI Point names
            start: Start time
            end: End time
            max_count: Maximum values per tag (0 = no limit)
            parallel: Use parallel extraction

        Returns:
            Dictionary mapping tag names to lists of PIValues
        """
        if parallel and len(tags) > 1:
            return self._parallel_recorded_values(tags, start, end, max_count)
        else:
            return self._bulk_recorded_values(tags, start, end, max_count)

    def _bulk_recorded_values(
        self,
        tags: list[str],
        start: PITimestamp,
        end: PITimestamp,
        max_count: int = 0,
    ) -> dict[str, list[PIValue]]:
        """Get recorded values using the bulk API.

        Args:
            tags: List of PI Point names
            start: Start time
            end: End time
            max_count: Maximum values per tag

        Returns:
            Dictionary mapping tag names to lists of PIValues
        """
        PIPointList = self._sdk.pi_point_list_class
        point_list = PIPointList()

        for tag in tags:
            point = self._connection.get_point(tag)
            point_list.Add(point)

        time_range = self._create_time_range(start, end)
        AFBoundaryType = self._sdk.get_type("OSIsoft.AF.Data", "AFBoundaryType")

        # Use bulk RecordedValues
        PIPagingConfiguration = self._sdk.get_type(
            "OSIsoft.AF.PI", "PIPagingConfiguration"
        )
        paging_config = PIPagingConfiguration(
            PIPagingConfiguration.PageType.TagCount,
            len(tags),
        )

        bulk_results = point_list.RecordedValues(
            time_range,
            AFBoundaryType.Inside,
            None,  # filter expression
            False,  # include filtered
            paging_config,
        )

        result = {}
        for i, tag in enumerate(tags):
            values = []
            try:
                for af_value in bulk_results[i]:
                    values.append(self._convert_value(af_value))
            except Exception as e:
                logger.warning(f"Error processing values for {tag}: {e}")

            if max_count > 0:
                values = values[:max_count]

            result[tag] = values

        return result

    def _parallel_recorded_values(
        self,
        tags: list[str],
        start: PITimestamp,
        end: PITimestamp,
        max_count: int = 0,
    ) -> dict[str, list[PIValue]]:
        """Get recorded values using parallel operations.

        Args:
            tags: List of PI Point names
            start: Start time
            end: End time
            max_count: Maximum values per tag

        Returns:
            Dictionary mapping tag names to lists of PIValues
        """
        result = {}
        errors = {}

        def fetch_tag(tag: str) -> BulkResult:
            try:
                point = self._connection.get_point(tag)
                time_range = self._create_time_range(start, end)
                AFBoundaryType = self._sdk.get_type("OSIsoft.AF.Data", "AFBoundaryType")

                af_values = point.RecordedValues(
                    time_range,
                    AFBoundaryType.Inside,
                    None,
                    False,
                    max_count,
                )

                values = [self._convert_value(v) for v in af_values]
                return BulkResult(tag=tag, values=values, success=True)

            except Exception as e:
                return BulkResult(
                    tag=tag, values=[], success=False, error=str(e)
                )

        with ThreadPoolExecutor(max_workers=self._max_parallel) as executor:
            futures = {executor.submit(fetch_tag, tag): tag for tag in tags}

            for future in as_completed(futures):
                bulk_result = future.result()
                if bulk_result.success:
                    result[bulk_result.tag] = bulk_result.values
                else:
                    result[bulk_result.tag] = []
                    errors[bulk_result.tag] = bulk_result.error

        if errors:
            logger.warning(f"Errors occurred for {len(errors)} tags: {errors}")

        return result

    def interpolated_values(
        self,
        tags: list[str],
        start: PITimestamp,
        end: PITimestamp,
        interval: str = "1h",
        parallel: bool = True,
    ) -> dict[str, list[PIValue]]:
        """Get interpolated values for multiple tags.

        Args:
            tags: List of PI Point names
            start: Start time
            end: End time
            interval: Time interval
            parallel: Use parallel extraction

        Returns:
            Dictionary mapping tag names to lists of PIValues
        """
        result = {}

        def fetch_tag(tag: str) -> BulkResult:
            try:
                point = self._connection.get_point(tag)
                time_range = self._create_time_range(start, end)

                AFTimeSpan = self._sdk.get_type("OSIsoft.AF.Time", "AFTimeSpan")
                time_interval = AFTimeSpan.Parse(interval)

                af_values = point.InterpolatedValues(
                    time_range,
                    time_interval,
                    None,
                    False,
                )

                values = [self._convert_value(v) for v in af_values]
                return BulkResult(tag=tag, values=values, success=True)

            except Exception as e:
                return BulkResult(
                    tag=tag, values=[], success=False, error=str(e)
                )

        if parallel and len(tags) > 1:
            with ThreadPoolExecutor(max_workers=self._max_parallel) as executor:
                futures = {executor.submit(fetch_tag, tag): tag for tag in tags}

                for future in as_completed(futures):
                    bulk_result = future.result()
                    result[bulk_result.tag] = bulk_result.values
        else:
            for tag in tags:
                bulk_result = fetch_tag(tag)
                result[bulk_result.tag] = bulk_result.values

        return result

    def summaries(
        self,
        tags: list[str],
        start: PITimestamp,
        end: PITimestamp,
        summary_types: SummaryType | list[SummaryType] = SummaryType.AVERAGE,
        parallel: bool = True,
    ) -> dict[str, dict[str, Any]]:
        """Get summary values for multiple tags.

        Args:
            tags: List of PI Point names
            start: Start time
            end: End time
            summary_types: Summary type(s) to calculate
            parallel: Use parallel extraction

        Returns:
            Dictionary mapping tag names to summary dictionaries
        """
        result = {}

        def fetch_summary(tag: str) -> BulkSummaryResult:
            try:
                point = self._connection.get_point(tag)
                time_range = self._create_time_range(start, end)

                AFSummaryTypes = self._sdk.get_type("OSIsoft.AF.Data", "AFSummaryTypes")
                AFCalculationBasis = self._sdk.get_type(
                    "OSIsoft.AF.Data", "AFCalculationBasis"
                )
                AFTimestampCalculation = self._sdk.get_type(
                    "OSIsoft.AF.Data", "AFTimestampCalculation"
                )

                if isinstance(summary_types, list):
                    # Start with first type, then OR the rest
                    sdk_summary = AFSummaryTypes(summary_types[0].value)
                    for st in summary_types[1:]:
                        sdk_summary |= AFSummaryTypes(st.value)
                else:
                    sdk_summary = AFSummaryTypes(summary_types.value)

                summaries_result = point.Summary(
                    time_range,
                    sdk_summary,
                    AFCalculationBasis.TimeWeighted,
                    AFTimestampCalculation.Auto,
                )

                summary_dict = {}
                summary_name_map = {
                    1: "total",
                    2: "average",
                    4: "minimum",
                    8: "maximum",
                    16: "range",
                    32: "std_dev",
                    128: "count",
                    8192: "percent_good",
                }

                # PI SDK returns IDictionary<AFSummaryTypes, AFValue>
                for key in summaries_result.Keys:
                    summary_type_value = int(key)
                    name = summary_name_map.get(summary_type_value, str(summary_type_value))
                    af_value = summaries_result[key]
                    value = af_value.Value
                    if hasattr(value, "Value"):
                        value = value.Value
                    try:
                        summary_dict[name] = float(value)
                    except (ValueError, TypeError):
                        summary_dict[name] = value

                return BulkSummaryResult(
                    tag=tag, summaries=summary_dict, success=True
                )

            except Exception as e:
                return BulkSummaryResult(
                    tag=tag, summaries={}, success=False, error=str(e)
                )

        if parallel and len(tags) > 1:
            with ThreadPoolExecutor(max_workers=self._max_parallel) as executor:
                futures = {executor.submit(fetch_summary, tag): tag for tag in tags}

                for future in as_completed(futures):
                    bulk_result = future.result()
                    result[bulk_result.tag] = bulk_result.summaries
        else:
            for tag in tags:
                bulk_result = fetch_summary(tag)
                result[bulk_result.tag] = bulk_result.summaries

        return result

    def recorded_values_chunked(
        self,
        tags: list[str],
        time_range: TimeRange,
        chunk_size: int = 100,
        callback: Callable[[str, list[PIValue]], None] | None = None,
    ) -> dict[str, list[PIValue]]:
        """Get recorded values in chunks for very large datasets.

        This method processes tags in chunks to manage memory usage
        for very large datasets.

        Args:
            tags: List of PI Point names
            time_range: Time range for data
            chunk_size: Number of tags per chunk
            callback: Optional callback for each chunk completion

        Returns:
            Dictionary mapping tag names to lists of PIValues
        """
        result = {}

        for i in range(0, len(tags), chunk_size):
            chunk = tags[i : i + chunk_size]
            chunk_result = self.recorded_values(
                chunk,
                time_range.start,
                time_range.end,
            )

            for tag, values in chunk_result.items():
                result[tag] = values
                if callback:
                    callback(tag, values)

            logger.info(
                f"Processed chunk {i // chunk_size + 1}/"
                f"{(len(tags) + chunk_size - 1) // chunk_size}"
            )

        return result

    def validate_tags(self, tags: list[str]) -> dict[str, bool]:
        """Validate that tags exist.

        Args:
            tags: List of PI Point names to validate

        Returns:
            Dictionary mapping tag names to existence status
        """
        result = {}

        for tag in tags:
            result[tag] = self._connection.point_exists(tag)

        return result
