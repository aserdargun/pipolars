"""AF Attribute data extraction.

This module provides functionality for extracting data from
AF Attributes in the PI Asset Framework.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.exceptions import PIDataError
from pipolars.core.types import AFTime, DataQuality, PITimestamp, PIValue, SummaryType

if TYPE_CHECKING:
    from pipolars.connection.af_database import AFDatabaseConnection

logger = logging.getLogger(__name__)


@dataclass
class AttributeInfo:
    """Information about an AF Attribute."""

    name: str
    path: str
    description: str
    uom: str
    type_name: str
    is_pi_point: bool
    pi_point_path: str | None
    default_value: Any


class AFAttributeExtractor:
    """Extracts data from AF Attributes.

    This class provides methods for retrieving time-series and
    configuration data from AF Attributes.

    Example:
        >>> extractor = AFAttributeExtractor(af_connection)
        >>> values = extractor.recorded_values(
        ...     "/Plant/Unit1|Temperature",
        ...     start="*-1d",
        ...     end="*"
        ... )
    """

    def __init__(self, connection: AFDatabaseConnection) -> None:
        """Initialize the extractor.

        Args:
            connection: Active AF Database connection
        """
        self._connection = connection
        self._sdk = get_sdk_manager()

    def _parse_attribute_path(self, path: str) -> tuple[str, str]:
        """Parse an attribute path into element and attribute parts.

        Args:
            path: Attribute path (e.g., "/Plant/Unit1|Temperature")

        Returns:
            Tuple of (element_path, attribute_name)
        """
        if "|" in path:
            element_path, attribute_name = path.rsplit("|", 1)
        else:
            raise PIDataError(
                f"Invalid attribute path: {path}. "
                "Expected format: /element/path|AttributeName"
            )
        return element_path, attribute_name

    def _get_attribute(self, path: str) -> Any:
        """Get an AF Attribute by path.

        Args:
            path: Attribute path

        Returns:
            AFAttribute object
        """
        element_path, attribute_name = self._parse_attribute_path(path)
        return self._connection.get_attribute(element_path, attribute_name)

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
        """Create an AFTimeRange from start and end times."""
        AFTimeRange = self._sdk.af_time_range_class
        start_time = self._parse_time(start)
        end_time = self._parse_time(end)
        return AFTimeRange(start_time, end_time)

    def _convert_value(self, af_value: Any) -> PIValue:
        """Convert an AFValue to PIValue."""
        timestamp = af_value.Timestamp.LocalTime

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

    def get_attribute_info(self, path: str) -> AttributeInfo:
        """Get information about an AF Attribute.

        Args:
            path: Attribute path

        Returns:
            AttributeInfo with attribute metadata
        """
        attribute = self._get_attribute(path)

        pi_point_path = None
        if hasattr(attribute, "PIPoint") and attribute.PIPoint:
            pi_point_path = str(attribute.PIPoint.GetPath())

        return AttributeInfo(
            name=str(attribute.Name),
            path=str(attribute.GetPath()),
            description=str(attribute.Description or ""),
            uom=str(attribute.DisplayUOM) if attribute.DisplayUOM else "",
            type_name=str(attribute.Type.Name) if attribute.Type else "Unknown",
            is_pi_point=attribute.DataReferencePlugIn is not None
            and "PI Point" in str(attribute.DataReferencePlugIn.Name),
            pi_point_path=pi_point_path,
            default_value=attribute.DefaultValue,
        )

    def get_value(self, path: str) -> PIValue:
        """Get the current value of an AF Attribute.

        Args:
            path: Attribute path

        Returns:
            Current PIValue
        """
        attribute = self._get_attribute(path)
        af_value = attribute.GetValue()
        return self._convert_value(af_value)

    def get_values(self, paths: list[str]) -> dict[str, PIValue]:
        """Get current values for multiple AF Attributes.

        Args:
            paths: List of attribute paths

        Returns:
            Dictionary mapping paths to PIValues
        """
        result = {}
        for path in paths:
            result[path] = self.get_value(path)
        return result

    def recorded_values(
        self,
        path: str,
        start: PITimestamp,
        end: PITimestamp,
        max_count: int = 0,
    ) -> list[PIValue]:
        """Get recorded values for an AF Attribute.

        Args:
            path: Attribute path
            start: Start time
            end: End time
            max_count: Maximum number of values (0 = no limit)

        Returns:
            List of PIValue objects
        """
        attribute = self._get_attribute(path)
        time_range = self._create_time_range(start, end)

        AFBoundaryType = self._sdk.get_type("OSIsoft.AF.Data", "AFBoundaryType")

        af_values = attribute.Data.RecordedValues(
            time_range,
            AFBoundaryType.Inside,
            None,  # uom
            None,  # filter expression
            False,  # include filtered values
            max_count,
        )

        return [self._convert_value(v) for v in af_values]

    def interpolated_values(
        self,
        path: str,
        start: PITimestamp,
        end: PITimestamp,
        interval: str = "1h",
    ) -> list[PIValue]:
        """Get interpolated values for an AF Attribute.

        Args:
            path: Attribute path
            start: Start time
            end: End time
            interval: Time interval

        Returns:
            List of PIValue objects at regular intervals
        """
        attribute = self._get_attribute(path)
        time_range = self._create_time_range(start, end)

        AFTimeSpan = self._sdk.get_type("OSIsoft.AF.Time", "AFTimeSpan")
        time_interval = AFTimeSpan.Parse(interval)

        af_values = attribute.Data.InterpolatedValues(
            time_range,
            time_interval,
            None,  # uom
            None,  # filter expression
            False,  # include filtered values
        )

        return [self._convert_value(v) for v in af_values]

    def summary(
        self,
        path: str,
        start: PITimestamp,
        end: PITimestamp,
        summary_types: SummaryType | list[SummaryType] = SummaryType.AVERAGE,
    ) -> dict[str, Any]:
        """Get summary values for an AF Attribute.

        Args:
            path: Attribute path
            start: Start time
            end: End time
            summary_types: Summary type(s) to calculate

        Returns:
            Dictionary with summary values
        """
        attribute = self._get_attribute(path)
        time_range = self._create_time_range(start, end)

        AFSummaryTypes = self._sdk.get_type("OSIsoft.AF.Data", "AFSummaryTypes")
        AFCalculationBasis = self._sdk.get_type("OSIsoft.AF.Data", "AFCalculationBasis")
        AFTimestampCalculation = self._sdk.get_type(
            "OSIsoft.AF.Data", "AFTimestampCalculation"
        )

        if isinstance(summary_types, list):
            sdk_summary = AFSummaryTypes.None_
            for st in summary_types:
                sdk_summary |= AFSummaryTypes(st.value)
        else:
            sdk_summary = AFSummaryTypes(summary_types.value)

        summaries = attribute.Data.Summary(
            time_range,
            sdk_summary,
            AFCalculationBasis.TimeWeighted,
            AFTimestampCalculation.Auto,
        )

        result = {}
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

        for summary in summaries:
            summary_type_value = int(summary.SummaryType)
            name = summary_name_map.get(summary_type_value, str(summary_type_value))
            result[name] = summary.Value.Value

        return result

    def get_element_attributes(
        self,
        element_path: str,
        recursive: bool = False,
    ) -> list[AttributeInfo]:
        """Get all attributes for an element.

        Args:
            element_path: Path to the AF Element
            recursive: Include attributes from child elements

        Returns:
            List of AttributeInfo objects
        """
        element = self._connection.get_element(element_path)
        attributes = element.Attributes

        result = []
        for attr in attributes:
            pi_point_path = None
            if hasattr(attr, "PIPoint") and attr.PIPoint:
                pi_point_path = str(attr.PIPoint.GetPath())

            result.append(
                AttributeInfo(
                    name=str(attr.Name),
                    path=str(attr.GetPath()),
                    description=str(attr.Description or ""),
                    uom=str(attr.DisplayUOM) if attr.DisplayUOM else "",
                    type_name=str(attr.Type.Name) if attr.Type else "Unknown",
                    is_pi_point=attr.DataReferencePlugIn is not None
                    and "PI Point" in str(attr.DataReferencePlugIn.Name),
                    pi_point_path=pi_point_path,
                    default_value=attr.DefaultValue,
                )
            )

        if recursive:
            for child in element.Elements:
                child_attrs = self.get_element_attributes(
                    child.GetPath(), recursive=True
                )
                result.extend(child_attrs)

        return result
