"""Event Frame data extraction.

This module provides functionality for extracting Event Frame data
from PI Asset Framework.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.exceptions import PIDataError
from pipolars.core.types import AFTime, PITimestamp, PIValue

if TYPE_CHECKING:
    from pipolars.connection.af_database import AFDatabaseConnection

logger = logging.getLogger(__name__)


@dataclass
class EventFrameInfo:
    """Information about an Event Frame."""

    name: str
    id: str
    description: str
    template_name: str | None
    start_time: datetime | None
    end_time: datetime | None
    duration: float | None  # Duration in seconds
    is_acknowledged: bool
    severity: str
    primary_element_path: str | None
    categories: list[str]
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventFrameSearchCriteria:
    """Criteria for searching Event Frames."""

    name_filter: str = "*"
    template_name: str | None = None
    element_path: str | None = None
    start_time: PITimestamp | None = None
    end_time: PITimestamp | None = None
    in_progress_only: bool = False
    severity: str | None = None
    categories: list[str] | None = None


class EventFrameExtractor:
    """Extracts Event Frame data from AF.

    Event Frames are time-based events that capture data during
    specific periods, such as batch operations, alarms, or
    production cycles.

    Example:
        >>> extractor = EventFrameExtractor(af_connection)
        >>> events = extractor.search(
        ...     start_time="*-7d",
        ...     end_time="*",
        ...     template_name="BatchRun"
        ... )
        >>> for event in events:
        ...     print(f"{event.name}: {event.start_time} - {event.end_time}")
    """

    def __init__(self, connection: AFDatabaseConnection) -> None:
        """Initialize the extractor.

        Args:
            connection: Active AF Database connection
        """
        self._connection = connection
        self._sdk = get_sdk_manager()

    def _parse_time(self, time: PITimestamp) -> Any:
        """Convert a timestamp to AFTime."""
        AFTime_class = self._sdk.af_time_class

        if isinstance(time, datetime):
            return AFTime_class(time.isoformat())
        elif isinstance(time, AFTime):
            return AFTime_class(time.expression)
        else:
            return AFTime_class(str(time))

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

    def _convert_event_frame(
        self,
        af_event: Any,
        include_attributes: bool = False,
    ) -> EventFrameInfo:
        """Convert an AFEventFrame to EventFrameInfo.

        Args:
            af_event: The AFEventFrame from the SDK
            include_attributes: Whether to include attribute values

        Returns:
            EventFrameInfo object
        """
        template_name = None
        if af_event.Template:
            template_name = str(af_event.Template.Name)

        primary_element_path = None
        if af_event.PrimaryReferencedElement:
            primary_element_path = str(af_event.PrimaryReferencedElement.GetPath())

        categories = []
        if af_event.Categories:
            categories = [str(c.Name) for c in af_event.Categories]

        # Calculate duration and convert timestamps
        duration = None
        end_time = None
        start_time = None

        if af_event.StartTime:
            start_time = self._convert_net_datetime(af_event.StartTime.LocalTime)

        if af_event.EndTime:
            end_time = self._convert_net_datetime(af_event.EndTime.LocalTime)
            if start_time:
                delta = end_time - start_time
                duration = delta.total_seconds()

        # Get attribute values if requested
        attributes = {}
        if include_attributes and af_event.Attributes:
            for attr in af_event.Attributes:
                try:
                    value = attr.GetValue()
                    if value:
                        attributes[str(attr.Name)] = value.Value
                except Exception:
                    pass

        return EventFrameInfo(
            name=str(af_event.Name),
            id=str(af_event.ID),
            description=str(af_event.Description or ""),
            template_name=template_name,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            is_acknowledged=bool(af_event.IsAcknowledged),
            severity=str(af_event.Severity) if af_event.Severity else "None",
            primary_element_path=primary_element_path,
            categories=categories,
            attributes=attributes,
        )

    def get_event_frame(
        self,
        event_id: str,
        include_attributes: bool = True,
    ) -> EventFrameInfo:
        """Get an Event Frame by ID.

        Args:
            event_id: The Event Frame GUID
            include_attributes: Include attribute values

        Returns:
            EventFrameInfo object
        """
        database = self._connection.database

        # Find event frame by ID
        AFEventFrame = self._sdk.get_type("OSIsoft.AF.EventFrame", "AFEventFrame")
        System_Guid = self._sdk.get_type("System", "Guid")

        guid = System_Guid.Parse(event_id)
        af_event = AFEventFrame.FindEventFrame(database.PISystem, guid)

        if af_event is None:
            raise PIDataError(f"Event Frame not found: {event_id}")

        return self._convert_event_frame(af_event, include_attributes)

    def search(
        self,
        criteria: EventFrameSearchCriteria | None = None,
        start_time: PITimestamp | None = None,
        end_time: PITimestamp | None = None,
        template_name: str | None = None,
        element_path: str | None = None,
        name_filter: str = "*",
        max_count: int = 1000,
        include_attributes: bool = False,
    ) -> list[EventFrameInfo]:
        """Search for Event Frames.

        Args:
            criteria: Search criteria object (alternative to individual params)
            start_time: Search start time
            end_time: Search end time
            template_name: Filter by template
            element_path: Filter by element
            name_filter: Name pattern filter
            max_count: Maximum results
            include_attributes: Include attribute values

        Returns:
            List of EventFrameInfo objects
        """
        # Use criteria object if provided
        if criteria:
            start_time = criteria.start_time
            end_time = criteria.end_time
            template_name = criteria.template_name
            element_path = criteria.element_path
            name_filter = criteria.name_filter

        database = self._connection.database

        # Get AF SDK types
        AFEventFrameSearch = self._sdk.get_type(
            "OSIsoft.AF.Search", "AFEventFrameSearch"
        )
        AFSearchField = self._sdk.get_type("OSIsoft.AF.Search", "AFSearchField")
        AFSearchToken = self._sdk.get_type("OSIsoft.AF.Search", "AFSearchToken")

        # Build search tokens
        tokens = []

        if name_filter and name_filter != "*":
            tokens.append(AFSearchToken(AFSearchField.Name, "*", name_filter))

        if template_name:
            tokens.append(AFSearchToken(AFSearchField.Template, "=", template_name))

        if start_time and end_time:
            tokens.append(AFSearchToken(AFSearchField.Start, ">=", str(start_time)))
            tokens.append(AFSearchToken(AFSearchField.End, "<=", str(end_time)))

        # Create search
        search = AFEventFrameSearch(database, "EventSearch", tokens)
        search.MaxCount = max_count

        # Execute search
        results = list(search.FindEventFrames())

        # Filter by element if specified
        if element_path:
            filtered = []
            for ef in results:
                if ef.PrimaryReferencedElement:
                    if str(ef.PrimaryReferencedElement.GetPath()).startswith(element_path):
                        filtered.append(ef)
            results = filtered

        return [
            self._convert_event_frame(ef, include_attributes) for ef in results[:max_count]
        ]

    def _create_time_range(self, start: PITimestamp, end: PITimestamp) -> Any:
        """Create an AFTimeRange."""
        AFTimeRange = self._sdk.af_time_range_class
        start_time = self._parse_time(start)
        end_time = self._parse_time(end)
        return AFTimeRange(start_time, end_time)

    def get_event_data(
        self,
        event_id: str,
        attribute_names: list[str] | None = None,
    ) -> dict[str, list[PIValue]]:
        """Get time-series data captured during an Event Frame.

        Args:
            event_id: The Event Frame ID
            attribute_names: Specific attributes to retrieve (None for all)

        Returns:
            Dictionary mapping attribute names to lists of PIValue
        """
        event = self.get_event_frame(event_id)
        database = self._connection.database

        # Get the actual AFEventFrame
        AFEventFrame = self._sdk.get_type("OSIsoft.AF.EventFrame", "AFEventFrame")
        System_Guid = self._sdk.get_type("System", "Guid")

        guid = System_Guid.Parse(event_id)
        af_event = AFEventFrame.FindEventFrame(database.PISystem, guid)

        if af_event is None:
            raise PIDataError(f"Event Frame not found: {event_id}")

        result = {}
        time_range = self._create_time_range(event.start_time or "*", event.end_time or "*")

        for attr in af_event.Attributes:
            attr_name = str(attr.Name)

            if attribute_names and attr_name not in attribute_names:
                continue

            try:
                AFBoundaryType = self._sdk.get_type("OSIsoft.AF.Data", "AFBoundaryType")
                af_values = attr.Data.RecordedValues(
                    time_range,
                    AFBoundaryType.Inside,
                    None,
                    None,
                    False,
                    0,
                )

                values = []
                for af_value in af_values:
                    values.append(
                        PIValue(
                            timestamp=self._convert_net_datetime(af_value.Timestamp.LocalTime),
                            value=af_value.Value,
                        )
                    )

                result[attr_name] = values

            except Exception as e:
                logger.warning(f"Failed to get data for attribute {attr_name}: {e}")

        return result

    def get_child_event_frames(
        self,
        parent_id: str,
        include_attributes: bool = False,
    ) -> list[EventFrameInfo]:
        """Get child Event Frames of a parent Event Frame.

        Args:
            parent_id: Parent Event Frame ID
            include_attributes: Include attribute values

        Returns:
            List of child EventFrameInfo objects
        """
        database = self._connection.database

        AFEventFrame = self._sdk.get_type("OSIsoft.AF.EventFrame", "AFEventFrame")
        System_Guid = self._sdk.get_type("System", "Guid")

        guid = System_Guid.Parse(parent_id)
        parent = AFEventFrame.FindEventFrame(database.PISystem, guid)

        if parent is None:
            raise PIDataError(f"Parent Event Frame not found: {parent_id}")

        children = []
        for child in parent.EventFrames:
            children.append(self._convert_event_frame(child, include_attributes))

        return children

    def get_event_frames_by_element(
        self,
        element_path: str,
        start_time: PITimestamp | None = None,
        end_time: PITimestamp | None = None,
        max_count: int = 100,
        include_attributes: bool = False,
    ) -> list[EventFrameInfo]:
        """Get Event Frames associated with an element.

        Args:
            element_path: Path to the AF Element
            start_time: Optional start time filter
            end_time: Optional end time filter
            max_count: Maximum results
            include_attributes: Include attribute values

        Returns:
            List of EventFrameInfo objects
        """
        return self.search(
            element_path=element_path,
            start_time=start_time,
            end_time=end_time,
            max_count=max_count,
            include_attributes=include_attributes,
        )
