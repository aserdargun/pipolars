"""AF Analysis data extraction.

This module provides functionality for extracting AF Analysis metadata
from PI Asset Framework.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.types import AnalysisInfo, AnalysisStatus

if TYPE_CHECKING:
    from pipolars.connection.af_database import AFDatabaseConnection

logger = logging.getLogger(__name__)


@dataclass
class AnalysisSearchCriteria:
    """Criteria for searching AF Analyses."""

    name_filter: str = "*"
    """Name pattern to search for (supports wildcards)."""

    template_name: str | None = None
    """Optional template name filter."""

    element_path: str | None = None
    """Optional element path filter."""

    status: AnalysisStatus | None = None
    """Optional status filter."""

    enabled_only: bool = False
    """Only return enabled analyses."""


class AFAnalysisExtractor:
    """Extracts AF Analysis metadata from AF.

    AF Analyses are calculations and logic defined in the Asset Framework
    that run on a schedule or triggered basis, producing output values
    that can be stored in PI Points or AF Attributes.

    Example:
        >>> extractor = AFAnalysisExtractor(af_connection)
        >>> analyses = extractor.get_analyses("/Plant/Unit1")
        >>> for analysis in analyses:
        ...     print(f"{analysis.name}: {analysis.status}")
    """

    def __init__(self, connection: AFDatabaseConnection) -> None:
        """Initialize the extractor.

        Args:
            connection: Active AF Database connection
        """
        self._connection = connection
        self._sdk = get_sdk_manager()

    def _get_status(self, af_analysis: Any) -> AnalysisStatus:
        """Convert AF Analysis status to AnalysisStatus enum.

        Args:
            af_analysis: The AFAnalysis object

        Returns:
            AnalysisStatus enum value
        """
        try:
            if hasattr(af_analysis, "Status"):
                status_str = str(af_analysis.Status)
                if "Running" in status_str:
                    return AnalysisStatus.RUNNING
                elif "Stopped" in status_str:
                    return AnalysisStatus.STOPPED
                elif "Error" in status_str:
                    return AnalysisStatus.ERROR
        except Exception:
            pass
        return AnalysisStatus.UNKNOWN

    def _convert_analysis(self, af_analysis: Any) -> AnalysisInfo:
        """Convert an AFAnalysis to AnalysisInfo.

        Args:
            af_analysis: The AFAnalysis from the SDK

        Returns:
            AnalysisInfo object
        """
        # Target information
        target_id = ""
        target_name = ""
        target_path = ""
        if hasattr(af_analysis, "Target") and af_analysis.Target:
            target = af_analysis.Target
            target_id = str(target.ID) if hasattr(target, "ID") else ""
            target_name = str(target.Name) if hasattr(target, "Name") else ""
            if hasattr(target, "GetPath"):
                with contextlib.suppress(Exception):
                    target_path = str(target.GetPath())

        # Template information
        template_id = ""
        template_name = ""
        template_description = ""
        if hasattr(af_analysis, "Template") and af_analysis.Template:
            template = af_analysis.Template
            template_id = str(template.ID) if hasattr(template, "ID") else ""
            template_name = str(template.Name) if hasattr(template, "Name") else ""
            template_description = (
                str(template.Description) if hasattr(template, "Description") and template.Description else ""
            )

        # Categories
        categories: tuple[str, ...] = ()
        if hasattr(af_analysis, "Categories") and af_analysis.Categories:
            with contextlib.suppress(Exception):
                categories = tuple(str(c.Name) for c in af_analysis.Categories)

        # Time rule configuration
        time_rule_plugin_id = ""
        time_rule_config_string = ""
        if hasattr(af_analysis, "TimeRule") and af_analysis.TimeRule:
            time_rule = af_analysis.TimeRule
            if hasattr(time_rule, "PlugIn") and time_rule.PlugIn:
                time_rule_plugin_id = str(time_rule.PlugIn.ID) if hasattr(time_rule.PlugIn, "ID") else ""
            if hasattr(time_rule, "ConfigString"):
                time_rule_config_string = str(time_rule.ConfigString) if time_rule.ConfigString else ""

        # Analysis rule configuration
        analysis_rule_max_queue_size = None
        if hasattr(af_analysis, "AnalysisRule") and af_analysis.AnalysisRule:
            rule = af_analysis.AnalysisRule
            if hasattr(rule, "MaxQueueSize"):
                with contextlib.suppress(ValueError, TypeError):
                    analysis_rule_max_queue_size = int(rule.MaxQueueSize)

        # Group ID
        group_id = ""
        if hasattr(af_analysis, "GroupId"):
            with contextlib.suppress(Exception):
                group_id = str(af_analysis.GroupId) if af_analysis.GroupId else ""

        # Priority
        priority = None
        if hasattr(af_analysis, "Priority"):
            with contextlib.suppress(ValueError, TypeError):
                priority = int(af_analysis.Priority)

        # Maximum queue time
        maximum_queue_time = ""
        if hasattr(af_analysis, "MaximumQueueTime"):
            with contextlib.suppress(Exception):
                maximum_queue_time = str(af_analysis.MaximumQueueTime) if af_analysis.MaximumQueueTime else ""

        # Auto-created event frame count
        auto_created_event_frame_count = None
        if hasattr(af_analysis, "AutoCreatedEventFrameCount"):
            with contextlib.suppress(ValueError, TypeError):
                auto_created_event_frame_count = int(af_analysis.AutoCreatedEventFrameCount)

        # Output attributes
        output_attributes: tuple[str, ...] = ()
        if hasattr(af_analysis, "AnalysisRule") and af_analysis.AnalysisRule:
            rule = af_analysis.AnalysisRule
            if hasattr(rule, "GetOutputs"):
                try:
                    outputs = rule.GetOutputs()
                    if outputs:
                        output_attributes = tuple(str(o.Attribute.Name) for o in outputs if o.Attribute)
                except Exception:
                    pass

        # Is enabled
        is_enabled = False
        if hasattr(af_analysis, "IsEnabled"):
            with contextlib.suppress(Exception):
                is_enabled = bool(af_analysis.IsEnabled)

        # Get path
        path = ""
        if hasattr(af_analysis, "GetPath"):
            with contextlib.suppress(Exception):
                path = str(af_analysis.GetPath())

        return AnalysisInfo(
            name=str(af_analysis.Name),
            id=str(af_analysis.ID) if hasattr(af_analysis, "ID") else "",
            path=path,
            description=str(af_analysis.Description) if af_analysis.Description else "",
            target_id=target_id,
            target_name=target_name,
            target_path=target_path,
            template_id=template_id,
            template_name=template_name,
            template_description=template_description,
            status=self._get_status(af_analysis),
            is_enabled=is_enabled,
            categories=categories,
            time_rule_plugin_id=time_rule_plugin_id,
            time_rule_config_string=time_rule_config_string,
            analysis_rule_max_queue_size=analysis_rule_max_queue_size,
            group_id=group_id,
            priority=priority,
            maximum_queue_time=maximum_queue_time,
            auto_created_event_frame_count=auto_created_event_frame_count,
            output_attributes=output_attributes,
        )

    def get_analysis(self, analysis_id: str) -> AnalysisInfo:
        """Get an AF Analysis by ID.

        Args:
            analysis_id: The Analysis GUID

        Returns:
            AnalysisInfo object
        """
        database = self._connection.database

        # Find analysis by ID
        AFAnalysis = self._sdk.get_type("OSIsoft.AF.Analysis", "AFAnalysis")
        System_Guid = self._sdk.get_type("System", "Guid")

        guid = System_Guid.Parse(analysis_id)
        af_analysis = AFAnalysis.FindAnalysis(database.PISystem, guid)

        if af_analysis is None:
            from pipolars.core.exceptions import PIDataError
            raise PIDataError(f"Analysis not found: {analysis_id}")

        return self._convert_analysis(af_analysis)

    def get_analyses_for_element(
        self,
        element_path: str,
        recursive: bool = False,
    ) -> list[AnalysisInfo]:
        """Get all analyses for an element.

        Args:
            element_path: Path to the AF Element
            recursive: Include analyses from child elements

        Returns:
            List of AnalysisInfo objects
        """
        element = self._connection.get_element(element_path)
        analyses = element.Analyses

        result = []
        if analyses:
            for analysis in analyses:
                try:
                    result.append(self._convert_analysis(analysis))
                except Exception as e:
                    logger.warning(f"Failed to convert analysis: {e}")

        if recursive and element.Elements:
            for child in element.Elements:
                child_path = str(child.GetPath())
                child_analyses = self.get_analyses_for_element(child_path, recursive=True)
                result.extend(child_analyses)

        return result

    def search(
        self,
        criteria: AnalysisSearchCriteria | None = None,
        name_filter: str = "*",
        template_name: str | None = None,
        element_path: str | None = None,
        status: AnalysisStatus | None = None,
        enabled_only: bool = False,
        max_count: int = 1000,
    ) -> list[AnalysisInfo]:
        """Search for AF Analyses.

        Args:
            criteria: Search criteria object (alternative to individual params)
            name_filter: Name pattern filter (supports wildcards)
            template_name: Filter by template name
            element_path: Filter by element path
            status: Filter by status
            enabled_only: Only return enabled analyses
            max_count: Maximum results

        Returns:
            List of AnalysisInfo objects
        """
        # Use criteria object if provided
        if criteria:
            name_filter = criteria.name_filter
            template_name = criteria.template_name
            element_path = criteria.element_path
            status = criteria.status
            enabled_only = criteria.enabled_only

        database = self._connection.database

        # Get AF SDK types for search
        AFAnalysisSearch = self._sdk.get_type("OSIsoft.AF.Search", "AFAnalysisSearch")
        AFSearchField = self._sdk.get_type("OSIsoft.AF.Search", "AFSearchField")
        AFSearchToken = self._sdk.get_type("OSIsoft.AF.Search", "AFSearchToken")

        # Build search tokens
        tokens = []

        if name_filter and name_filter != "*":
            tokens.append(AFSearchToken(AFSearchField.Name, "*", name_filter))

        if template_name:
            tokens.append(AFSearchToken(AFSearchField.Template, "=", template_name))

        # Create search
        search = AFAnalysisSearch(database, "AnalysisSearch", tokens)
        search.MaxCount = max_count

        # Execute search
        results = list(search.FindAnalyses())

        # Apply additional filters
        filtered_results = []
        for af_analysis in results:
            # Filter by element path
            if element_path:
                if hasattr(af_analysis, "Target") and af_analysis.Target:
                    target_path = str(af_analysis.Target.GetPath())
                    if not target_path.startswith(element_path):
                        continue
                else:
                    continue

            # Filter by status
            if status:
                analysis_status = self._get_status(af_analysis)
                if analysis_status != status:
                    continue

            # Filter by enabled
            if enabled_only:
                if not (hasattr(af_analysis, "IsEnabled") and af_analysis.IsEnabled):
                    continue

            filtered_results.append(af_analysis)

            if len(filtered_results) >= max_count:
                break

        return [self._convert_analysis(a) for a in filtered_results]

    def get_analyses_by_template(
        self,
        template_name: str,
        max_count: int = 1000,
    ) -> list[AnalysisInfo]:
        """Get all analyses that use a specific template.

        Args:
            template_name: The template name to filter by
            max_count: Maximum results

        Returns:
            List of AnalysisInfo objects
        """
        return self.search(template_name=template_name, max_count=max_count)

    def get_running_analyses(
        self,
        element_path: str | None = None,
        max_count: int = 1000,
    ) -> list[AnalysisInfo]:
        """Get all running analyses.

        Args:
            element_path: Optional element path filter
            max_count: Maximum results

        Returns:
            List of AnalysisInfo objects with Running status
        """
        return self.search(
            element_path=element_path,
            status=AnalysisStatus.RUNNING,
            max_count=max_count,
        )

    def get_analyses_with_errors(
        self,
        element_path: str | None = None,
        max_count: int = 1000,
    ) -> list[AnalysisInfo]:
        """Get all analyses with errors.

        Args:
            element_path: Optional element path filter
            max_count: Maximum results

        Returns:
            List of AnalysisInfo objects with Error status
        """
        return self.search(
            element_path=element_path,
            status=AnalysisStatus.ERROR,
            max_count=max_count,
        )

    def get_all_analyses(
        self,
        max_count: int = 10000,
    ) -> list[AnalysisInfo]:
        """Get all analyses in the database.

        Args:
            max_count: Maximum results

        Returns:
            List of all AnalysisInfo objects
        """
        return self.search(name_filter="*", max_count=max_count)
