"""AF Element data extraction.

This module provides functionality for navigating and extracting
data from AF Elements in the PI Asset Framework.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.exceptions import PIDataError

if TYPE_CHECKING:
    from pipolars.connection.af_database import AFDatabaseConnection

logger = logging.getLogger(__name__)


@dataclass
class ElementInfo:
    """Information about an AF Element."""

    name: str
    path: str
    description: str
    template_name: str | None
    categories: list[str]
    attributes: list[str]
    child_count: int


@dataclass
class ElementTree:
    """Hierarchical representation of AF Elements."""

    element: ElementInfo
    children: list[ElementTree] = field(default_factory=list)


class AFElementExtractor:
    """Extracts element hierarchy and metadata from AF.

    This class provides methods for navigating the AF Element
    hierarchy and extracting element metadata.

    Example:
        >>> extractor = AFElementExtractor(af_connection)
        >>> elements = extractor.get_elements("/Plant")
        >>> for elem in elements:
        ...     print(f"{elem.name}: {elem.template_name}")
    """

    def __init__(self, connection: AFDatabaseConnection) -> None:
        """Initialize the extractor.

        Args:
            connection: Active AF Database connection
        """
        self._connection = connection
        self._sdk = get_sdk_manager()

    def _convert_element(self, af_element: Any) -> ElementInfo:
        """Convert an AFElement to ElementInfo.

        Args:
            af_element: The AFElement from the SDK

        Returns:
            ElementInfo object
        """
        template_name = None
        if af_element.Template:
            template_name = str(af_element.Template.Name)

        categories = []
        if af_element.Categories:
            categories = [str(c.Name) for c in af_element.Categories]

        attributes = []
        if af_element.Attributes:
            attributes = [str(a.Name) for a in af_element.Attributes]

        return ElementInfo(
            name=str(af_element.Name),
            path=str(af_element.GetPath()),
            description=str(af_element.Description or ""),
            template_name=template_name,
            categories=categories,
            attributes=attributes,
            child_count=af_element.Elements.Count if af_element.Elements else 0,
        )

    def get_element(self, path: str) -> ElementInfo:
        """Get an AF Element by path.

        Args:
            path: The element path

        Returns:
            ElementInfo for the element
        """
        af_element = self._connection.get_element(path)
        return self._convert_element(af_element)

    def get_elements(
        self,
        path: str = "",
        max_count: int = 1000,
    ) -> list[ElementInfo]:
        """Get child elements under a path.

        Args:
            path: Parent element path (empty for root)
            max_count: Maximum number of elements

        Returns:
            List of ElementInfo objects
        """
        af_elements = self._connection.get_elements(path, recursive=False, max_count=max_count)
        return [self._convert_element(e) for e in af_elements]

    def get_element_tree(
        self,
        path: str = "",
        max_depth: int = 3,
    ) -> list[ElementTree]:
        """Get a tree of elements starting from a path.

        Args:
            path: Root element path
            max_depth: Maximum depth to traverse

        Returns:
            List of ElementTree objects
        """
        if max_depth <= 0:
            return []

        elements = self.get_elements(path)
        trees = []

        for element in elements:
            children = []
            if max_depth > 1 and element.child_count > 0:
                children = self.get_element_tree(element.path, max_depth - 1)

            trees.append(ElementTree(element=element, children=children))

        return trees

    def search_elements(
        self,
        query: str,
        template: str | None = None,
        category: str | None = None,
        max_count: int = 1000,
    ) -> list[ElementInfo]:
        """Search for elements by name pattern.

        Args:
            query: Name pattern (supports wildcards)
            template: Optional template filter
            category: Optional category filter
            max_count: Maximum results

        Returns:
            List of matching ElementInfo objects
        """
        af_elements = self._connection.search_elements(query, max_count=max_count)
        results = []

        for af_element in af_elements:
            # Apply filters
            if template and af_element.Template:
                if str(af_element.Template.Name) != template:
                    continue

            if category and af_element.Categories:
                category_names = [str(c.Name) for c in af_element.Categories]
                if category not in category_names:
                    continue

            results.append(self._convert_element(af_element))

        return results

    def get_elements_by_template(
        self,
        template_name: str,
        root_path: str = "",
        max_count: int = 1000,
    ) -> list[ElementInfo]:
        """Get elements that use a specific template.

        Args:
            template_name: The template name to filter by
            root_path: Root path to search from
            max_count: Maximum results

        Returns:
            List of ElementInfo objects
        """
        # Get all elements and filter by template
        af_elements = self._connection.get_elements(
            root_path, recursive=True, max_count=max_count * 2
        )

        results = []
        for af_element in af_elements:
            if af_element.Template and str(af_element.Template.Name) == template_name:
                results.append(self._convert_element(af_element))
                if len(results) >= max_count:
                    break

        return results

    def get_element_path_to_root(self, path: str) -> list[ElementInfo]:
        """Get the path from an element to the root.

        Args:
            path: Element path

        Returns:
            List of ElementInfo from root to element
        """
        result = []
        current_path = path

        while current_path:
            try:
                element = self.get_element(current_path)
                result.insert(0, element)

                # Get parent path
                if "/" in current_path:
                    current_path = current_path.rsplit("/", 1)[0]
                    if not current_path:
                        break
                else:
                    break
            except Exception:
                break

        return result

    def get_sibling_elements(self, path: str) -> list[ElementInfo]:
        """Get sibling elements (elements at the same level).

        Args:
            path: Element path

        Returns:
            List of sibling ElementInfo objects
        """
        if "/" in path:
            parent_path = path.rsplit("/", 1)[0]
        else:
            parent_path = ""

        siblings = self.get_elements(parent_path)

        # Remove the current element from siblings
        element_name = path.rsplit("/", 1)[-1]
        return [e for e in siblings if e.name != element_name]

    def flatten_hierarchy(
        self,
        path: str = "",
        max_depth: int = 10,
    ) -> list[ElementInfo]:
        """Flatten the element hierarchy into a list.

        Args:
            path: Root path
            max_depth: Maximum depth to traverse

        Returns:
            Flat list of all ElementInfo objects
        """
        result = []

        def _flatten(current_path: str, depth: int) -> None:
            if depth > max_depth:
                return

            elements = self.get_elements(current_path)
            for element in elements:
                result.append(element)
                if element.child_count > 0:
                    _flatten(element.path, depth + 1)

        _flatten(path, 1)
        return result
