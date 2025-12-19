"""AF Database connection management.

This module provides connection management for PI Asset Framework (AF)
databases, enabling access to AF Elements, Attributes, and Event Frames.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.config import AFServerConfig
from pipolars.core.exceptions import PIConnectionError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class AFDatabaseConnection:
    """Manages connection to an AF Database.

    This class handles:
    - AF Server discovery and connection
    - AF Database access
    - AF Element and Attribute navigation

    Example:
        >>> config = AFServerConfig(host="my-af-server", database="MyDatabase")
        >>> with AFDatabaseConnection(config) as conn:
        ...     elements = conn.get_elements("/Plant/Unit1")
        ...     for element in elements:
        ...         print(element.Name)
    """

    def __init__(self, config: AFServerConfig | str) -> None:
        """Initialize the AF Database connection.

        Args:
            config: AF Server configuration or connection string
        """
        if isinstance(config, str):
            # Parse connection string or treat as host
            if "\\" in config:
                # Format: "server\\database"
                parts = config.split("\\", 1)
                config = AFServerConfig(host=parts[0], database=parts[1])
            else:
                config = AFServerConfig(host=config)

        self._config = config
        self._sdk = get_sdk_manager()
        self._pi_system: Any = None
        self._database: Any = None
        self._connected = False

    @property
    def config(self) -> AFServerConfig:
        """Get the AF Server configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Check if connected to the AF Database."""
        return self._connected and self._database is not None

    @property
    def database(self) -> Any:
        """Get the underlying AFDatabase object.

        Raises:
            PIConnectionError: If not connected
        """
        if not self.is_connected:
            raise PIConnectionError("Not connected to AF Database")
        return self._database

    @property
    def pi_system(self) -> Any:
        """Get the underlying PISystem object.

        Raises:
            PIConnectionError: If not connected
        """
        if self._pi_system is None:
            raise PIConnectionError("Not connected to PI System")
        return self._pi_system

    def connect(self) -> None:
        """Establish connection to the AF Database.

        Raises:
            PIConnectionError: If connection fails
        """
        if self._connected:
            return

        try:
            # Initialize the SDK
            self._sdk.initialize()

            # Get PISystems collection
            PISystems = self._sdk.get_type("OSIsoft.AF", "PISystems")
            systems = PISystems()

            # Get the PI System (AF Server)
            if self._config.host:
                self._pi_system = systems[self._config.host]
            else:
                # Use default
                self._pi_system = systems.DefaultPISystem

            if self._pi_system is None:
                raise PIConnectionError(
                    f"PI System not found: {self._config.host or 'default'}"
                )

            # Connect to the PI System
            self._pi_system.Connect()

            # Get the database
            if self._config.database:
                self._database = self._pi_system.Databases[self._config.database]
            else:
                # Use default database
                self._database = self._pi_system.Databases.DefaultDatabase

            if self._database is None:
                raise PIConnectionError(
                    f"AF Database not found: {self._config.database or 'default'}"
                )

            self._connected = True
            logger.info(
                f"Connected to AF Database: {self._database.Name} "
                f"on {self._pi_system.Name}"
            )

        except PIConnectionError:
            raise
        except Exception as e:
            raise PIConnectionError(
                f"Failed to connect to AF Database: {e}",
                details={"error_type": type(e).__name__},
            ) from e

    def disconnect(self) -> None:
        """Disconnect from the AF Database."""
        if self._pi_system is not None and self._connected:
            try:
                self._pi_system.Disconnect()
                logger.info("Disconnected from PI System")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self._pi_system = None
                self._database = None
                self._connected = False

    def __enter__(self) -> AFDatabaseConnection:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()

    def get_element(self, path: str) -> Any:
        """Get an AF Element by path.

        Args:
            path: The element path (e.g., "/Plant/Unit1/Reactor")

        Returns:
            The AFElement object

        Raises:
            PIConnectionError: If element not found or not connected
        """
        if not self.is_connected:
            raise PIConnectionError("Not connected to AF Database")

        try:
            element = self._database.Elements[path]
            if element is None:
                # Try finding by path
                element = self._database.FindElementsByPath(path)
                if element and len(element) > 0:
                    return element[0]
                raise PIConnectionError(f"AF Element not found: {path}")
            return element
        except PIConnectionError:
            raise
        except Exception as e:
            raise PIConnectionError(
                f"Failed to get AF Element: {path}",
                details={"error": str(e)},
            ) from e

    def get_elements(
        self,
        path: str = "",
        recursive: bool = False,
        max_count: int = 1000,
    ) -> list[Any]:
        """Get AF Elements under a path.

        Args:
            path: The parent element path (empty for root)
            recursive: Whether to search recursively
            max_count: Maximum number of elements to return

        Returns:
            List of AFElement objects
        """
        if not self.is_connected:
            raise PIConnectionError("Not connected to AF Database")

        try:
            if path:
                parent = self.get_element(path)
                elements = parent.Elements
            else:
                elements = self._database.Elements

            result = []
            for element in elements:
                if len(result) >= max_count:
                    break
                result.append(element)

                if recursive:
                    # Recursively get child elements
                    child_elements = self.get_elements(
                        element.GetPath(),
                        recursive=True,
                        max_count=max_count - len(result),
                    )
                    result.extend(child_elements)

            return result[:max_count]

        except PIConnectionError:
            raise
        except Exception as e:
            raise PIConnectionError(
                f"Failed to get AF Elements: {e}",
                details={"path": path},
            ) from e

    def get_attribute(self, element_path: str, attribute_name: str) -> Any:
        """Get an AF Attribute by element path and name.

        Args:
            element_path: Path to the parent element
            attribute_name: Name of the attribute

        Returns:
            The AFAttribute object
        """
        element = self.get_element(element_path)
        attribute = element.Attributes[attribute_name]

        if attribute is None:
            raise PIConnectionError(
                f"AF Attribute not found: {attribute_name}",
                details={"element": element_path},
            )

        return attribute

    def search_elements(
        self,
        query: str,
        search_root: str = "",
        max_count: int = 1000,
    ) -> list[Any]:
        """Search for AF Elements by name pattern.

        Args:
            query: Search pattern (supports wildcards)
            search_root: Root path for search
            max_count: Maximum results to return

        Returns:
            List of matching AFElement objects
        """
        if not self.is_connected:
            raise PIConnectionError("Not connected to AF Database")

        try:
            AFSearchField = self._sdk.get_type("OSIsoft.AF.Search", "AFSearchField")
            AFSearchToken = self._sdk.get_type("OSIsoft.AF.Search", "AFSearchToken")
            AFElementSearch = self._sdk.get_type("OSIsoft.AF.Search", "AFElementSearch")

            # Create search
            tokens = [AFSearchToken(AFSearchField.Name, "*", query)]
            search = AFElementSearch(self._database, "ElementSearch", tokens)
            search.MaxCount = max_count

            # Execute search
            results = list(search.FindElements())
            return results[:max_count]

        except Exception as e:
            raise PIConnectionError(
                f"Failed to search AF Elements: {e}",
                details={"query": query},
            ) from e

    @classmethod
    def list_databases(cls, host: str | None = None) -> list[str]:
        """List all AF Databases on a PI System.

        Args:
            host: The PI System hostname (None for default)

        Returns:
            List of database names
        """
        sdk = get_sdk_manager()
        sdk.initialize()

        PISystems = sdk.get_type("OSIsoft.AF", "PISystems")
        systems = PISystems()

        if host:
            pi_system = systems[host]
        else:
            pi_system = systems.DefaultPISystem

        if pi_system is None:
            return []

        return [str(db.Name) for db in pi_system.Databases]
