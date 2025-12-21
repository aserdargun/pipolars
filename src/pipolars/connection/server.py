"""PI Server connection management.

This module provides the connection interface to PI Data Archive servers,
handling connection lifecycle, authentication, and server discovery.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator, Iterator

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.config import PIServerConfig
from pipolars.core.exceptions import PIConnectionError, PIPointNotFoundError

logger = logging.getLogger(__name__)


class PIServerConnection:
    """Manages connection to a PI Data Archive server.

    This class handles:
    - Establishing and maintaining server connections
    - Server discovery and enumeration
    - PI Point access and caching
    - Connection pooling and lifecycle

    Example:
        >>> config = PIServerConfig(host="my-pi-server")
        >>> with PIServerConnection(config) as conn:
        ...     point = conn.get_point("SINUSOID")
        ...     values = point.recorded_values("*-1d", "*")
    """

    def __init__(self, config: PIServerConfig | str) -> None:
        """Initialize the PI Server connection.

        Args:
            config: Server configuration or hostname string
        """
        if isinstance(config, str):
            config = PIServerConfig(host=config)

        self._config = config
        self._sdk = get_sdk_manager()
        self._server: Any = None
        self._connected = False
        self._point_cache: dict[str, Any] = {}

    @property
    def config(self) -> PIServerConfig:
        """Get the server configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Check if connected to the server."""
        return self._connected and self._server is not None

    @property
    def server(self) -> Any:
        """Get the underlying PIServer object.

        Raises:
            PIConnectionError: If not connected
        """
        if not self.is_connected:
            raise PIConnectionError(
                "Not connected to PI Server",
                server=self._config.host,
            )
        return self._server

    @property
    def name(self) -> str:
        """Get the server name."""
        if self._server:
            return str(self._server.Name)
        return self._config.host

    def connect(self) -> None:
        """Establish connection to the PI Server.

        Raises:
            PIConnectionError: If connection fails
        """
        if self._connected:
            return

        try:
            # Initialize the SDK if not already done
            self._sdk.initialize()

            # Get the PIServers collection
            PIServers = self._sdk.pi_servers_class

            # Find or connect to the server
            servers = PIServers.GetPIServers()
            self._server = servers[self._config.host]

            if self._server is None:
                # Try connecting by name
                PIServer = self._sdk.pi_server_class
                self._server = PIServer.FindPIServer(self._config.host)

            if self._server is None:
                raise PIConnectionError(
                    f"PI Server not found: {self._config.host}",
                    server=self._config.host,
                )

            # Connect with timeout
            self._server.Connect()

            self._connected = True
            logger.info(f"Connected to PI Server: {self.name}")

        except PIConnectionError:
            raise
        except Exception as e:
            raise PIConnectionError(
                f"Failed to connect to PI Server: {e}",
                server=self._config.host,
                details={"error_type": type(e).__name__},
            ) from e

    def disconnect(self) -> None:
        """Disconnect from the PI Server."""
        if self._server is not None and self._connected:
            try:
                self._server.Disconnect()
                logger.info(f"Disconnected from PI Server: {self.name}")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self._server = None
                self._connected = False
                self._point_cache.clear()

    def __enter__(self) -> PIServerConnection:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()

    def get_point(self, tag_name: str, use_cache: bool = True) -> Any:
        """Get a PI Point by name.

        Args:
            tag_name: The PI Point name (tag name)
            use_cache: Whether to use the point cache

        Returns:
            The PIPoint object

        Raises:
            PIPointNotFoundError: If the point doesn't exist
            PIConnectionError: If not connected
        """
        if not self.is_connected:
            raise PIConnectionError(
                "Not connected to PI Server",
                server=self._config.host,
            )

        # Check cache first
        if use_cache and tag_name in self._point_cache:
            return self._point_cache[tag_name]

        try:
            PIPoint = self._sdk.pi_point_class
            point = PIPoint.FindPIPoint(self._server, tag_name)

            if point is None:
                raise PIPointNotFoundError(tag_name, server=self.name)

            if use_cache:
                self._point_cache[tag_name] = point

            return point

        except PIPointNotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise PIPointNotFoundError(tag_name, server=self.name) from e
            raise PIConnectionError(
                f"Failed to get PI Point: {tag_name}",
                server=self._config.host,
                details={"error": str(e)},
            ) from e

    def get_points(self, tag_names: list[str]) -> list[Any]:
        """Get multiple PI Points by name.

        Args:
            tag_names: List of PI Point names

        Returns:
            List of PIPoint objects

        Raises:
            PIPointNotFoundError: If any point doesn't exist
            PIConnectionError: If not connected
        """
        points = []
        not_found = []

        for tag_name in tag_names:
            try:
                point = self.get_point(tag_name)
                points.append(point)
            except PIPointNotFoundError:
                not_found.append(tag_name)

        if not_found:
            raise PIPointNotFoundError(
                not_found[0] if len(not_found) == 1 else f"{len(not_found)} tags",
                server=self.name,
            )

        return points

    def search_points(
        self,
        query: str,
        max_results: int = 1000,
    ) -> list[Any]:
        """Search for PI Points matching a pattern.

        Args:
            query: Search pattern (supports wildcards like "*" and "?")
            max_results: Maximum number of results to return

        Returns:
            List of matching PIPoint objects
        """
        if not self.is_connected:
            raise PIConnectionError(
                "Not connected to PI Server",
                server=self._config.host,
            )

        try:
            PIPoint = self._sdk.pi_point_class

            # Use PIPoint.FindPIPoints for pattern matching
            points = PIPoint.FindPIPoints(self._server, query, None, None)

            # Convert to list and limit results
            result = []
            for i, point in enumerate(points):
                if i >= max_results:
                    break
                result.append(point)

            return result

        except Exception as e:
            raise PIConnectionError(
                f"Failed to search PI Points: {e}",
                server=self._config.host,
            ) from e

    def point_exists(self, tag_name: str) -> bool:
        """Check if a PI Point exists.

        Args:
            tag_name: The PI Point name

        Returns:
            True if the point exists, False otherwise
        """
        try:
            self.get_point(tag_name, use_cache=False)
            return True
        except PIPointNotFoundError:
            return False

    @classmethod
    def list_servers(cls) -> list[str]:
        """List all known PI Servers.

        Returns:
            List of server names
        """
        sdk = get_sdk_manager()
        sdk.initialize()

        PIServers = sdk.pi_servers_class
        servers = PIServers.GetPIServers()

        return [str(s.Name) for s in servers]

    @classmethod
    def get_default_server(cls) -> str | None:
        """Get the default PI Server name.

        Returns:
            The default server name, or None if not configured
        """
        sdk = get_sdk_manager()
        sdk.initialize()

        PIServers = sdk.pi_servers_class
        default = PIServers.DefaultPIServer

        if default:
            return str(default.Name)
        return None


@contextmanager
def pi_connection(
    server: str | PIServerConfig,
) -> Generator[PIServerConnection, None, None]:
    """Context manager for PI Server connections.

    Args:
        server: Server hostname or configuration

    Yields:
        Connected PIServerConnection

    Example:
        >>> with pi_connection("my-pi-server") as conn:
        ...     point = conn.get_point("SINUSOID")
    """
    conn = PIServerConnection(server)
    try:
        conn.connect()
        yield conn
    finally:
        conn.disconnect()
