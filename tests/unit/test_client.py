"""Unit tests for PIClient."""

from unittest.mock import MagicMock, patch

import pytest

from pipolars.api.client import PIClient
from pipolars.core.config import PIConfig, PIServerConfig


class TestPIClientInitialization:
    """Tests for PIClient initialization."""

    @patch("pipolars.api.client.PIServerConnection")
    def test_blank_server_uses_default(self, mock_connection: MagicMock) -> None:
        """Test that blank server name uses default localhost."""
        client = PIClient("")

        assert client.config.server.host == "localhost"

    @patch("pipolars.api.client.PIServerConnection")
    def test_whitespace_server_uses_default(self, mock_connection: MagicMock) -> None:
        """Test that whitespace-only server name uses default localhost."""
        client = PIClient("   ")

        assert client.config.server.host == "localhost"

    @patch("pipolars.api.client.PIServerConnection")
    def test_none_server_uses_default(self, mock_connection: MagicMock) -> None:
        """Test that None server uses default localhost."""
        client = PIClient(None)

        assert client.config.server.host == "localhost"

    @patch("pipolars.api.client.PIServerConnection")
    def test_no_args_uses_default(self, mock_connection: MagicMock) -> None:
        """Test that no arguments uses default localhost."""
        client = PIClient()

        assert client.config.server.host == "localhost"

    @patch("pipolars.api.client.PIServerConnection")
    def test_explicit_server_name(self, mock_connection: MagicMock) -> None:
        """Test that explicit server name is used."""
        client = PIClient("my-pi-server")

        assert client.config.server.host == "my-pi-server"

    @patch("pipolars.api.client.PIServerConnection")
    def test_server_config_object(self, mock_connection: MagicMock) -> None:
        """Test that PIServerConfig object is used."""
        server_config = PIServerConfig(host="config-server")
        client = PIClient(server_config)

        assert client.config.server.host == "config-server"

    @patch("pipolars.api.client.PIServerConnection")
    def test_full_config_object(self, mock_connection: MagicMock) -> None:
        """Test that full PIConfig object is used."""
        config = PIConfig(server=PIServerConfig(host="full-config-server"))
        client = PIClient(config=config)

        assert client.config.server.host == "full-config-server"

    @patch("pipolars.api.client.PIServerConnection")
    def test_config_takes_precedence_over_server(
        self, mock_connection: MagicMock
    ) -> None:
        """Test that config parameter takes precedence over server."""
        config = PIConfig(server=PIServerConfig(host="config-host"))
        client = PIClient(server="server-arg", config=config)

        assert client.config.server.host == "config-host"
