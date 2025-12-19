"""Tests for PIPolars configuration."""

from datetime import timedelta
from pathlib import Path

import pytest

from pipolars.core.config import (
    AFServerConfig,
    AuthMethod,
    CacheBackend,
    CacheConfig,
    PIConfig,
    PIServerConfig,
    PolarsConfig,
    QueryConfig,
)


class TestPIServerConfig:
    """Tests for PIServerConfig class."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = PIServerConfig(host="test-server")

        assert config.host == "test-server"
        assert config.port == 5450
        assert config.timeout == 30
        assert config.auth_method == AuthMethod.WINDOWS

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = PIServerConfig(
            host="test-server",
            port=5451,
            timeout=60,
            auth_method=AuthMethod.EXPLICIT,
            username="user",
            password="password",
        )

        assert config.port == 5451
        assert config.timeout == 60
        assert config.auth_method == AuthMethod.EXPLICIT

    def test_explicit_auth_requires_credentials(self) -> None:
        """Test that explicit auth requires username/password."""
        with pytest.raises(ValueError):
            PIServerConfig(
                host="test-server",
                auth_method=AuthMethod.EXPLICIT,
            )


class TestCacheConfig:
    """Tests for CacheConfig class."""

    def test_default_values(self) -> None:
        """Test default cache configuration."""
        config = CacheConfig()

        assert config.backend == CacheBackend.NONE
        assert config.max_size_mb == 1024
        assert config.ttl_hours == 24

    def test_ttl_property(self) -> None:
        """Test TTL property returns timedelta."""
        config = CacheConfig(ttl_hours=12)

        assert config.ttl == timedelta(hours=12)


class TestPolarsConfig:
    """Tests for PolarsConfig class."""

    def test_default_values(self) -> None:
        """Test default Polars configuration."""
        config = PolarsConfig()

        assert config.timestamp_column == "timestamp"
        assert config.value_column == "value"
        assert config.timezone == "UTC"

    def test_invalid_timezone(self) -> None:
        """Test that invalid timezone raises error."""
        with pytest.raises(ValueError):
            PolarsConfig(timezone="Invalid/Timezone")


class TestPIConfig:
    """Tests for PIConfig class."""

    def test_default_values(self) -> None:
        """Test default full configuration."""
        config = PIConfig()

        assert config.server is not None
        assert config.cache is not None
        assert config.polars is not None

    def test_custom_server(self) -> None:
        """Test custom server configuration."""
        config = PIConfig(
            server=PIServerConfig(host="my-server"),
        )

        assert config.server.host == "my-server"

    def test_to_dict_masks_password(self) -> None:
        """Test that to_dict masks sensitive fields."""
        config = PIConfig(
            server=PIServerConfig(
                host="test",
                auth_method=AuthMethod.EXPLICIT,
                username="user",
                password="secret",
            ),
        )

        d = config.to_dict()
        assert d["server"]["password"] == "***"
