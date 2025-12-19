"""Pytest configuration and fixtures for PIPolars tests."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pipolars.core.config import PIConfig, PIServerConfig
from pipolars.core.types import DataQuality, PIValue


@pytest.fixture
def sample_pi_values() -> list[PIValue]:
    """Create sample PIValue objects for testing."""
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    values = []

    for i in range(100):
        values.append(
            PIValue(
                timestamp=base_time + timedelta(hours=i),
                value=50.0 + (i % 10) * 5.0,
                quality=DataQuality.GOOD,
            )
        )

    return values


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Create a sample Polars DataFrame for testing."""
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    return pl.DataFrame({
        "timestamp": [base_time + timedelta(hours=i) for i in range(100)],
        "value": [50.0 + (i % 10) * 5.0 for i in range(100)],
    })


@pytest.fixture
def mock_pi_config() -> PIConfig:
    """Create a mock PI configuration."""
    return PIConfig(
        server=PIServerConfig(host="test-server"),
    )


@pytest.fixture
def mock_pi_server() -> Generator[MagicMock, None, None]:
    """Create a mock PI Server connection."""
    with patch("pipolars.connection.server.PIServerConnection") as mock:
        server = MagicMock()
        server.is_connected = True
        server.name = "test-server"
        mock.return_value = server
        yield server


@pytest.fixture
def mock_sdk_manager() -> Generator[MagicMock, None, None]:
    """Create a mock SDK manager."""
    with patch("pipolars.connection.sdk.PISDKManager") as mock:
        manager = MagicMock()
        manager.is_initialized = True
        mock.return_value = manager
        yield manager


class MockPIPoint:
    """Mock PI Point for testing."""

    def __init__(self, name: str, values: list[PIValue] | None = None) -> None:
        self.Name = name
        self._values = values or []

    def CurrentValue(self) -> Any:
        """Return mock current value."""
        mock_value = MagicMock()
        mock_value.Timestamp.LocalTime = datetime.now()
        mock_value.Value = 50.0
        mock_value.IsGood = True
        return mock_value

    def RecordedValues(self, *args: Any, **kwargs: Any) -> list[Any]:
        """Return mock recorded values."""
        return [self._create_mock_value(v) for v in self._values]

    def _create_mock_value(self, pv: PIValue) -> Any:
        """Create a mock AF value from a PIValue."""
        mock_value = MagicMock()
        mock_value.Timestamp.LocalTime = pv.timestamp
        mock_value.Value = pv.value
        mock_value.IsGood = pv.quality == DataQuality.GOOD
        return mock_value


@pytest.fixture
def mock_pi_point(sample_pi_values: list[PIValue]) -> MockPIPoint:
    """Create a mock PI Point."""
    return MockPIPoint("SINUSOID", sample_pi_values)
