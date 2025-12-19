"""Tests for PIPolars type definitions."""

from datetime import datetime

import pytest

from pipolars.core.types import (
    AFTime,
    DataQuality,
    PIValue,
    PointConfig,
    PointType,
    SummaryType,
    TimeRange,
)


class TestAFTime:
    """Tests for AFTime class."""

    def test_now(self) -> None:
        """Test AFTime.now() creation."""
        time = AFTime.now()
        assert time.expression == "*"

    def test_today(self) -> None:
        """Test AFTime.today() creation."""
        time = AFTime.today()
        assert time.expression == "t"

    def test_yesterday(self) -> None:
        """Test AFTime.yesterday() creation."""
        time = AFTime.yesterday()
        assert time.expression == "y"

    def test_ago_days(self) -> None:
        """Test AFTime.ago() with days."""
        time = AFTime.ago(days=7)
        assert time.expression == "*-7d"

    def test_ago_hours(self) -> None:
        """Test AFTime.ago() with hours."""
        time = AFTime.ago(hours=24)
        assert time.expression == "*-24h"

    def test_ago_multiple(self) -> None:
        """Test AFTime.ago() with multiple units."""
        time = AFTime.ago(days=1, hours=2)
        assert "1d" in time.expression
        assert "2h" in time.expression

    def test_from_datetime(self) -> None:
        """Test AFTime.from_datetime()."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        time = AFTime.from_datetime(dt)
        assert "2024-01-15" in time.expression

    def test_str_representation(self) -> None:
        """Test string representation."""
        time = AFTime("*-1d")
        assert str(time) == "*-1d"


class TestPIValue:
    """Tests for PIValue class."""

    def test_creation(self) -> None:
        """Test PIValue creation."""
        timestamp = datetime.now()
        value = PIValue(timestamp=timestamp, value=50.0)

        assert value.timestamp == timestamp
        assert value.value == 50.0
        assert value.quality == DataQuality.GOOD
        assert value.is_good is True

    def test_bad_quality(self) -> None:
        """Test PIValue with bad quality."""
        value = PIValue(
            timestamp=datetime.now(),
            value=0.0,
            quality=DataQuality.BAD,
        )

        assert value.is_good is False

    def test_to_dict(self) -> None:
        """Test PIValue to_dict conversion."""
        timestamp = datetime.now()
        value = PIValue(timestamp=timestamp, value=100.0)

        d = value.to_dict()
        assert d["timestamp"] == timestamp
        assert d["value"] == 100.0
        assert d["quality"] == DataQuality.GOOD.value


class TestTimeRange:
    """Tests for TimeRange class."""

    def test_last_days(self) -> None:
        """Test TimeRange.last() with days."""
        tr = TimeRange.last(days=7)
        assert isinstance(tr.start, AFTime)
        assert isinstance(tr.end, AFTime)

    def test_last_hours(self) -> None:
        """Test TimeRange.last() with hours."""
        tr = TimeRange.last(hours=24)
        assert isinstance(tr.start, AFTime)

    def test_today(self) -> None:
        """Test TimeRange.today()."""
        tr = TimeRange.today()
        assert isinstance(tr.start, AFTime)
        assert tr.start.expression == "t"


class TestPointConfig:
    """Tests for PointConfig class."""

    def test_creation(self) -> None:
        """Test PointConfig creation."""
        config = PointConfig(
            name="SINUSOID",
            point_id=12345,
            point_type=PointType.FLOAT64,
            description="Test point",
            engineering_units="degC",
        )

        assert config.name == "SINUSOID"
        assert config.point_id == 12345
        assert config.point_type == PointType.FLOAT64


class TestSummaryType:
    """Tests for SummaryType enum."""

    def test_values(self) -> None:
        """Test SummaryType values."""
        assert SummaryType.AVERAGE.value == 2
        assert SummaryType.MINIMUM.value == 4
        assert SummaryType.MAXIMUM.value == 8
        assert SummaryType.TOTAL.value == 1
