"""Tests for PIPolars type definitions."""

from datetime import datetime

import pytest

from pipolars.core.types import (
    AFTime,
    AnalysisInfo,
    AnalysisStatus,
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


class TestPointConfigExtended:
    """Tests for PointConfig extended attributes."""

    def test_alarm_thresholds(self) -> None:
        """Test PointConfig with alarm thresholds."""
        config = PointConfig(
            name="TEMP1",
            point_id=100,
            point_type=PointType.FLOAT64,
            value_high_alarm=100.0,
            value_low_alarm=0.0,
            value_high_warning=90.0,
            value_low_warning=10.0,
        )

        assert config.value_high_alarm == 100.0
        assert config.value_low_alarm == 0.0
        assert config.value_high_warning == 90.0
        assert config.value_low_warning == 10.0

    def test_rate_of_change_limits(self) -> None:
        """Test PointConfig with rate of change limits."""
        config = PointConfig(
            name="FLOW1",
            point_id=101,
            point_type=PointType.FLOAT32,
            roc_high_value=10.0,
            roc_low_value=-10.0,
        )

        assert config.roc_high_value == 10.0
        assert config.roc_low_value == -10.0

    def test_interface_information(self) -> None:
        """Test PointConfig with interface information."""
        config = PointConfig(
            name="PRESS1",
            point_id=102,
            point_type=PointType.FLOAT64,
            interface_id=5,
            interface_name="PI-OPC",
        )

        assert config.interface_id == 5
        assert config.interface_name == "PI-OPC"

    def test_scan_and_source_info(self) -> None:
        """Test PointConfig with scan and source information."""
        config = PointConfig(
            name="LEVEL1",
            point_id=103,
            point_type=PointType.FLOAT32,
            scan_time="1s",
            source_point_id=200,
            source_point_name="SOURCE_TAG",
        )

        assert config.scan_time == "1s"
        assert config.source_point_id == 200
        assert config.source_point_name == "SOURCE_TAG"

    def test_additional_metadata(self) -> None:
        """Test PointConfig with additional metadata."""
        config = PointConfig(
            name="VALVE1",
            point_id=104,
            point_type=PointType.DIGITAL,
            conversion_factor=1.5,
            device_name="PLC001",
            alias="VALVE_MAIN",
        )

        assert config.conversion_factor == 1.5
        assert config.device_name == "PLC001"
        assert config.alias == "VALVE_MAIN"

    def test_all_new_attributes_default_none(self) -> None:
        """Test that new attributes default to None or empty string."""
        config = PointConfig(
            name="TEST",
            point_id=1,
            point_type=PointType.FLOAT64,
        )

        assert config.value_high_alarm is None
        assert config.value_low_alarm is None
        assert config.value_high_warning is None
        assert config.value_low_warning is None
        assert config.roc_high_value is None
        assert config.roc_low_value is None
        assert config.interface_id is None
        assert config.interface_name == ""
        assert config.scan_time == ""
        assert config.source_point_id is None
        assert config.source_point_name == ""
        assert config.conversion_factor is None
        assert config.device_name == ""
        assert config.alias == ""


class TestAnalysisStatus:
    """Tests for AnalysisStatus enum."""

    def test_status_values(self) -> None:
        """Test AnalysisStatus values."""
        assert AnalysisStatus.RUNNING.value == "Running"
        assert AnalysisStatus.STOPPED.value == "Stopped"
        assert AnalysisStatus.ERROR.value == "Error"
        assert AnalysisStatus.UNKNOWN.value == "Unknown"

    def test_status_is_string_enum(self) -> None:
        """Test that AnalysisStatus is a string enum."""
        assert isinstance(AnalysisStatus.RUNNING, str)
        assert AnalysisStatus.RUNNING == "Running"


class TestAnalysisInfo:
    """Tests for AnalysisInfo dataclass."""

    def test_basic_creation(self) -> None:
        """Test AnalysisInfo basic creation."""
        info = AnalysisInfo(
            name="Temperature Analysis",
            id="abc-123-def",
            path="/Plant/Unit1|Temperature Analysis",
        )

        assert info.name == "Temperature Analysis"
        assert info.id == "abc-123-def"
        assert info.path == "/Plant/Unit1|Temperature Analysis"
        assert info.description == ""
        assert info.status == AnalysisStatus.UNKNOWN
        assert info.is_enabled is False

    def test_full_creation(self) -> None:
        """Test AnalysisInfo with all attributes."""
        info = AnalysisInfo(
            name="Flow Analysis",
            id="guid-123",
            path="/Plant/Unit1|Flow Analysis",
            description="Calculates flow rate",
            target_id="target-guid",
            target_name="Unit1",
            target_path="/Plant/Unit1",
            template_id="template-guid",
            template_name="FlowTemplate",
            template_description="Template for flow analyses",
            status=AnalysisStatus.RUNNING,
            is_enabled=True,
            categories=("Production", "Flow"),
            time_rule_plugin_id="periodic",
            time_rule_config_string="1h",
            analysis_rule_max_queue_size=100,
            group_id="group-1",
            priority=5,
            maximum_queue_time="10m",
            auto_created_event_frame_count=50,
            output_attributes=("FlowRate", "TotalFlow"),
        )

        assert info.name == "Flow Analysis"
        assert info.description == "Calculates flow rate"
        assert info.target_name == "Unit1"
        assert info.template_name == "FlowTemplate"
        assert info.status == AnalysisStatus.RUNNING
        assert info.is_enabled is True
        assert info.categories == ("Production", "Flow")
        assert info.time_rule_config_string == "1h"
        assert info.analysis_rule_max_queue_size == 100
        assert info.priority == 5
        assert info.auto_created_event_frame_count == 50
        assert info.output_attributes == ("FlowRate", "TotalFlow")

    def test_default_values(self) -> None:
        """Test AnalysisInfo default values."""
        info = AnalysisInfo(
            name="Test",
            id="id",
            path="/path",
        )

        assert info.target_id == ""
        assert info.target_name == ""
        assert info.target_path == ""
        assert info.template_id == ""
        assert info.template_name == ""
        assert info.template_description == ""
        assert info.categories == ()
        assert info.time_rule_plugin_id == ""
        assert info.time_rule_config_string == ""
        assert info.analysis_rule_max_queue_size is None
        assert info.group_id == ""
        assert info.priority is None
        assert info.maximum_queue_time == ""
        assert info.auto_created_event_frame_count is None
        assert info.output_attributes == ()

    def test_frozen_dataclass(self) -> None:
        """Test that AnalysisInfo is immutable."""
        info = AnalysisInfo(
            name="Test",
            id="id",
            path="/path",
        )

        with pytest.raises(AttributeError):
            info.name = "New Name"  # type: ignore
