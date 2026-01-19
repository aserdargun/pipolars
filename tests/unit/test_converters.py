"""Tests for PIPolars data converters."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from pipolars.core.config import PolarsConfig
from pipolars.core.types import DataQuality, PIValue
from pipolars.transform.converters import (
    PIToPolarsConverter,
    multi_tag_to_dataframe,
    summaries_to_dataframe,
    values_to_dataframe,
)


class TestPIToPolarsConverter:
    """Tests for PIToPolarsConverter class."""

    @pytest.fixture
    def converter(self) -> PIToPolarsConverter:
        """Create a converter instance."""
        return PIToPolarsConverter()

    @pytest.fixture
    def sample_values(self) -> list[PIValue]:
        """Create sample PIValue objects."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        return [
            PIValue(timestamp=base_time + timedelta(hours=i), value=float(i * 10))
            for i in range(10)
        ]

    def test_values_to_dataframe_basic(
        self,
        converter: PIToPolarsConverter,
        sample_values: list[PIValue],
    ) -> None:
        """Test basic conversion to DataFrame."""
        df = converter.values_to_dataframe(sample_values)

        assert isinstance(df, pl.DataFrame)
        assert len(df) == len(sample_values)
        assert "timestamp" in df.columns
        assert "value" in df.columns

    def test_values_to_dataframe_with_quality(
        self,
        converter: PIToPolarsConverter,
        sample_values: list[PIValue],
    ) -> None:
        """Test conversion with quality column."""
        df = converter.values_to_dataframe(sample_values, include_quality=True)

        assert "quality" in df.columns

    def test_values_to_dataframe_empty(
        self,
        converter: PIToPolarsConverter,
    ) -> None:
        """Test conversion of empty list."""
        df = converter.values_to_dataframe([])

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0
        assert "timestamp" in df.columns
        assert "value" in df.columns

    def test_multi_tag_to_dataframe(
        self,
        converter: PIToPolarsConverter,
        sample_values: list[PIValue],
    ) -> None:
        """Test multi-tag conversion."""
        tag_values = {
            "TAG1": sample_values,
            "TAG2": sample_values,
        }

        df = converter.multi_tag_to_dataframe(tag_values)

        assert len(df) == 2 * len(sample_values)
        assert "tag" in df.columns
        assert df["tag"].unique().len() == 2

    def test_multi_tag_to_dataframe_pivot(
        self,
        converter: PIToPolarsConverter,
        sample_values: list[PIValue],
    ) -> None:
        """Test multi-tag conversion with pivot."""
        tag_values = {
            "TAG1": sample_values,
            "TAG2": sample_values,
        }

        df = converter.multi_tag_to_dataframe(tag_values, pivot=True)

        # After pivot, tags should be columns
        assert "TAG1" in df.columns
        assert "TAG2" in df.columns

    def test_summaries_to_dataframe(
        self,
        converter: PIToPolarsConverter,
    ) -> None:
        """Test summary conversion."""
        summaries = {
            "TAG1": {"average": 50.0, "minimum": 10.0, "maximum": 90.0},
            "TAG2": {"average": 60.0, "minimum": 20.0, "maximum": 100.0},
        }

        df = converter.summaries_to_dataframe(summaries)

        assert len(df) == 2
        assert "tag" in df.columns
        assert "average" in df.columns

    def test_values_to_series(
        self,
        converter: PIToPolarsConverter,
        sample_values: list[PIValue],
    ) -> None:
        """Test conversion to Series."""
        series = converter.values_to_series(sample_values, name="test_values")

        assert isinstance(series, pl.Series)
        assert series.name == "test_values"
        assert len(series) == len(sample_values)

    def test_to_lazy_frame(
        self,
        converter: PIToPolarsConverter,
        sample_values: list[PIValue],
    ) -> None:
        """Test conversion to LazyFrame."""
        lf = converter.to_lazy_frame(sample_values)

        assert isinstance(lf, pl.LazyFrame)

        # Collect to verify
        df = lf.collect()
        assert len(df) == len(sample_values)


class TestDigitalStateConversion:
    """Tests for digital state (non-numeric) value conversion."""

    @pytest.fixture
    def converter(self) -> PIToPolarsConverter:
        """Create a converter instance."""
        return PIToPolarsConverter()

    @pytest.fixture
    def digital_values(self) -> list[PIValue]:
        """Create sample digital state PIValue objects."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        states = ["ON", "OFF", "ON", "ON", "OFF", "ON", "OFF", "OFF", "ON", "OFF"]
        return [
            PIValue(timestamp=base_time + timedelta(hours=i), value=states[i])
            for i in range(10)
        ]

    @pytest.fixture
    def mixed_digital_values(self) -> list[PIValue]:
        """Create digital values with various state names."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        states = ["RUN", "STOP", "IDLE", "RUN", "FAULT", "RUN", "STOP", "IDLE", "RUN", "STOP"]
        return [
            PIValue(timestamp=base_time + timedelta(hours=i), value=states[i])
            for i in range(10)
        ]

    def test_digital_values_to_dataframe(
        self,
        converter: PIToPolarsConverter,
        digital_values: list[PIValue],
    ) -> None:
        """Test conversion of digital state values to DataFrame."""
        df = converter.values_to_dataframe(digital_values)

        assert isinstance(df, pl.DataFrame)
        assert len(df) == len(digital_values)
        assert "timestamp" in df.columns
        assert "value" in df.columns

        # Values should be preserved as strings, not null
        assert df["value"].null_count() == 0
        assert df["value"].dtype == pl.Utf8

        # Check actual values
        values_list = df["value"].to_list()
        assert "ON" in values_list
        assert "OFF" in values_list

    def test_digital_values_preserve_state_names(
        self,
        converter: PIToPolarsConverter,
        mixed_digital_values: list[PIValue],
    ) -> None:
        """Test that digital state names are preserved correctly."""
        df = converter.values_to_dataframe(mixed_digital_values)

        values_list = df["value"].to_list()
        expected_states = ["RUN", "STOP", "IDLE", "RUN", "FAULT", "RUN", "STOP", "IDLE", "RUN", "STOP"]
        assert values_list == expected_states

    def test_digital_values_with_quality(
        self,
        converter: PIToPolarsConverter,
        digital_values: list[PIValue],
    ) -> None:
        """Test digital value conversion with quality column."""
        df = converter.values_to_dataframe(digital_values, include_quality=True)

        assert "quality" in df.columns
        assert df["value"].null_count() == 0

    def test_multi_tag_digital_values(
        self,
        converter: PIToPolarsConverter,
        digital_values: list[PIValue],
    ) -> None:
        """Test multi-tag conversion with digital values."""
        tag_values = {
            "DIGITAL_TAG1": digital_values,
            "DIGITAL_TAG2": digital_values,
        }

        df = converter.multi_tag_to_dataframe(tag_values)

        assert len(df) == 2 * len(digital_values)
        assert "tag" in df.columns
        assert df["value"].null_count() == 0
        assert df["value"].dtype == pl.Utf8

    def test_multi_tag_digital_values_pivot(
        self,
        converter: PIToPolarsConverter,
        digital_values: list[PIValue],
    ) -> None:
        """Test multi-tag digital conversion with pivot."""
        tag_values = {
            "DIGITAL_TAG1": digital_values,
            "DIGITAL_TAG2": digital_values,
        }

        df = converter.multi_tag_to_dataframe(tag_values, pivot=True)

        assert "DIGITAL_TAG1" in df.columns
        assert "DIGITAL_TAG2" in df.columns

    def test_digital_values_to_series(
        self,
        converter: PIToPolarsConverter,
        digital_values: list[PIValue],
    ) -> None:
        """Test conversion of digital values to Series."""
        series = converter.values_to_series(digital_values, name="digital_states")

        assert isinstance(series, pl.Series)
        assert series.name == "digital_states"
        assert len(series) == len(digital_values)
        assert series.null_count() == 0
        assert series.dtype == pl.Utf8

    def test_numeric_string_values_remain_numeric(
        self,
        converter: PIToPolarsConverter,
    ) -> None:
        """Test that numeric strings are converted to floats."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        values = [
            PIValue(timestamp=base_time + timedelta(hours=i), value=str(float(i * 10)))
            for i in range(5)
        ]

        df = converter.values_to_dataframe(values)

        # Should remain as Float64 since strings are numeric
        assert df["value"].dtype == pl.Float64
        assert df["value"].null_count() == 0

    def test_digital_values_with_none(
        self,
        converter: PIToPolarsConverter,
    ) -> None:
        """Test digital values with some None values."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        values = [
            PIValue(timestamp=base_time, value="ON"),
            PIValue(timestamp=base_time + timedelta(hours=1), value=None),
            PIValue(timestamp=base_time + timedelta(hours=2), value="OFF"),
        ]

        df = converter.values_to_dataframe(values)

        assert df["value"].dtype == pl.Utf8
        assert df["value"].null_count() == 1
        values_list = df["value"].to_list()
        assert values_list[0] == "ON"
        assert values_list[1] is None
        assert values_list[2] == "OFF"


class TestConvenienceFunctions:
    """Tests for convenience conversion functions."""

    @pytest.fixture
    def sample_values(self) -> list[PIValue]:
        """Create sample PIValue objects."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        return [
            PIValue(timestamp=base_time + timedelta(hours=i), value=float(i * 10))
            for i in range(10)
        ]

    def test_values_to_dataframe_function(
        self,
        sample_values: list[PIValue],
    ) -> None:
        """Test values_to_dataframe convenience function."""
        df = values_to_dataframe(sample_values)

        assert isinstance(df, pl.DataFrame)
        assert len(df) == len(sample_values)

    def test_multi_tag_to_dataframe_function(
        self,
        sample_values: list[PIValue],
    ) -> None:
        """Test multi_tag_to_dataframe convenience function."""
        tag_values = {"TAG1": sample_values}

        df = multi_tag_to_dataframe(tag_values)

        assert isinstance(df, pl.DataFrame)

    def test_summaries_to_dataframe_function(self) -> None:
        """Test summaries_to_dataframe convenience function."""
        summaries = {"TAG1": {"average": 50.0}}

        df = summaries_to_dataframe(summaries)

        assert isinstance(df, pl.DataFrame)
