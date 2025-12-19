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
