"""Converters for transforming PI data to Polars DataFrames.

This module provides high-performance conversion functions for
transforming PI System data structures into Polars DataFrames.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import polars as pl

from pipolars.core.config import PolarsConfig
from pipolars.core.types import (
    DataQuality,
    PIValue,
    PI_VALUE_SCHEMA,
    PI_VALUE_WITH_TAG_SCHEMA,
    SUMMARY_SCHEMA,
)


class PIToPolarsConverter:
    """Converts PI System data to Polars DataFrames.

    This class provides optimized conversion methods that leverage
    Polars' columnar format for efficient memory usage and fast
    data processing.

    Example:
        >>> converter = PIToPolarsConverter()
        >>> df = converter.values_to_dataframe(values)
        >>> df = converter.multi_tag_to_dataframe({"TAG1": values1, "TAG2": values2})
    """

    def __init__(self, config: PolarsConfig | None = None) -> None:
        """Initialize the converter.

        Args:
            config: Optional Polars configuration
        """
        self._config = config or PolarsConfig()

    @property
    def config(self) -> PolarsConfig:
        """Get the Polars configuration."""
        return self._config

    def values_to_dataframe(
        self,
        values: Sequence[PIValue],
        include_quality: bool | None = None,
    ) -> pl.DataFrame:
        """Convert a list of PIValues to a Polars DataFrame.

        Args:
            values: Sequence of PIValue objects
            include_quality: Include quality column (default from config)

        Returns:
            Polars DataFrame with timestamp and value columns
        """
        if not values:
            # Return empty DataFrame with schema
            return pl.DataFrame(
                schema={
                    self._config.timestamp_column: pl.Datetime("us", self._config.timezone),
                    self._config.value_column: pl.Float64,
                }
            )

        include_quality = (
            include_quality if include_quality is not None else self._config.include_quality
        )

        # Build column lists for efficient DataFrame construction
        timestamps: list[datetime] = []
        data_values: list[float | None] = []
        qualities: list[int] | None = [] if include_quality else None

        for pv in values:
            timestamps.append(pv.timestamp)

            # Handle different value types
            if isinstance(pv.value, (int, float)):
                data_values.append(float(pv.value))
            elif isinstance(pv.value, str):
                # Try to convert string to float, otherwise use None
                try:
                    data_values.append(float(pv.value))
                except (ValueError, TypeError):
                    data_values.append(None)
            else:
                data_values.append(None)

            if qualities is not None:
                qualities.append(pv.quality.value)

        # Build DataFrame
        data = {
            self._config.timestamp_column: timestamps,
            self._config.value_column: data_values,
        }

        if qualities is not None:
            data[self._config.quality_column] = qualities

        df = pl.DataFrame(data)

        # Convert timestamp to proper datetime type with timezone
        df = df.with_columns(
            pl.col(self._config.timestamp_column)
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone(self._config.timezone)
        )

        return df

    def multi_tag_to_dataframe(
        self,
        tag_values: Mapping[str, Sequence[PIValue]],
        include_quality: bool | None = None,
        pivot: bool = False,
    ) -> pl.DataFrame:
        """Convert multiple tags' values to a single DataFrame.

        Args:
            tag_values: Dictionary mapping tag names to value lists
            include_quality: Include quality column
            pivot: If True, pivot to wide format with tags as columns

        Returns:
            Polars DataFrame with tag, timestamp, and value columns
        """
        if not tag_values:
            return pl.DataFrame(
                schema={
                    self._config.tag_column: pl.Utf8,
                    self._config.timestamp_column: pl.Datetime("us", self._config.timezone),
                    self._config.value_column: pl.Float64,
                }
            )

        include_quality = (
            include_quality if include_quality is not None else self._config.include_quality
        )

        # Collect all data in lists
        all_tags: list[str] = []
        all_timestamps: list[datetime] = []
        all_values: list[float | None] = []
        all_qualities: list[int] | None = [] if include_quality else None

        for tag_name, values in tag_values.items():
            for pv in values:
                all_tags.append(tag_name)
                all_timestamps.append(pv.timestamp)

                if isinstance(pv.value, (int, float)):
                    all_values.append(float(pv.value))
                elif isinstance(pv.value, str):
                    try:
                        all_values.append(float(pv.value))
                    except (ValueError, TypeError):
                        all_values.append(None)
                else:
                    all_values.append(None)

                if all_qualities is not None:
                    all_qualities.append(pv.quality.value)

        # Build DataFrame
        data = {
            self._config.tag_column: all_tags,
            self._config.timestamp_column: all_timestamps,
            self._config.value_column: all_values,
        }

        if all_qualities is not None:
            data[self._config.quality_column] = all_qualities

        df = pl.DataFrame(data)

        # Convert timestamp
        df = df.with_columns(
            pl.col(self._config.timestamp_column)
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone(self._config.timezone)
        )

        # Optionally pivot to wide format
        if pivot:
            df = df.pivot(
                index=self._config.timestamp_column,
                on=self._config.tag_column,
                values=self._config.value_column,
            ).sort(self._config.timestamp_column)

        return df

    def summaries_to_dataframe(
        self,
        tag_summaries: dict[str, dict[str, Any]],
    ) -> pl.DataFrame:
        """Convert summary results to a DataFrame.

        Args:
            tag_summaries: Dictionary mapping tag names to summary dictionaries

        Returns:
            Polars DataFrame with summary statistics
        """
        if not tag_summaries:
            return pl.DataFrame(
                schema={
                    self._config.tag_column: pl.Utf8,
                    "average": pl.Float64,
                    "minimum": pl.Float64,
                    "maximum": pl.Float64,
                    "total": pl.Float64,
                    "count": pl.Int64,
                }
            )

        rows = []
        for tag_name, summaries in tag_summaries.items():
            row = {self._config.tag_column: tag_name}
            row.update(summaries)
            rows.append(row)

        return pl.DataFrame(rows)

    def time_series_summaries_to_dataframe(
        self,
        tag_interval_summaries: dict[str, list[dict[str, Any]]],
    ) -> pl.DataFrame:
        """Convert time-series summaries to a DataFrame.

        Args:
            tag_interval_summaries: Dictionary mapping tag names to
                                    lists of interval summaries

        Returns:
            Polars DataFrame with time-series summary data
        """
        if not tag_interval_summaries:
            return pl.DataFrame(
                schema={
                    self._config.tag_column: pl.Utf8,
                    self._config.timestamp_column: pl.Datetime("us", self._config.timezone),
                    "average": pl.Float64,
                }
            )

        all_rows = []
        for tag_name, intervals in tag_interval_summaries.items():
            for interval in intervals:
                row = {self._config.tag_column: tag_name}
                row.update(interval)
                all_rows.append(row)

        df = pl.DataFrame(all_rows)

        # Convert timestamp column if present
        if self._config.timestamp_column in df.columns:
            df = df.with_columns(
                pl.col(self._config.timestamp_column)
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone(self._config.timezone)
            )

        return df

    def values_to_series(
        self,
        values: Sequence[PIValue],
        name: str | None = None,
    ) -> pl.Series:
        """Convert PIValues to a Polars Series.

        Args:
            values: Sequence of PIValue objects
            name: Optional series name

        Returns:
            Polars Series of values
        """
        data_values: list[float | None] = []
        for pv in values:
            if isinstance(pv.value, (int, float)):
                data_values.append(float(pv.value))
            elif isinstance(pv.value, str):
                try:
                    data_values.append(float(pv.value))
                except (ValueError, TypeError):
                    data_values.append(None)
            else:
                data_values.append(None)

        return pl.Series(name or self._config.value_column, data_values)

    def to_lazy_frame(
        self,
        values: Sequence[PIValue],
        include_quality: bool | None = None,
    ) -> pl.LazyFrame:
        """Convert PIValues to a Polars LazyFrame for deferred execution.

        Args:
            values: Sequence of PIValue objects
            include_quality: Include quality column

        Returns:
            Polars LazyFrame for lazy evaluation
        """
        df = self.values_to_dataframe(values, include_quality)
        return df.lazy()


# Convenience functions for quick conversion


def values_to_dataframe(
    values: Sequence[PIValue],
    include_quality: bool = False,
    config: PolarsConfig | None = None,
) -> pl.DataFrame:
    """Convert PIValues to a Polars DataFrame.

    Convenience function for quick conversion.

    Args:
        values: Sequence of PIValue objects
        include_quality: Include quality column
        config: Optional Polars configuration

    Returns:
        Polars DataFrame
    """
    converter = PIToPolarsConverter(config)
    return converter.values_to_dataframe(values, include_quality)


def multi_tag_to_dataframe(
    tag_values: dict[str, Sequence[PIValue]],
    pivot: bool = False,
    include_quality: bool = False,
    config: PolarsConfig | None = None,
) -> pl.DataFrame:
    """Convert multiple tags' values to a DataFrame.

    Convenience function for quick conversion.

    Args:
        tag_values: Dictionary mapping tag names to value lists
        pivot: If True, pivot to wide format
        include_quality: Include quality column
        config: Optional Polars configuration

    Returns:
        Polars DataFrame
    """
    converter = PIToPolarsConverter(config)
    return converter.multi_tag_to_dataframe(tag_values, include_quality, pivot)


def summaries_to_dataframe(
    tag_summaries: dict[str, dict[str, Any]],
    config: PolarsConfig | None = None,
) -> pl.DataFrame:
    """Convert summary results to a DataFrame.

    Convenience function for quick conversion.

    Args:
        tag_summaries: Dictionary mapping tag names to summaries
        config: Optional Polars configuration

    Returns:
        Polars DataFrame
    """
    converter = PIToPolarsConverter(config)
    return converter.summaries_to_dataframe(tag_summaries)
