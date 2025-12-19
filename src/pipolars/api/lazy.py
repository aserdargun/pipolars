"""Lazy evaluation support for PI queries.

This module provides lazy evaluation capabilities, allowing queries
to be built and optimized before execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

import polars as pl

from pipolars.core.types import PITimestamp, SummaryType

if TYPE_CHECKING:
    from pipolars.api.client import PIClient


@dataclass
class LazyOperation:
    """Represents a lazy operation to be applied."""

    name: str
    func: Callable[[pl.LazyFrame], pl.LazyFrame]
    kwargs: dict[str, Any] = field(default_factory=dict)


class LazyPIQuery:
    """Lazy query builder for deferred PI data operations.

    This class allows building complex data transformations that
    are only executed when explicitly collected. This enables
    query optimization and efficient memory usage.

    Example:
        >>> lazy_df = client.query("SINUSOID") \\
        ...     .last(hours=24) \\
        ...     .recorded() \\
        ...     .to_lazy()

        >>> result = lazy_df \\
        ...     .resample("1h") \\
        ...     .filter(pl.col("value") > 50) \\
        ...     .with_columns([
        ...         pl.col("value").rolling_mean(window_size=12).alias("rolling_avg")
        ...     ]) \\
        ...     .collect()
    """

    def __init__(
        self,
        client: PIClient,
        tags: list[str],
        start: PITimestamp,
        end: PITimestamp,
    ) -> None:
        """Initialize the lazy query.

        Args:
            client: PIClient instance
            tags: List of tags to query
            start: Start time
            end: End time
        """
        self._client = client
        self._tags = tags
        self._start = start
        self._end = end
        self._operations: list[LazyOperation] = []
        self._query_type = "recorded"
        self._interval: str | None = None

    def recorded(self) -> LazyPIQuery:
        """Set query to retrieve recorded values.

        Returns:
            Self for method chaining
        """
        self._query_type = "recorded"
        return self

    def interpolated(self, interval: str = "1h") -> LazyPIQuery:
        """Set query to retrieve interpolated values.

        Args:
            interval: Interpolation interval

        Returns:
            Self for method chaining
        """
        self._query_type = "interpolated"
        self._interval = interval
        return self

    def filter(self, predicate: pl.Expr) -> LazyPIQuery:
        """Add a filter operation.

        Args:
            predicate: Polars expression for filtering

        Returns:
            Self for method chaining
        """
        self._operations.append(
            LazyOperation(
                name="filter",
                func=lambda lf, p=predicate: lf.filter(p),
            )
        )
        return self

    def select(self, *exprs: pl.Expr | str) -> LazyPIQuery:
        """Add a select operation.

        Args:
            *exprs: Columns or expressions to select

        Returns:
            Self for method chaining
        """
        self._operations.append(
            LazyOperation(
                name="select",
                func=lambda lf, e=exprs: lf.select(e),
            )
        )
        return self

    def with_columns(self, exprs: list[pl.Expr]) -> LazyPIQuery:
        """Add columns using expressions.

        Args:
            exprs: List of Polars expressions

        Returns:
            Self for method chaining
        """
        self._operations.append(
            LazyOperation(
                name="with_columns",
                func=lambda lf, e=exprs: lf.with_columns(e),
            )
        )
        return self

    def sort(
        self,
        by: str | list[str],
        descending: bool = False,
    ) -> LazyPIQuery:
        """Add a sort operation.

        Args:
            by: Column(s) to sort by
            descending: Sort in descending order

        Returns:
            Self for method chaining
        """
        self._operations.append(
            LazyOperation(
                name="sort",
                func=lambda lf, b=by, d=descending: lf.sort(b, descending=d),
            )
        )
        return self

    def resample(
        self,
        interval: str,
        timestamp_col: str = "timestamp",
        aggregation: str = "mean",
    ) -> LazyPIQuery:
        """Resample data to a new time interval.

        Args:
            interval: Target interval (e.g., "1h", "1d")
            timestamp_col: Name of timestamp column
            aggregation: Aggregation method

        Returns:
            Self for method chaining
        """

        def resample_func(lf: pl.LazyFrame) -> pl.LazyFrame:
            numeric_cols = [
                c
                for c in lf.columns
                if c != timestamp_col
            ]

            if aggregation == "mean":
                agg_exprs = [pl.col(c).mean().alias(c) for c in numeric_cols]
            elif aggregation == "sum":
                agg_exprs = [pl.col(c).sum().alias(c) for c in numeric_cols]
            elif aggregation == "min":
                agg_exprs = [pl.col(c).min().alias(c) for c in numeric_cols]
            elif aggregation == "max":
                agg_exprs = [pl.col(c).max().alias(c) for c in numeric_cols]
            else:
                agg_exprs = [pl.col(c).mean().alias(c) for c in numeric_cols]

            return lf.group_by_dynamic(timestamp_col, every=interval).agg(agg_exprs)

        self._operations.append(
            LazyOperation(name="resample", func=resample_func)
        )
        return self

    def rolling(
        self,
        column: str,
        window_size: int,
        operation: str = "mean",
        alias: str | None = None,
    ) -> LazyPIQuery:
        """Add a rolling window calculation.

        Args:
            column: Column to apply rolling window to
            window_size: Size of the rolling window
            operation: Rolling operation ("mean", "sum", "min", "max", "std")
            alias: Output column name

        Returns:
            Self for method chaining
        """
        output_name = alias or f"{column}_rolling_{operation}"

        ops = {
            "mean": lambda c, w: pl.col(c).rolling_mean(window_size=w),
            "sum": lambda c, w: pl.col(c).rolling_sum(window_size=w),
            "min": lambda c, w: pl.col(c).rolling_min(window_size=w),
            "max": lambda c, w: pl.col(c).rolling_max(window_size=w),
            "std": lambda c, w: pl.col(c).rolling_std(window_size=w),
        }

        if operation not in ops:
            raise ValueError(f"Unknown rolling operation: {operation}")

        expr = ops[operation](column, window_size).alias(output_name)

        self._operations.append(
            LazyOperation(
                name="rolling",
                func=lambda lf, e=expr: lf.with_columns([e]),
            )
        )
        return self

    def diff(
        self,
        column: str,
        alias: str | None = None,
    ) -> LazyPIQuery:
        """Calculate difference from previous value.

        Args:
            column: Column to calculate diff on
            alias: Output column name

        Returns:
            Self for method chaining
        """
        output_name = alias or f"{column}_diff"

        self._operations.append(
            LazyOperation(
                name="diff",
                func=lambda lf, c=column, n=output_name: lf.with_columns([
                    pl.col(c).diff().alias(n)
                ]),
            )
        )
        return self

    def pct_change(
        self,
        column: str,
        alias: str | None = None,
    ) -> LazyPIQuery:
        """Calculate percentage change from previous value.

        Args:
            column: Column to calculate pct_change on
            alias: Output column name

        Returns:
            Self for method chaining
        """
        output_name = alias or f"{column}_pct_change"

        self._operations.append(
            LazyOperation(
                name="pct_change",
                func=lambda lf, c=column, n=output_name: lf.with_columns([
                    pl.col(c).pct_change().alias(n)
                ]),
            )
        )
        return self

    def fill_null(
        self,
        column: str | None = None,
        value: Any = None,
        strategy: str = "forward",
    ) -> LazyPIQuery:
        """Fill null values.

        Args:
            column: Column to fill (None for all)
            value: Value to fill with
            strategy: Fill strategy ("forward", "backward", "mean", "zero")

        Returns:
            Self for method chaining
        """

        def fill_func(lf: pl.LazyFrame) -> pl.LazyFrame:
            if value is not None:
                if column:
                    return lf.with_columns([pl.col(column).fill_null(value)])
                else:
                    return lf.fill_null(value)
            elif strategy == "forward":
                if column:
                    return lf.with_columns([pl.col(column).forward_fill()])
                else:
                    return lf.select([pl.all().forward_fill()])
            elif strategy == "backward":
                if column:
                    return lf.with_columns([pl.col(column).backward_fill()])
                else:
                    return lf.select([pl.all().backward_fill()])
            elif strategy == "mean":
                if column:
                    return lf.with_columns([
                        pl.col(column).fill_null(pl.col(column).mean())
                    ])
                else:
                    return lf
            elif strategy == "zero":
                return lf.fill_null(0)
            else:
                return lf

        self._operations.append(LazyOperation(name="fill_null", func=fill_func))
        return self

    def head(self, n: int = 10) -> LazyPIQuery:
        """Limit to first n rows.

        Args:
            n: Number of rows

        Returns:
            Self for method chaining
        """
        self._operations.append(
            LazyOperation(
                name="head",
                func=lambda lf, limit=n: lf.head(limit),
            )
        )
        return self

    def tail(self, n: int = 10) -> LazyPIQuery:
        """Limit to last n rows.

        Args:
            n: Number of rows

        Returns:
            Self for method chaining
        """
        self._operations.append(
            LazyOperation(
                name="tail",
                func=lambda lf, limit=n: lf.tail(limit),
            )
        )
        return self

    def collect(self) -> pl.DataFrame:
        """Execute the query and collect results.

        Returns:
            DataFrame with results
        """
        # Fetch initial data
        if self._query_type == "recorded":
            df = self._client.recorded_values(
                self._tags,
                start=self._start,
                end=self._end,
            )
        elif self._query_type == "interpolated":
            df = self._client.interpolated_values(
                self._tags,
                start=self._start,
                end=self._end,
                interval=self._interval or "1h",
            )
        else:
            df = self._client.recorded_values(
                self._tags,
                start=self._start,
                end=self._end,
            )

        # Convert to lazy and apply operations
        lf = df.lazy()

        for op in self._operations:
            lf = op.func(lf)

        return lf.collect()

    def to_lazy_frame(self) -> pl.LazyFrame:
        """Get the LazyFrame without collecting.

        Returns:
            LazyFrame with operations
        """
        # Fetch initial data
        if self._query_type == "recorded":
            df = self._client.recorded_values(
                self._tags,
                start=self._start,
                end=self._end,
            )
        elif self._query_type == "interpolated":
            df = self._client.interpolated_values(
                self._tags,
                start=self._start,
                end=self._end,
                interval=self._interval or "1h",
            )
        else:
            df = self._client.recorded_values(
                self._tags,
                start=self._start,
                end=self._end,
            )

        # Convert to lazy and apply operations
        lf = df.lazy()

        for op in self._operations:
            lf = op.func(lf)

        return lf

    def explain(self) -> str:
        """Get the query execution plan.

        Returns:
            String representation of the query plan
        """
        operations = [op.name for op in self._operations]
        return (
            f"LazyPIQuery Plan:\n"
            f"  Tags: {self._tags}\n"
            f"  Type: {self._query_type}\n"
            f"  Range: {self._start} to {self._end}\n"
            f"  Operations: {' -> '.join(operations) if operations else 'none'}"
        )

    def __repr__(self) -> str:
        """String representation."""
        return self.explain()
