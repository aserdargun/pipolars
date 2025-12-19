#!/usr/bin/env python3
"""Basic usage examples for PIPolars.

This script demonstrates the fundamental usage patterns of PIPolars
for extracting PI System data as Polars DataFrames.

Usage:
    uv run examples/basic_usage.py

Or with inline script dependencies:
    # /// script
    # dependencies = ["pipolars"]
    # ///
"""

from pipolars import PIClient, PIConfig, SummaryType
from pipolars.core.config import CacheBackend, CacheConfig, PIServerConfig


def example_basic_connection() -> None:
    """Example: Basic connection and snapshot."""
    print("=" * 60)
    print("Example: Basic Connection and Snapshot")
    print("=" * 60)

    # Simple connection using hostname
    with PIClient("my-pi-server") as client:
        # Get current value
        df = client.snapshot("SINUSOID")
        print(f"Current value:\n{df}\n")


def example_recorded_values() -> None:
    """Example: Retrieving recorded values."""
    print("=" * 60)
    print("Example: Recorded Values")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get last 24 hours of data
        df = client.recorded_values(
            "SINUSOID",
            start="*-1d",
            end="*",
        )
        print(f"Recorded values (last 24h): {len(df)} rows")
        print(df.head())
        print()

        # Get data for multiple tags
        df_multi = client.recorded_values(
            ["TAG1", "TAG2", "TAG3"],
            start="*-1h",
            end="*",
        )
        print(f"Multi-tag data: {len(df_multi)} rows")
        print(df_multi.head())
        print()


def example_interpolated_values() -> None:
    """Example: Retrieving interpolated values."""
    print("=" * 60)
    print("Example: Interpolated Values")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get hourly interpolated values
        df = client.interpolated_values(
            "SINUSOID",
            start="*-1d",
            end="*",
            interval="1h",
        )
        print(f"Hourly values: {len(df)} rows")
        print(df.head())
        print()

        # Get 15-minute intervals for multiple tags, pivoted
        df_pivot = client.interpolated_values(
            ["TAG1", "TAG2"],
            start="*-4h",
            end="*",
            interval="15m",
            pivot=True,  # Tags become columns
        )
        print(f"Pivoted data:\n{df_pivot.head()}\n")


def example_summaries() -> None:
    """Example: Calculating summaries."""
    print("=" * 60)
    print("Example: Summary Statistics")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get summary for a single tag
        df = client.summary(
            "SINUSOID",
            start="*-7d",
            end="*",
            summary_types=[
                SummaryType.AVERAGE,
                SummaryType.MINIMUM,
                SummaryType.MAXIMUM,
                SummaryType.STD_DEV,
            ],
        )
        print(f"Weekly summary:\n{df}\n")

        # Get hourly summaries
        df_hourly = client.summaries(
            "SINUSOID",
            start="*-1d",
            end="*",
            interval="1h",
            summary_types=SummaryType.AVERAGE,
        )
        print(f"Hourly averages: {len(df_hourly)} rows")
        print(df_hourly.head())
        print()


def example_query_builder() -> None:
    """Example: Using the fluent query builder."""
    print("=" * 60)
    print("Example: Query Builder")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Fluent query building
        df = (
            client.query("SINUSOID")
            .last(hours=24)
            .interpolated(interval="30m")
            .with_quality()
            .to_dataframe()
        )
        print(f"Query result: {len(df)} rows")
        print(df.head())
        print()

        # Multi-tag query with pivot
        df_pivot = (
            client.query(["TAG1", "TAG2", "TAG3"])
            .time_range("*-2h", "*")
            .interpolated(interval="10m")
            .pivot()
            .to_dataframe()
        )
        print(f"Pivoted query:\n{df_pivot.head()}\n")


def example_convenience_methods() -> None:
    """Example: Using convenience methods."""
    print("=" * 60)
    print("Example: Convenience Methods")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get last N hours/days
        df_last = client.last("SINUSOID", hours=4)
        print(f"Last 4 hours: {len(df_last)} rows")

        # Get today's data
        df_today = client.today("SINUSOID")
        print(f"Today's data: {len(df_today)} rows")
        print()


def example_with_caching() -> None:
    """Example: Using caching for repeated queries."""
    print("=" * 60)
    print("Example: Caching")
    print("=" * 60)

    # Configure with SQLite cache
    config = PIConfig(
        server=PIServerConfig(host="my-pi-server"),
        cache=CacheConfig(
            backend=CacheBackend.SQLITE,
            ttl_hours=24,
        ),
    )

    with PIClient(config=config) as client:
        # First query - fetches from PI
        df1 = client.recorded_values("SINUSOID", "*-1h", "*")
        print(f"First query: {len(df1)} rows")

        # Second query - from cache (faster)
        df2 = client.recorded_values("SINUSOID", "*-1h", "*")
        print(f"Second query (cached): {len(df2)} rows")

        # Check cache stats
        stats = client.cache_stats()
        print(f"Cache stats: {stats}")
        print()


def example_data_processing() -> None:
    """Example: Processing data with Polars."""
    print("=" * 60)
    print("Example: Data Processing with Polars")
    print("=" * 60)

    import polars as pl

    with PIClient("my-pi-server") as client:
        # Get raw data
        df = client.interpolated_values(
            "SINUSOID",
            start="*-1d",
            end="*",
            interval="5m",
        )

        # Process with Polars
        result = (
            df.with_columns([
                pl.col("value").rolling_mean(window_size=12).alias("rolling_avg"),
                pl.col("value").diff().alias("change"),
                pl.col("value").pct_change().alias("pct_change"),
            ])
            .filter(pl.col("value") > 50)
            .select([
                "timestamp",
                "value",
                "rolling_avg",
                "change",
            ])
        )

        print(f"Processed data: {len(result)} rows")
        print(result.head(10))
        print()


def main() -> None:
    """Run all examples."""
    print("\n" + "=" * 60)
    print("PIPolars Basic Usage Examples")
    print("=" * 60 + "\n")

    # Note: These examples require a PI System connection
    # Uncomment the examples you want to run

    # example_basic_connection()
    # example_recorded_values()
    # example_interpolated_values()
    # example_summaries()
    # example_query_builder()
    # example_convenience_methods()
    # example_with_caching()
    # example_data_processing()

    print("Note: Uncomment examples in main() to run them.")
    print("Requires connection to a PI System.")


if __name__ == "__main__":
    main()
