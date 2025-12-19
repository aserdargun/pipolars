#!/usr/bin/env python3
"""Bulk data extraction examples for PIPolars.

This script demonstrates efficient bulk data extraction
from PI System for large-scale data retrieval.

Usage:
    uv run examples/bulk_extraction.py
"""

from pipolars import PIClient
from pipolars.core.types import SummaryType, TimeRange


def example_bulk_snapshots() -> None:
    """Example: Get snapshots for many tags at once."""
    print("=" * 60)
    print("Example: Bulk Snapshots")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get list of tags (e.g., from a search)
        tags = client.search_tags("REACTOR*", max_results=100)
        print(f"Found {len(tags)} tags")

        # Get all snapshots at once
        df = client.snapshots(tags)
        print(f"Snapshot data:\n{df}")


def example_bulk_historical() -> None:
    """Example: Bulk historical data extraction."""
    print("=" * 60)
    print("Example: Bulk Historical Data")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Define tags to extract
        tags = ["TAG1", "TAG2", "TAG3", "TAG4", "TAG5"]

        # Get recorded values for all tags
        df = client.recorded_values(
            tags,
            start="*-7d",
            end="*",
        )

        print(f"Total rows: {len(df)}")
        print(f"Unique tags: {df['tag'].n_unique()}")
        print(df.head())


def example_bulk_interpolated() -> None:
    """Example: Bulk interpolated data at regular intervals."""
    print("=" * 60)
    print("Example: Bulk Interpolated Data")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get hourly data for multiple tags, pivoted for analysis
        tags = ["TEMPERATURE", "PRESSURE", "FLOW", "LEVEL"]

        df = client.interpolated_values(
            tags,
            start="*-30d",
            end="*",
            interval="1h",
            pivot=True,  # Each tag becomes a column
        )

        print(f"Data shape: {df.shape}")
        print(f"Columns: {df.columns}")
        print(df.head())


def example_bulk_summaries() -> None:
    """Example: Bulk summary calculations."""
    print("=" * 60)
    print("Example: Bulk Summaries")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get summaries for multiple tags
        tags = client.search_tags("PROCESS*", max_results=50)

        df = client.summary(
            tags,
            start="*-7d",
            end="*",
            summary_types=[
                SummaryType.AVERAGE,
                SummaryType.MINIMUM,
                SummaryType.MAXIMUM,
                SummaryType.STD_DEV,
            ],
        )

        print(f"Summary data for {len(tags)} tags:")
        print(df)


def example_chunked_extraction() -> None:
    """Example: Chunked extraction for very large datasets."""
    print("=" * 60)
    print("Example: Chunked Extraction")
    print("=" * 60)

    import polars as pl

    with PIClient("my-pi-server") as client:
        # For very large tag lists, process in chunks
        all_tags = client.search_tags("*", max_results=1000)
        chunk_size = 100

        all_dfs = []

        for i in range(0, len(all_tags), chunk_size):
            chunk = all_tags[i : i + chunk_size]
            print(f"Processing chunk {i // chunk_size + 1}: {len(chunk)} tags")

            df = client.summary(
                chunk,
                start="*-1d",
                end="*",
                summary_types=SummaryType.AVERAGE,
            )

            all_dfs.append(df)

        # Combine all chunks
        result = pl.concat(all_dfs)
        print(f"\nTotal results: {len(result)} tags")
        print(result.head())


def example_parallel_time_ranges() -> None:
    """Example: Parallel extraction of different time ranges."""
    print("=" * 60)
    print("Example: Parallel Time Range Extraction")
    print("=" * 60)

    from concurrent.futures import ThreadPoolExecutor

    import polars as pl

    def extract_month(client: PIClient, month: int, year: int) -> pl.DataFrame:
        """Extract data for a specific month."""
        start = f"{year}-{month:02d}-01"
        if month == 12:
            end = f"{year + 1}-01-01"
        else:
            end = f"{year}-{month + 1:02d}-01"

        df = client.interpolated_values(
            "SINUSOID",
            start=start,
            end=end,
            interval="1h",
        )

        return df.with_columns(pl.lit(f"{year}-{month:02d}").alias("month"))

    # Note: For production use, consider connection pooling
    with PIClient("my-pi-server") as client:
        # Extract multiple months
        months = [(2024, m) for m in range(1, 7)]

        # Sequential extraction (safer with single connection)
        all_dfs = []
        for year, month in months:
            print(f"Extracting {year}-{month:02d}")
            df = extract_month(client, month, year)
            all_dfs.append(df)

        result = pl.concat(all_dfs)
        print(f"\nTotal rows: {len(result)}")
        print(result.group_by("month").len())


def example_large_time_range() -> None:
    """Example: Extracting large time ranges efficiently."""
    print("=" * 60)
    print("Example: Large Time Range Extraction")
    print("=" * 60)

    import polars as pl

    with PIClient("my-pi-server") as client:
        # For very long time ranges, use interpolated values
        # to control data volume

        # 1 year of hourly data
        df = client.interpolated_values(
            "SINUSOID",
            start="*-365d",
            end="*",
            interval="1h",
        )

        print(f"One year of hourly data: {len(df)} rows")

        # Calculate monthly statistics
        monthly = (
            df.with_columns([
                pl.col("timestamp").dt.strftime("%Y-%m").alias("month")
            ])
            .group_by("month")
            .agg([
                pl.col("value").mean().alias("avg"),
                pl.col("value").std().alias("std"),
                pl.col("value").min().alias("min"),
                pl.col("value").max().alias("max"),
            ])
            .sort("month")
        )

        print("\nMonthly statistics:")
        print(monthly)


def main() -> None:
    """Run bulk extraction examples."""
    print("\n" + "=" * 60)
    print("PIPolars Bulk Extraction Examples")
    print("=" * 60 + "\n")

    # Note: These examples require a PI System connection
    # Uncomment the examples you want to run

    # example_bulk_snapshots()
    # example_bulk_historical()
    # example_bulk_interpolated()
    # example_bulk_summaries()
    # example_chunked_extraction()
    # example_parallel_time_ranges()
    # example_large_time_range()

    print("Note: Uncomment examples in main() to run them.")
    print("Requires connection to a PI System.")


if __name__ == "__main__":
    main()
