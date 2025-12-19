#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars>=1.17.0",
#     "pythonnet>=3.0.3",
#     "pydantic>=2.10.0",
#     "pydantic-settings>=2.6.0",
#     "pyarrow>=18.0.0",
# ]
# ///
"""Standalone PIPolars script using uv run.

This script demonstrates using PIPolars with uv's inline script
dependencies feature. Run with:

    uv run scripts/standalone_example.py

The script will automatically install dependencies in an isolated
environment.
"""

import sys
from pathlib import Path

# Add src to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import polars as pl

from pipolars import PIClient, SummaryType


def main() -> None:
    """Main function demonstrating standalone usage."""
    print("PIPolars Standalone Example")
    print("=" * 50)

    # Configuration via environment or command line
    import os

    server = os.environ.get("PI_SERVER", "localhost")
    tag = os.environ.get("PI_TAG", "SINUSOID")

    print(f"Connecting to: {server}")
    print(f"Tag: {tag}")

    try:
        with PIClient(server) as client:
            # Get last 24 hours of data
            print("\n1. Getting last 24 hours of data...")
            df = client.recorded_values(tag, start="*-1d", end="*")
            print(f"   Retrieved {len(df)} values")

            # Calculate statistics
            print("\n2. Calculating statistics...")
            stats = df.select([
                pl.col("value").mean().alias("mean"),
                pl.col("value").std().alias("std"),
                pl.col("value").min().alias("min"),
                pl.col("value").max().alias("max"),
            ])
            print(f"   Mean: {stats['mean'][0]:.2f}")
            print(f"   Std:  {stats['std'][0]:.2f}")
            print(f"   Min:  {stats['min'][0]:.2f}")
            print(f"   Max:  {stats['max'][0]:.2f}")

            # Get hourly averages
            print("\n3. Getting hourly averages...")
            hourly = client.summaries(
                tag,
                start="*-1d",
                end="*",
                interval="1h",
                summary_types=SummaryType.AVERAGE,
            )
            print(f"   Retrieved {len(hourly)} hourly averages")

            # Show sample
            print("\n4. Sample data (first 5 rows):")
            print(df.head())

    except Exception as e:
        print(f"\nError: {e}")
        print("\nNote: This script requires:")
        print("  1. PI AF SDK installed (.NET 4.8)")
        print("  2. Valid PI Server connection")
        print("\nSet environment variables:")
        print("  PI_SERVER=your-pi-server")
        print("  PI_TAG=your-tag-name")
        sys.exit(1)


if __name__ == "__main__":
    main()
