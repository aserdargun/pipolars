#!/usr/bin/env python3
"""Data science workflow examples with PIPolars.

This script demonstrates using PIPolars in typical data science
workflows, including feature engineering, anomaly detection,
and time series analysis.

Usage:
    uv run examples/data_science_workflow.py
"""

import polars as pl

from pipolars import PIClient


def example_feature_engineering() -> None:
    """Example: Feature engineering for ML models."""
    print("=" * 60)
    print("Example: Feature Engineering")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get process data
        df = client.interpolated_values(
            ["TEMPERATURE", "PRESSURE", "FLOW"],
            start="*-7d",
            end="*",
            interval="5m",
            pivot=True,
        )

        # Feature engineering with Polars
        features = df.with_columns([
            # Rolling statistics
            pl.col("TEMPERATURE").rolling_mean(window_size=12).alias("temp_rolling_avg"),
            pl.col("TEMPERATURE").rolling_std(window_size=12).alias("temp_rolling_std"),
            pl.col("PRESSURE").rolling_mean(window_size=12).alias("pressure_rolling_avg"),

            # Lag features
            pl.col("TEMPERATURE").shift(1).alias("temp_lag_1"),
            pl.col("TEMPERATURE").shift(6).alias("temp_lag_6"),

            # Difference features
            pl.col("TEMPERATURE").diff().alias("temp_diff"),
            pl.col("PRESSURE").diff().alias("pressure_diff"),

            # Rate of change
            pl.col("FLOW").pct_change().alias("flow_pct_change"),

            # Hour of day (cyclic encoding)
            (pl.col("timestamp").dt.hour() * 2 * 3.14159 / 24).sin().alias("hour_sin"),
            (pl.col("timestamp").dt.hour() * 2 * 3.14159 / 24).cos().alias("hour_cos"),

            # Day of week
            pl.col("timestamp").dt.weekday().alias("day_of_week"),
        ])

        # Remove rows with nulls from rolling calculations
        features = features.drop_nulls()

        print(f"Features shape: {features.shape}")
        print(f"Feature columns: {features.columns}")
        print(features.head())


def example_anomaly_detection() -> None:
    """Example: Simple anomaly detection using Z-scores."""
    print("=" * 60)
    print("Example: Anomaly Detection")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get sensor data
        df = client.interpolated_values(
            "TEMPERATURE",
            start="*-30d",
            end="*",
            interval="1h",
        )

        # Calculate rolling statistics for anomaly detection
        result = (
            df.with_columns([
                pl.col("value").rolling_mean(window_size=24).alias("rolling_mean"),
                pl.col("value").rolling_std(window_size=24).alias("rolling_std"),
            ])
            .with_columns([
                # Z-score
                ((pl.col("value") - pl.col("rolling_mean")) / pl.col("rolling_std"))
                .alias("z_score")
            ])
            .with_columns([
                # Flag anomalies (|z-score| > 3)
                (pl.col("z_score").abs() > 3).alias("is_anomaly")
            ])
        )

        # Get anomalies
        anomalies = result.filter(pl.col("is_anomaly"))

        print(f"Total data points: {len(result)}")
        print(f"Anomalies detected: {len(anomalies)}")

        if len(anomalies) > 0:
            print("\nAnomaly samples:")
            print(anomalies.head())


def example_time_series_analysis() -> None:
    """Example: Time series decomposition and analysis."""
    print("=" * 60)
    print("Example: Time Series Analysis")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get hourly data for analysis
        df = client.interpolated_values(
            "SINUSOID",
            start="*-30d",
            end="*",
            interval="1h",
        )

        # Resample to daily for trend analysis
        daily = (
            df.group_by_dynamic("timestamp", every="1d")
            .agg([
                pl.col("value").mean().alias("daily_avg"),
                pl.col("value").min().alias("daily_min"),
                pl.col("value").max().alias("daily_max"),
                pl.col("value").std().alias("daily_std"),
            ])
        )

        # Calculate trend using moving average
        trend_analysis = daily.with_columns([
            pl.col("daily_avg").rolling_mean(window_size=7).alias("weekly_trend"),
            pl.col("daily_avg").diff().alias("daily_change"),
        ])

        # Volatility analysis
        volatility = daily.with_columns([
            (pl.col("daily_max") - pl.col("daily_min")).alias("daily_range"),
            (pl.col("daily_std") / pl.col("daily_avg") * 100).alias("cv_percent"),
        ])

        print("Daily statistics:")
        print(daily.head())
        print("\nTrend analysis:")
        print(trend_analysis.head())
        print("\nVolatility analysis:")
        print(volatility.describe())


def example_correlation_analysis() -> None:
    """Example: Correlation analysis between multiple tags."""
    print("=" * 60)
    print("Example: Correlation Analysis")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get multiple related process variables
        tags = ["TEMPERATURE", "PRESSURE", "FLOW", "LEVEL"]

        df = client.interpolated_values(
            tags,
            start="*-7d",
            end="*",
            interval="15m",
            pivot=True,
        )

        # Calculate correlation matrix using Polars
        # First, ensure we have no nulls
        df_clean = df.drop_nulls()

        # Calculate pairwise correlations
        correlations = {}
        for tag1 in tags:
            for tag2 in tags:
                if tag1 <= tag2:  # Avoid duplicates
                    corr = df_clean.select(
                        pl.corr(tag1, tag2).alias("correlation")
                    ).item()
                    correlations[f"{tag1} vs {tag2}"] = corr

        print("Correlation Matrix:")
        for pair, corr in correlations.items():
            print(f"  {pair}: {corr:.4f}")


def example_batch_analysis() -> None:
    """Example: Batch/campaign analysis."""
    print("=" * 60)
    print("Example: Batch Analysis")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Simulate batch analysis by grouping by time periods
        df = client.interpolated_values(
            ["TEMPERATURE", "PRESSURE"],
            start="*-30d",
            end="*",
            interval="5m",
            pivot=True,
        )

        # Group by day (simulating batches)
        batch_stats = (
            df.with_columns([
                pl.col("timestamp").dt.date().alias("batch_date")
            ])
            .group_by("batch_date")
            .agg([
                # Temperature statistics
                pl.col("TEMPERATURE").mean().alias("temp_avg"),
                pl.col("TEMPERATURE").std().alias("temp_std"),
                pl.col("TEMPERATURE").min().alias("temp_min"),
                pl.col("TEMPERATURE").max().alias("temp_max"),

                # Pressure statistics
                pl.col("PRESSURE").mean().alias("pressure_avg"),
                pl.col("PRESSURE").std().alias("pressure_std"),

                # Count
                pl.len().alias("sample_count"),
            ])
            .sort("batch_date")
        )

        print(f"Batch statistics ({len(batch_stats)} batches):")
        print(batch_stats)

        # Identify best and worst batches
        best_batch = batch_stats.sort("temp_std").head(1)
        worst_batch = batch_stats.sort("temp_std", descending=True).head(1)

        print("\nMost consistent batch (lowest temp std):")
        print(best_batch)
        print("\nLeast consistent batch (highest temp std):")
        print(worst_batch)


def example_export_data() -> None:
    """Example: Exporting data to various formats."""
    print("=" * 60)
    print("Example: Data Export")
    print("=" * 60)

    with PIClient("my-pi-server") as client:
        # Get data
        df = client.interpolated_values(
            "SINUSOID",
            start="*-1d",
            end="*",
            interval="1h",
        )

        # Export to various formats
        # CSV
        df.write_csv("output/sinusoid_data.csv")
        print("Exported to CSV")

        # Parquet (efficient for large datasets)
        df.write_parquet("output/sinusoid_data.parquet")
        print("Exported to Parquet")

        # JSON
        df.write_json("output/sinusoid_data.json")
        print("Exported to JSON")

        # Arrow IPC (for interoperability)
        df.write_ipc("output/sinusoid_data.arrow")
        print("Exported to Arrow IPC")

        print("\nAll exports completed successfully!")


def main() -> None:
    """Run data science workflow examples."""
    print("\n" + "=" * 60)
    print("PIPolars Data Science Workflow Examples")
    print("=" * 60 + "\n")

    # Note: These examples require a PI System connection
    # Uncomment the examples you want to run

    # example_feature_engineering()
    # example_anomaly_detection()
    # example_time_series_analysis()
    # example_correlation_analysis()
    # example_batch_analysis()
    # example_export_data()

    print("Note: Uncomment examples in main() to run them.")
    print("Requires connection to a PI System.")


if __name__ == "__main__":
    main()
