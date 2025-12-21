"""
Test script for PIPolars library.

This script demonstrates how to connect to a PI Server and extract data
using the PIPolars library.

Usage:
    1. Update PI_SERVER with your PI Server hostname
    2. Update TAG_NAME with a valid PI Point on your server
    3. Run: uv run python examples/test_pipolars.py
"""

from pipolars import PIClient, SummaryType

# =============================================================================
# Configuration - Update these values for your environment
# =============================================================================
PI_SERVER = "GENCOPI"  # Replace with your PI Server hostname
TAG_NAME = "TFN.01MKA01CE074§XE01§OUT"  # Replace with a valid PI Point name
TAG_LIST = ["TFN.01LBA10FT905§ZE01§OUT", "TFN.01MKA10CT010§XE01§OUT", "TFN.01LBA10CT101§§YQ00_HW"]  # Replace with your tags


def main():
    """Main test function."""
    print("=" * 60)
    print("PIPolars Library Test")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # 1. Basic Connection
    # -------------------------------------------------------------------------
    print("\n1. Connecting to PI Server...")

    with PIClient(PI_SERVER) as client:
        print(f"   Connected to: {client.server_name}")
        print(f"   Is connected: {client.is_connected}")

        # ---------------------------------------------------------------------
        # 2. Search for Tags
        # ---------------------------------------------------------------------
        print("\n2. Searching for tags matching 'SIN*'...")
        tags = client.search_tags("SIN*", max_results=10)
        print(f"   Found {len(tags)} tags:")
        for tag in tags[:5]:  # Show first 5
            print(f"     - {tag}")

        # ---------------------------------------------------------------------
        # 3. Check if Tag Exists
        # ---------------------------------------------------------------------
        print(f"\n3. Checking if tag '{TAG_NAME}' exists...")
        exists = client.tag_exists(TAG_NAME)
        print(f"   Tag exists: {exists}")

        if not exists:
            print(f"\n   ERROR: Tag '{TAG_NAME}' not found. Please update TAG_NAME.")
            return

        # ---------------------------------------------------------------------
        # 4. Get Tag Info
        # ---------------------------------------------------------------------
        print(f"\n4. Getting info for tag '{TAG_NAME}'...")
        info = client.tag_info(TAG_NAME)
        print(f"   Name: {info['name']}")
        print(f"   Type: {info['point_type']}")
        print(f"   Description: {info['description']}")
        print(f"   Units: {info['engineering_units']}")

        # ---------------------------------------------------------------------
        # 5. Get Snapshot (Current Value)
        # ---------------------------------------------------------------------
        print(f"\n5. Getting snapshot value for '{TAG_NAME}'...")
        df = client.snapshot(TAG_NAME)
        print(df)

        # ---------------------------------------------------------------------
        # 6. Get Recorded Values (Last Hour)
        # ---------------------------------------------------------------------
        print(f"\n6. Getting recorded values for last hour...")
        df = client.recorded_values(TAG_NAME, start="*-1h", end="*")
        print(f"   Retrieved {len(df)} values")
        print(df.head())

        # ---------------------------------------------------------------------
        # 7. Get Recorded Values (Using convenience method)
        # ---------------------------------------------------------------------
        print(f"\n7. Using convenience method 'last()'...")
        df = client.last(TAG_NAME, hours=2)
        print(f"   Retrieved {len(df)} values from last 2 hours")
        print(df.head())

        # ---------------------------------------------------------------------
        # 8. Get Interpolated Values
        # ---------------------------------------------------------------------
        print(f"\n8. Getting interpolated values (15-minute intervals)...")
        df = client.interpolated_values(
            TAG_NAME,
            start="*-4h",
            end="*",
            interval="15m"
        )
        print(f"   Retrieved {len(df)} interpolated values")
        print(df)

        # ---------------------------------------------------------------------
        # 9. Get Plot Values
        # ---------------------------------------------------------------------
        print(f"\n9. Getting plot-optimized values...")
        df = client.plot_values(TAG_NAME, start="*-1d", end="*", intervals=100)
        print(f"   Retrieved {len(df)} plot values")
        print(df.head())

        # ---------------------------------------------------------------------
        # 10. Get Summary Statistics
        # ---------------------------------------------------------------------
        print(f"\n10. Getting summary statistics for last 24 hours...")
        df = client.summary(
            TAG_NAME,
            start="*-1d",
            end="*",
            summary_types=[
                SummaryType.AVERAGE,
                SummaryType.MINIMUM,
                SummaryType.MAXIMUM,
                SummaryType.STD_DEV,
                SummaryType.COUNT,
            ]
        )
        print(df)

        # ---------------------------------------------------------------------
        # 11. Get Time-Series Summaries
        # ---------------------------------------------------------------------
        print(f"\n11. Getting hourly summaries for last 8 hours...")
        df = client.summaries(
            TAG_NAME,
            start="*-8h",
            end="*",
            interval="1h",
            summary_types=[SummaryType.AVERAGE, SummaryType.MAXIMUM]
        )
        print(df)

        # ---------------------------------------------------------------------
        # 12. Multiple Tags (if available)
        # ---------------------------------------------------------------------
        print(f"\n12. Getting data for multiple tags...")
        # First check which tags exist
        existing_tags = [t for t in TAG_LIST if client.tag_exists(t)]

        if len(existing_tags) >= 2:
            print(f"   Using tags: {existing_tags}")
            df = client.recorded_values(
                existing_tags,
                start="*-1h",
                end="*"
            )
            print(f"   Retrieved {len(df)} total values")
            print(df.head(10))

            # Pivot format (wide)
            print("\n   Pivot format (wide):")
            df_pivot = client.recorded_values(
                existing_tags,
                start="*-1h",
                end="*",
                pivot=True
            )
            print(df_pivot.head(10))
        else:
            print(f"   Skipping - need at least 2 existing tags")

        # ---------------------------------------------------------------------
        # 13. Using Query Builder (Fluent API)
        # ---------------------------------------------------------------------
        print(f"\n13. Using query builder...")
        df = (
            client.query(TAG_NAME)
            .time_range("*-2h", "*")
            .recorded()
            .to_dataframe()
        )
        print(f"   Retrieved {len(df)} values using query builder")
        print(df.head())

        # ---------------------------------------------------------------------
        # 14. Today's Data
        # ---------------------------------------------------------------------
        print(f"\n14. Getting today's data...")
        df = client.today(TAG_NAME)
        print(f"   Retrieved {len(df)} values for today")
        print(df.head())

        # ---------------------------------------------------------------------
        # 15. Cache Stats
        # ---------------------------------------------------------------------
        print(f"\n15. Cache statistics...")
        stats = client.cache_stats()
        print(f"   {stats}")

    print("\n" + "=" * 60)
    print("Test completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
