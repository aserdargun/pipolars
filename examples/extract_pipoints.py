"""
PI Data Extractor - Extract PI Points from PI Data Archive

This script extracts PI Points from PI Data Archive with comprehensive attributes.

Uses Windows authentication to connect to the server.
"""

import json
import sys
import io
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Fix encoding for Windows console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add pipolars to path
sys.path.insert(0, str(Path(__file__).parent / "pipolars" / "src"))

from pipolars.connection.server import PIServerConnection
from pipolars.core.config import PIServerConfig

# Global verbose flag for detailed logging
VERBOSE_LOGGING = True


def log_verbose(message: str) -> None:
    """Print message if verbose logging is enabled."""
    if VERBOSE_LOGGING:
        print(message)


def serialize_datetime(obj: Any) -> Any:
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def convert_net_datetime(net_datetime: Any) -> datetime | None:
    """Convert a .NET DateTime to Python datetime."""
    try:
        return datetime(
            net_datetime.Year,
            net_datetime.Month,
            net_datetime.Day,
            net_datetime.Hour,
            net_datetime.Minute,
            net_datetime.Second,
            net_datetime.Millisecond * 1000,
        )
    except Exception:
        return None


def extract_pi_points(pi_server: str, pattern: str = "*", max_count: int = 100000) -> list[dict[str, Any]]:
    """
    Extract all PI Points from the PI Data Archive with comprehensive attributes.

    Args:
        pi_server: PI Server name
        pattern: Tag search pattern (default: "*" for all tags)
        max_count: Maximum number of tags to retrieve

    Returns:
        List of PI Point dictionaries with all attributes and properties.
    """
    config = PIServerConfig(host=pi_server)

    points_list = []

    with PIServerConnection(config) as conn:
        print(f"Connected to PI Server: {conn.name}")

        # Search for all PI points
        print(f"Searching for PI Points with pattern: {pattern}")
        points = conn.search_points(pattern, max_count)
        print(f"Found {len(points)} PI Points")

        # List of all standard PI point attributes to retrieve
        point_attributes = [
            "pointid",
            "tag",
            "pointtype",
            "pointtypex",
            "descriptor",
            "engunits",
            "zero",
            "span",
            "displaydigits",
            "typicalvalue",
            "pointsource",
            "sourcetag",
            "step",
            "future",
            "compressing",
            "compdev",
            "compdevpercent",
            "compmin",
            "compmax",
            "excdev",
            "excdevpercent",
            "excmin",
            "excmax",
            "filtercode",
            "shutdown",
            "archiving",
            "scan",
            "location1",
            "location2",
            "location3",
            "location4",
            "location5",
            "userint1",
            "userint2",
            "userreal1",
            "userreal2",
            "digitalset",
            "ptclassname",
            "ptclassid",
            "ptsecurity",
            "datasecurity",
            "squession",
            "totalcode",
            "exdesc",
            "instrumenttag",
            "creationdate",
            "creator",
            "changedate",
            "changer",
            "ptowner",
            "ptgroup",
            "ptaccess",
            "recno",
        ]

        for i, point in enumerate(points):
            try:
                point_info: dict[str, Any] = {
                    "name": str(point.Name),
                    "id": int(point.ID) if hasattr(point, "ID") else None,
                }

                log_verbose(f"  [Extracting] {point_info['name']}")

                # Get all available attributes
                try:
                    attrs = point.GetAttributes(point_attributes)

                    # Map attributes to dictionary
                    for attr_name in point_attributes:
                        try:
                            if attrs.ContainsKey(attr_name):
                                value = attrs[attr_name]
                                # Convert .NET types to Python types
                                if value is not None:
                                    if hasattr(value, "ToString"):
                                        # Check if it's a numeric type
                                        try:
                                            str_val = str(value)
                                            if "." in str_val:
                                                value = float(value)
                                            else:
                                                value = int(value)
                                        except (ValueError, TypeError):
                                            value = str(value.ToString())
                                    else:
                                        value = value
                                point_info[attr_name] = value
                            else:
                                point_info[attr_name] = None
                        except Exception:
                            point_info[attr_name] = None

                except Exception as e:
                    point_info["attributes_error"] = str(e)
                    log_verbose(f"    -> Error getting attributes: {e}")

                # Get current value info
                try:
                    current_value = point.CurrentValue()
                    if current_value:
                        cv_val = current_value.Value
                        try:
                            cv_val = float(cv_val)
                        except (ValueError, TypeError):
                            cv_val = str(cv_val) if cv_val else None

                        point_info["current_value"] = {
                            "value": cv_val,
                            "timestamp": convert_net_datetime(current_value.Timestamp.LocalTime),
                            "is_good": bool(current_value.IsGood),
                        }
                except Exception:
                    point_info["current_value"] = None

                # Point class info
                try:
                    if hasattr(point, "PointClass") and point.PointClass:
                        point_info["point_class"] = {
                            "name": str(point.PointClass.Name) if hasattr(point.PointClass, "Name") else None,
                            "id": int(point.PointClass.ID) if hasattr(point.PointClass, "ID") else None,
                        }
                except Exception:
                    point_info["point_class"] = None

                # Server info
                try:
                    if hasattr(point, "Server") and point.Server:
                        point_info["server_name"] = str(point.Server.Name)
                except Exception:
                    point_info["server_name"] = None

                points_list.append(point_info)

                if (i + 1) % 500 == 0:
                    print(f"  Processed {i + 1}/{len(points)} points...")

            except Exception as e:
                print(f"  Error extracting point: {e}")
                # Add basic info even if detailed extraction fails
                try:
                    points_list.append({
                        "name": str(point.Name) if hasattr(point, "Name") else "Unknown",
                        "error": str(e),
                    })
                except Exception:
                    pass

    print("Disconnected from PI Server")
    return points_list


def main() -> None:
    """Main entry point for the PI Points extraction script."""
    # Configuration - modify these values for your environment
    PI_SERVER = "GENCOPI"  # PI Data Archive server name

    output_dir = Path(__file__).parent

    print("=" * 60)
    print("PI Points Extractor")
    print("=" * 60)
    print(f"PI Server: {PI_SERVER}")
    print("=" * 60)

    # Extract PI Points
    print("\nExtracting PI Points...")
    print("-" * 40)
    try:
        pi_points = extract_pi_points(PI_SERVER)
        print(f"\nExtracted {len(pi_points)} PI Points")

        # Save to JSON
        points_file = output_dir / "pipoints.json"
        with open(points_file, "w", encoding="utf-8") as f:
            json.dump(pi_points, f, indent=2, default=serialize_datetime, ensure_ascii=False)
        print(f"Saved PI Points to: {points_file}")

    except Exception as e:
        print(f"Error extracting PI Points: {e}")
        traceback.print_exc()
        pi_points = []

    # Summary
    print("\n" + "=" * 60)
    print("Extraction Complete!")
    print("=" * 60)
    print(f"PI Points extracted: {len(pi_points)}")
    print("=" * 60)


if __name__ == "__main__":
    # Configure logging to file
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("pipoints_extraction.log", mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        raise
