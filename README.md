# PIPolars

High-performance PI System data extraction library with Polars DataFrames.

PIPolars is a modern Python library for extracting data from OSIsoft PI System and converting it to Polars DataFrames for efficient data science workflows. It's designed to be faster, more memory-efficient, and more Pythonic than existing alternatives.

## Features

- **Polars DataFrames**: 10-100x faster than pandas for many operations
- **Modern Python**: Full type hints, Pydantic configuration, Python 3.10+
- **Efficient Bulk Operations**: Native bulk API support for extracting multiple tags
- **Lazy Evaluation**: Polars LazyFrame support for query optimization
- **Caching**: SQLite and Arrow IPC caching for reduced server load
- **Fluent Query API**: Method chaining for readable, declarative queries
- **uv Compatible**: Modern package management with Astral's uv

## Requirements

- Python 3.10+
- Windows (for PI AF SDK)
- OSIsoft PI AF SDK 2.x (.NET 4.8)

## Installation

### Using uv (recommended)

```bash
uv add pipolars
```

### Using pip

```bash
pip install pipolars
```

### Development Installation

```bash
git clone https://github.com/pipolars/pipolars.git
cd pipolars
uv sync
```

## Quick Start

### Basic Usage

```python
from pipolars import PIClient

# Connect to PI Server
with PIClient("my-pi-server") as client:
    # Get current value
    df = client.snapshot("SINUSOID")
    print(df)

    # Get last 24 hours of data
    df = client.recorded_values("SINUSOID", start="*-1d", end="*")
    print(f"Retrieved {len(df)} values")

    # Get multiple tags at once
    df = client.recorded_values(
        ["TAG1", "TAG2", "TAG3"],
        start="*-1h",
        end="*",
    )
```

### Query Builder (Fluent API)

```python
from pipolars import PIClient, SummaryType

with PIClient("my-pi-server") as client:
    # Fluent query building
    df = (
        client.query("SINUSOID")
        .last(hours=24)
        .interpolated(interval="15m")
        .with_quality()
        .to_dataframe()
    )

    # Multi-tag query with pivot
    df = (
        client.query(["TAG1", "TAG2", "TAG3"])
        .time_range("*-1d", "*")
        .interpolated(interval="1h")
        .pivot()  # Tags become columns
        .to_dataframe()
    )

    # Summary statistics
    df = (
        client.query("SINUSOID")
        .last(days=7)
        .summary(SummaryType.AVERAGE, SummaryType.MAXIMUM, SummaryType.MINIMUM)
        .to_dataframe()
    )
```

### Data Processing with Polars

```python
import polars as pl
from pipolars import PIClient

with PIClient("my-pi-server") as client:
    # Get data
    df = client.interpolated_values("SINUSOID", "*-1d", "*", interval="5m")

    # Process with Polars
    result = (
        df.with_columns([
            pl.col("value").rolling_mean(window_size=12).alias("rolling_avg"),
            pl.col("value").diff().alias("change"),
        ])
        .filter(pl.col("value") > 50)
    )
```

### Caching

```python
from pipolars import PIClient, PIConfig
from pipolars.core.config import CacheBackend, CacheConfig, PIServerConfig

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

    # Second query - from cache (faster)
    df2 = client.recorded_values("SINUSOID", "*-1h", "*")

    # Check cache stats
    print(client.cache_stats())
```

### Standalone Script with uv

You can run PIPolars scripts with uv's inline dependencies:

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pipolars"]
# ///

from pipolars import PIClient

with PIClient("my-pi-server") as client:
    df = client.last("SINUSOID", hours=24)
    print(df.describe())
```

Run with:

```bash
uv run my_script.py
```

## Configuration

### Environment Variables

```bash
# PI Server configuration
PI_SERVER_HOST=my-pi-server
PI_SERVER_PORT=5450
PI_SERVER_TIMEOUT=30

# Cache configuration
PIPOLARS_CACHE_BACKEND=sqlite
PIPOLARS_CACHE_TTL_HOURS=24

# Polars configuration
PIPOLARS_POLARS_TIMEZONE=America/New_York
```

### Configuration File

```python
from pipolars import PIConfig

config = PIConfig.from_file("config.toml")
```

## API Reference

### PIClient

Main client for PI data extraction.

| Method | Description |
|--------|-------------|
| `snapshot(tag)` | Get current value |
| `snapshots(tags)` | Get current values for multiple tags |
| `recorded_values(tags, start, end)` | Get recorded values |
| `interpolated_values(tags, start, end, interval)` | Get interpolated values |
| `plot_values(tag, start, end, intervals)` | Get plot-optimized values |
| `summary(tags, start, end, summary_types)` | Get summary statistics |
| `summaries(tag, start, end, interval, summary_types)` | Get interval summaries |
| `query(tags)` | Start building a query |
| `last(tags, hours, days, minutes)` | Convenience method for recent data |
| `today(tags)` | Get today's data |

### Time Expressions

PIPolars supports PI time expressions:

| Expression | Description |
|------------|-------------|
| `*` | Now |
| `*-1h` | 1 hour ago |
| `*-1d` | 1 day ago |
| `t` | Today at midnight |
| `y` | Yesterday at midnight |
| `2024-01-15` | Absolute date |
| `2024-01-15T10:00:00` | Absolute datetime |

### Summary Types

```python
from pipolars import SummaryType

SummaryType.TOTAL      # Sum of values
SummaryType.AVERAGE    # Time-weighted average
SummaryType.MINIMUM    # Minimum value
SummaryType.MAXIMUM    # Maximum value
SummaryType.RANGE      # Max - Min
SummaryType.STD_DEV    # Standard deviation
SummaryType.COUNT      # Number of values
SummaryType.PERCENT_GOOD  # Percentage of good values
```

## Comparison with PIconnect

| Feature | PIconnect | PIPolars |
|---------|-----------|----------|
| DataFrame output | pandas | **Polars** (faster) |
| Bulk operations | Limited | **Native bulk API** |
| Lazy evaluation | No | **Yes** (LazyFrame) |
| Caching | No | **SQLite + Arrow** |
| Type hints | Partial | **Full coverage** |
| Async support | No | **Optional async** |
| Package manager | pip | **uv compatible** |
| Memory efficiency | Medium | **High** (zero-copy) |
| Query builder | Basic | **Fluent API** |

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/pipolars/pipolars.git
cd pipolars

# Install with dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Run type checking
uv run mypy src

# Run linting
uv run ruff check src
```

### Running Tests

```bash
# Unit tests only
uv run pytest tests/unit

# Integration tests (requires PI connection)
PI_SERVER=my-server uv run pytest -m integration

# With coverage
uv run pytest --cov=pipolars --cov-report=html
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## Acknowledgments

- [OSIsoft/AVEVA](https://www.aveva.com/) for PI System
- [Polars](https://pola.rs/) for the amazing DataFrame library
- [Astral](https://astral.sh/) for uv and modern Python tooling
- [PIconnect](https://github.com/Hugovdberg/PIconnect) for inspiration
