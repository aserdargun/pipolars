# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PIPolars is a Python library for extracting data from OSIsoft PI System and converting it to Polars DataFrames. It requires Windows with the PI AF SDK installed.

## Development Commands

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest

# Run unit tests only
uv run pytest tests/unit

# Run integration tests (requires PI connection)
PI_SERVER=my-server uv run pytest -m integration

# Run tests with coverage
uv run pytest --cov=pipolars --cov-report=html

# Type checking
uv run mypy src

# Linting
uv run ruff check src
```

## Architecture

The codebase follows a layered architecture in `src/pipolars/`:

- **api/** - User-facing API layer
  - `client.py` - `PIClient` class, main entry point for users
  - `query.py` - Fluent query builder (`PIQuery`) for method chaining
  - `lazy.py` - Polars LazyFrame support

- **connection/** - PI System connectivity
  - `server.py` - PI Data Archive connections
  - `af_database.py` - AF Database connections
  - `sdk.py` - OSIsoft AF SDK wrapper using pythonnet
  - `auth.py` - Authentication handlers (Windows/explicit)

- **extraction/** - Data retrieval from PI
  - `points.py` - Single PI Point extraction
  - `bulk.py` - Bulk operations for multiple tags
  - `attributes.py`, `elements.py`, `events.py` - AF-specific extractors

- **transform/** - Data conversion to Polars
  - `converters.py` - PI types to Polars conversion
  - `timestamps.py` - Timestamp normalization
  - `digital_states.py` - Digital state value handling

- **cache/** - Result caching layer
  - `storage.py` - Backend implementations (Memory, SQLite, Arrow IPC)
  - `strategies.py` - TTL and eviction strategies

- **core/** - Shared types and configuration
  - `config.py` - Pydantic configuration models (`PIConfig`, `PIServerConfig`, `CacheConfig`, etc.)
  - `types.py` - Enums (`RetrievalMode`, `SummaryType`, `CacheBackend`) and data classes (`PIValue`, `TimeRange`)
  - `exceptions.py` - Exception hierarchy rooted at `PIPolarsError`

## Key Patterns

- Uses Pydantic v2 for configuration validation
- pythonnet (clr) is used for .NET interop with the PI AF SDK
- Strict mypy type checking is enforced
- Integration tests are marked with `@pytest.mark.integration`
- PI time expressions like `*-1h`, `*-1d`, `t` (today), `y` (yesterday) are supported throughout
