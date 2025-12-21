Changelog
=========

All notable changes to PIPolars will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
------------

Added
~~~~~

- Initial release

[0.1.0] - 2024-12-21
--------------------

Added
~~~~~

- Initial release of PIPolars
- ``PIClient`` class for PI System data extraction
- ``PIQuery`` fluent query builder with method chaining
- Support for recorded, interpolated, plot, and summary values
- Multi-tag bulk operations with parallelization
- Polars DataFrame output with optional quality information
- Cache backends: Memory, SQLite, and Arrow IPC
- Pydantic-based configuration with environment variable support
- Comprehensive type hints and mypy strict mode compliance
- PI time expression support (``*-1d``, ``t``, ``y``, etc.)
- ``AFTime`` helper class for programmatic time construction
- Exception hierarchy rooted at ``PIPolarsError``
- Windows integration via pythonnet and PI AF SDK
- Documentation with Sphinx and Read the Docs support

Features
~~~~~~~~

- **PIClient**: Main entry point with methods for:
  - ``snapshot()`` / ``snapshots()`` - Current values
  - ``recorded_values()`` - Archived data
  - ``interpolated_values()`` - Regular interval data
  - ``plot_values()`` - Plot-optimized data
  - ``summary()`` / ``summaries()`` - Statistical summaries
  - ``search_tags()`` / ``tag_exists()`` / ``tag_info()`` - Tag operations

- **PIQuery**: Fluent query builder with:
  - Time range methods (``time_range()``, ``last()``, ``today()``, etc.)
  - Query type methods (``recorded()``, ``interpolated()``, ``summary()``)
  - Option methods (``with_quality()``, ``pivot()``, ``limit()``)
  - Execution methods (``to_dataframe()``, ``to_lazy_frame()``)

- **Configuration**: Pydantic settings with:
  - ``PIServerConfig`` - Server connection settings
  - ``AFServerConfig`` - AF Database settings
  - ``CacheConfig`` - Caching behavior
  - ``QueryConfig`` - Query options
  - ``PolarsConfig`` - DataFrame output settings

- **Caching**: Multiple backends:
  - Memory (LRU with TTL)
  - SQLite (persistent, compressed)
  - Arrow IPC (native Polars format)

Dependencies
~~~~~~~~~~~~

- polars >= 1.17.0
- pythonnet >= 3.0.3
- pydantic >= 2.10.0
- pydantic-settings >= 2.6.0
- pyarrow >= 18.0.0

Requirements
~~~~~~~~~~~~

- Python 3.10+
- Windows with PI AF SDK 2.x
- .NET Framework 4.8

Known Issues
~~~~~~~~~~~~

- Documentation builds require mocking pythonnet on non-Windows platforms
- LazyFrame support is limited (converts to DataFrame internally)

Future Plans
~~~~~~~~~~~~

- Async/await support for non-blocking queries
- AF Element and Event Frame extraction
- Streaming support for large datasets
- Additional cache backends (Redis, etc.)
