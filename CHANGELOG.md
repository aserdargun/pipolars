# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-XX-XX

### Added

- Initial release of PIPolars
- `PIClient` for connecting to PI Data Archive servers
- `PIQuery` fluent interface for building data extraction queries
- Polars DataFrame output for high-performance data processing
- Caching support with multiple backends:
  - In-memory cache
  - SQLite persistent cache
  - Arrow IPC file cache
- Windows authentication support
- PI time expression parsing (`*-1h`, `*-1d`, `t`, `y`, etc.)
- Support for recorded values, interpolated values, and plot values retrieval
- Summary calculations (average, min, max, total, count, etc.)
- Digital state value handling
- Comprehensive type hints with `py.typed` marker
