"""Configuration management for PIPolars library.

This module provides configuration classes using Pydantic for validation
and supports loading from environment variables and configuration files.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthMethod(str, Enum):
    """Authentication methods for PI System connection."""

    WINDOWS = "windows"
    """Use Windows integrated authentication (NTLM/Kerberos)."""

    EXPLICIT = "explicit"
    """Use explicit username/password authentication."""


class CacheBackend(str, Enum):
    """Cache storage backends."""

    NONE = "none"
    """No caching."""

    MEMORY = "memory"
    """In-memory cache (lost on restart)."""

    SQLITE = "sqlite"
    """SQLite database cache."""

    ARROW = "arrow"
    """Apache Arrow IPC file cache."""


class PIServerConfig(BaseSettings):
    """Configuration for PI Data Archive connection.

    Attributes:
        host: PI Server hostname or IP address
        port: PI Server port (default: 5450)
        timeout: Connection timeout in seconds
        auth_method: Authentication method to use
        username: Username for explicit authentication
        password: Password for explicit authentication
    """

    model_config = SettingsConfigDict(
        env_prefix="PI_SERVER_",
        env_file=".env",
        extra="ignore",
    )

    host: str = Field(description="PI Server hostname or IP address")
    port: int = Field(default=5450, ge=1, le=65535)
    timeout: int = Field(default=30, ge=1, le=300, description="Connection timeout in seconds")
    auth_method: AuthMethod = Field(default=AuthMethod.WINDOWS)
    username: str | None = Field(default=None, description="Username for explicit auth")
    password: SecretStr | None = Field(default=None, description="Password for explicit auth")

    @model_validator(mode="after")
    def validate_explicit_auth(self) -> "PIServerConfig":
        """Validate that username/password are provided for explicit auth."""
        if self.auth_method == AuthMethod.EXPLICIT:
            if not self.username or not self.password:
                raise ValueError(
                    "Username and password are required for explicit authentication"
                )
        return self


class AFServerConfig(BaseSettings):
    """Configuration for AF Server connection.

    Attributes:
        host: AF Server hostname (if different from PI Server)
        database: Default AF Database name
        timeout: Connection timeout in seconds
    """

    model_config = SettingsConfigDict(
        env_prefix="AF_SERVER_",
        env_file=".env",
        extra="ignore",
    )

    host: str | None = Field(default=None, description="AF Server hostname")
    database: str | None = Field(default=None, description="Default AF Database name")
    timeout: int = Field(default=30, ge=1, le=300)


class CacheConfig(BaseSettings):
    """Configuration for data caching.

    Attributes:
        backend: Cache storage backend to use
        path: Path for file-based cache backends
        max_size_mb: Maximum cache size in megabytes
        ttl_hours: Time-to-live for cached data in hours
        compression: Enable compression for cached data
    """

    model_config = SettingsConfigDict(
        env_prefix="PIPOLARS_CACHE_",
        env_file=".env",
        extra="ignore",
    )

    backend: CacheBackend = Field(default=CacheBackend.NONE)
    path: Path = Field(default=Path.home() / ".pipolars" / "cache")
    max_size_mb: int = Field(default=1024, ge=0, description="Max cache size in MB")
    ttl_hours: int = Field(default=24, ge=0, description="Cache TTL in hours")
    compression: bool = Field(default=True, description="Enable compression")

    @property
    def ttl(self) -> timedelta:
        """Get TTL as a timedelta."""
        return timedelta(hours=self.ttl_hours)


class QueryConfig(BaseSettings):
    """Configuration for PI queries.

    Attributes:
        max_points_per_query: Maximum number of points in a single query
        default_page_size: Default page size for paginated queries
        max_values_per_request: Maximum values per request
        parallel_requests: Number of parallel requests for bulk operations
        retry_attempts: Number of retry attempts for failed requests
        retry_delay: Delay between retries in seconds
    """

    model_config = SettingsConfigDict(
        env_prefix="PIPOLARS_QUERY_",
        env_file=".env",
        extra="ignore",
    )

    max_points_per_query: int = Field(default=1000, ge=1, le=10000)
    default_page_size: int = Field(default=10000, ge=100, le=1000000)
    max_values_per_request: int = Field(default=150000, ge=1000, le=1000000)
    parallel_requests: int = Field(default=4, ge=1, le=32)
    retry_attempts: int = Field(default=3, ge=0, le=10)
    retry_delay: float = Field(default=1.0, ge=0.1, le=60.0)


class PolarsConfig(BaseSettings):
    """Configuration for Polars DataFrame output.

    Attributes:
        timestamp_column: Name of the timestamp column
        value_column: Name of the value column
        quality_column: Name of the quality column
        tag_column: Name of the tag column (for multi-tag queries)
        include_quality: Include quality column by default
        timezone: Default timezone for timestamps
    """

    model_config = SettingsConfigDict(
        env_prefix="PIPOLARS_POLARS_",
        env_file=".env",
        extra="ignore",
    )

    timestamp_column: str = Field(default="timestamp")
    value_column: str = Field(default="value")
    quality_column: str = Field(default="quality")
    tag_column: str = Field(default="tag")
    include_quality: bool = Field(default=False)
    timezone: str = Field(default="UTC")

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate that the timezone is valid."""
        import zoneinfo

        try:
            zoneinfo.ZoneInfo(v)
        except KeyError as e:
            raise ValueError(f"Invalid timezone: {v}") from e
        return v


class PIConfig(BaseSettings):
    """Main configuration class for PIPolars library.

    This class aggregates all configuration sections and provides
    a unified interface for configuration management.

    Example:
        >>> config = PIConfig(
        ...     server=PIServerConfig(host="my-pi-server"),
        ...     cache=CacheConfig(backend=CacheBackend.SQLITE),
        ... )
        >>> client = PIClient(config=config)
    """

    model_config = SettingsConfigDict(
        env_prefix="PIPOLARS_",
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    server: PIServerConfig = Field(default_factory=lambda: PIServerConfig(host="localhost"))
    af: AFServerConfig = Field(default_factory=AFServerConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    query: QueryConfig = Field(default_factory=QueryConfig)
    polars: PolarsConfig = Field(default_factory=PolarsConfig)

    # Global settings
    debug: bool = Field(default=False, description="Enable debug mode")
    log_level: str = Field(default="INFO", description="Logging level")

    @classmethod
    def from_file(cls, path: str | Path) -> "PIConfig":
        """Load configuration from a TOML or JSON file.

        Args:
            path: Path to the configuration file

        Returns:
            PIConfig instance with loaded configuration
        """
        import json

        path = Path(path)

        if path.suffix == ".toml":
            with open(path, "rb") as f:
                data = tomllib.load(f)
        elif path.suffix == ".json":
            with open(path) as f:
                data = json.load(f)
        else:
            raise ValueError(f"Unsupported config file format: {path.suffix}")

        return cls.model_validate(data)

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to a dictionary.

        Sensitive fields like passwords are masked.
        """
        data = self.model_dump()
        # Mask sensitive fields
        if data.get("server", {}).get("password"):
            data["server"]["password"] = "***"
        return data
