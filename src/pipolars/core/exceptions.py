"""Custom exceptions for PIPolars library.

This module defines a hierarchy of exceptions for handling various
error conditions when working with PI System data.
"""

from __future__ import annotations

from typing import Any


class PIPolarsError(Exception):
    """Base exception for all PIPolars errors.

    All custom exceptions in the library inherit from this class,
    making it easy to catch any PIPolars-related error.

    Attributes:
        message: Human-readable error message
        details: Optional dictionary with additional error context
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({detail_str})"
        return self.message


class PIConnectionError(PIPolarsError):
    """Raised when connection to PI System fails.

    This exception is raised when:
    - PI Server is unreachable
    - Authentication fails
    - AF Database connection fails
    - Network timeout occurs
    """

    def __init__(
        self,
        message: str,
        server: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if server:
            details["server"] = server
        super().__init__(message, details)
        self.server = server


class PIAuthenticationError(PIConnectionError):
    """Raised when PI System authentication fails.

    This exception is raised when:
    - Invalid credentials provided
    - User lacks permissions
    - Kerberos/NTLM authentication fails
    """

    pass


class PIDataError(PIPolarsError):
    """Raised when data retrieval or conversion fails.

    This exception is raised when:
    - Requested tag doesn't exist
    - Data type conversion fails
    - Invalid time range specified
    - Bulk operation partially fails
    """

    def __init__(
        self,
        message: str,
        tag: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if tag:
            details["tag"] = tag
        super().__init__(message, details)
        self.tag = tag


class PIPointNotFoundError(PIDataError):
    """Raised when a PI Point (tag) cannot be found.

    This exception is raised when:
    - Tag name doesn't exist in the PI Data Archive
    - Tag was deleted or renamed
    - User lacks access to the tag
    """

    def __init__(self, tag: str, server: str | None = None) -> None:
        details = {}
        if server:
            details["server"] = server
        super().__init__(f"PI Point not found: {tag}", tag=tag, details=details)


class PIQueryError(PIPolarsError):
    """Raised when a PI query is invalid or fails.

    This exception is raised when:
    - Invalid time expression provided
    - Query syntax error
    - Query timeout
    - Too many results requested
    """

    def __init__(
        self,
        message: str,
        query: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if query:
            details["query"] = query
        super().__init__(message, details)
        self.query = query


class PITimeParseError(PIQueryError):
    """Raised when a time expression cannot be parsed.

    This exception is raised when:
    - Invalid relative time expression (e.g., "*-invalid")
    - Malformed absolute timestamp
    - Unsupported time format
    """

    def __init__(self, expression: str, reason: str | None = None) -> None:
        details = {}
        if reason:
            details["reason"] = reason
        super().__init__(
            f"Cannot parse time expression: {expression}",
            query=expression,
            details=details,
        )


class PIBulkOperationError(PIDataError):
    """Raised when a bulk operation partially fails.

    This exception contains information about which operations
    succeeded and which failed.

    Attributes:
        succeeded: List of tags that succeeded
        failed: Dictionary mapping failed tags to their error messages
    """

    def __init__(
        self,
        message: str,
        succeeded: list[str],
        failed: dict[str, str],
    ) -> None:
        details = {
            "succeeded_count": len(succeeded),
            "failed_count": len(failed),
        }
        super().__init__(message, details=details)
        self.succeeded = succeeded
        self.failed = failed


class PIConfigurationError(PIPolarsError):
    """Raised when configuration is invalid.

    This exception is raised when:
    - Required configuration missing
    - Invalid configuration value
    - Configuration file parse error
    """

    pass


class PIAFSDKError(PIPolarsError):
    """Raised when the AF SDK encounters an error.

    This exception wraps errors from the underlying OSIsoft AF SDK
    and provides additional context.

    Attributes:
        sdk_error_code: The error code from AF SDK if available
        sdk_message: The original error message from AF SDK
    """

    def __init__(
        self,
        message: str,
        sdk_error_code: int | None = None,
        sdk_message: str | None = None,
    ) -> None:
        details: dict[str, Any] = {}
        if sdk_error_code is not None:
            details["sdk_error_code"] = sdk_error_code
        if sdk_message:
            details["sdk_message"] = sdk_message
        super().__init__(message, details)
        self.sdk_error_code = sdk_error_code
        self.sdk_message = sdk_message


class PICacheError(PIPolarsError):
    """Raised when cache operations fail.

    This exception is raised when:
    - Cache read/write fails
    - Cache corruption detected
    - Cache storage is full
    """

    pass


class PITransformError(PIPolarsError):
    """Raised when data transformation fails.

    This exception is raised when:
    - Cannot convert PI type to Polars type
    - Schema mismatch
    - Data validation fails
    """

    pass
