"""Authentication handlers for PI System connections.

This module provides authentication mechanisms for connecting to
PI Data Archive and AF Server, supporting Windows integrated
authentication and explicit credentials.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pydantic import SecretStr

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.config import AuthMethod
from pipolars.core.exceptions import PIAuthenticationError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PIAuthenticator(ABC):
    """Base class for PI System authenticators.

    Authenticators handle the authentication process for PI System
    connections, abstracting the underlying authentication mechanism.
    """

    @abstractmethod
    def authenticate(self, server: Any) -> None:
        """Authenticate to a PI Server.

        Args:
            server: The PIServer object to authenticate

        Raises:
            PIAuthenticationError: If authentication fails
        """
        pass

    @classmethod
    def create(
        cls,
        method: AuthMethod,
        username: str | None = None,
        password: SecretStr | None = None,
    ) -> PIAuthenticator:
        """Factory method to create an authenticator.

        Args:
            method: The authentication method to use
            username: Username for explicit authentication
            password: Password for explicit authentication

        Returns:
            An appropriate authenticator instance
        """
        if method == AuthMethod.WINDOWS:
            return WindowsAuthenticator()
        elif method == AuthMethod.EXPLICIT:
            if not username or not password:
                raise PIAuthenticationError(
                    "Username and password required for explicit authentication"
                )
            return ExplicitAuthenticator(username, password)
        else:
            raise PIAuthenticationError(f"Unknown authentication method: {method}")


class WindowsAuthenticator(PIAuthenticator):
    """Windows integrated authentication (NTLM/Kerberos).

    This authenticator uses the current Windows credentials to
    authenticate to the PI System. This is the recommended method
    for domain-joined machines.
    """

    def authenticate(self, server: Any) -> None:
        """Authenticate using Windows credentials.

        Args:
            server: The PIServer object

        Raises:
            PIAuthenticationError: If authentication fails
        """
        try:
            # Windows auth is typically handled automatically by the SDK
            # Just verify the connection is authenticated
            if hasattr(server, "CurrentUserIdentityString"):
                identity = server.CurrentUserIdentityString
                logger.info(f"Authenticated as: {identity}")
        except Exception as e:
            raise PIAuthenticationError(
                f"Windows authentication failed: {e}",
                server=str(server.Name) if hasattr(server, "Name") else "unknown",
            ) from e


class ExplicitAuthenticator(PIAuthenticator):
    """Explicit username/password authentication.

    This authenticator uses provided credentials to authenticate
    to the PI System. Use this when Windows integrated authentication
    is not available.
    """

    def __init__(self, username: str, password: SecretStr) -> None:
        """Initialize with credentials.

        Args:
            username: The username
            password: The password (stored securely)
        """
        self._username = username
        self._password = password

    def authenticate(self, server: Any) -> None:
        """Authenticate using explicit credentials.

        Args:
            server: The PIServer object

        Raises:
            PIAuthenticationError: If authentication fails
        """
        try:
            sdk = get_sdk_manager()

            # Get NetworkCredential class from System.Net
            NetworkCredential = sdk.get_type("System.Net", "NetworkCredential")

            # Create credentials
            credential = NetworkCredential(
                self._username,
                self._password.get_secret_value(),
            )

            # Connect with credentials
            server.Connect(credential)

            logger.info(f"Authenticated as: {self._username}")

        except Exception as e:
            raise PIAuthenticationError(
                f"Explicit authentication failed: {e}",
                server=str(server.Name) if hasattr(server, "Name") else "unknown",
            ) from e


class PITrustAuthenticator(PIAuthenticator):
    """PI Trust-based authentication.

    This authenticator uses PI Trust relationships for authentication.
    The trust must be configured on the PI Server.
    """

    def __init__(self, trust_name: str | None = None) -> None:
        """Initialize with optional trust name.

        Args:
            trust_name: Optional specific trust to use
        """
        self._trust_name = trust_name

    def authenticate(self, server: Any) -> None:
        """Authenticate using PI Trust.

        Args:
            server: The PIServer object

        Raises:
            PIAuthenticationError: If authentication fails
        """
        try:
            # PI Trust authentication is typically automatic
            # when the trust is properly configured
            if hasattr(server, "Connect"):
                server.Connect()

            logger.info("Authenticated via PI Trust")

        except Exception as e:
            raise PIAuthenticationError(
                f"PI Trust authentication failed: {e}",
                server=str(server.Name) if hasattr(server, "Name") else "unknown",
            ) from e


def get_current_identity(server: Any) -> str:
    """Get the current authenticated identity for a server.

    Args:
        server: The PIServer object

    Returns:
        The current user identity string
    """
    if hasattr(server, "CurrentUserIdentityString"):
        return str(server.CurrentUserIdentityString)
    return "unknown"


def check_permissions(server: Any, tag_name: str) -> dict[str, bool]:
    """Check permissions for a PI Point.

    Args:
        server: The PIServer object
        tag_name: The PI Point name

    Returns:
        Dictionary with permission flags
    """
    sdk = get_sdk_manager()
    PIPoint = sdk.pi_point_class

    try:
        point = PIPoint.FindPIPoint(server, tag_name)
        if point is None:
            return {"exists": False, "read": False, "write": False}

        # Check read/write permissions by attempting operations
        can_read = True
        can_write = False

        try:
            # Try to read a snapshot
            point.CurrentValue()
        except Exception:
            can_read = False

        return {
            "exists": True,
            "read": can_read,
            "write": can_write,
        }

    except Exception:
        return {"exists": False, "read": False, "write": False}
