"""AF SDK initialization and management.

This module handles loading and initializing the OSIsoft AF SDK
through pythonnet. It manages the .NET runtime and provides access
to AF SDK types.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pipolars.core.exceptions import PIAFSDKError, PIConfigurationError

if TYPE_CHECKING:
    from types import ModuleType


class PISDKManager:
    """Manages the AF SDK initialization and provides access to SDK types.

    This class is responsible for:
    - Loading the .NET runtime via pythonnet
    - Adding references to AF SDK assemblies
    - Providing access to AF SDK namespaces and types

    The SDK is initialized lazily on first access.

    Example:
        >>> sdk = PISDKManager()
        >>> sdk.initialize()
        >>> PIServer = sdk.get_type("OSIsoft.AF.PI", "PIServer")
        >>> servers = PIServer.FindPIServers()
    """

    # Default AF SDK installation paths
    DEFAULT_AF_SDK_PATHS = [
        r"C:\Program Files\PIPC\AF\PublicAssemblies\4.0",
        r"C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0",
        r"C:\Program Files\PIPC\pisdk",
    ]

    _instance: PISDKManager | None = None
    _initialized: bool = False

    def __new__(cls) -> PISDKManager:
        """Singleton pattern to ensure single SDK initialization."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize the SDK manager."""
        if not hasattr(self, "_clr"):
            self._clr: ModuleType | None = None
            self._af_sdk_path: Path | None = None
            self._assemblies_loaded: set[str] = set()

    @property
    def is_initialized(self) -> bool:
        """Check if the SDK has been initialized."""
        return self._initialized

    def initialize(self, af_sdk_path: str | Path | None = None) -> None:
        """Initialize the .NET runtime and load AF SDK assemblies.

        Args:
            af_sdk_path: Optional custom path to AF SDK assemblies.
                        If not provided, searches default locations.

        Raises:
            PIConfigurationError: If AF SDK cannot be found
            PIAFSDKError: If SDK initialization fails
        """
        if self._initialized:
            return

        # Find AF SDK path
        self._af_sdk_path = self._find_af_sdk_path(af_sdk_path)

        # Initialize pythonnet
        self._initialize_clr()

        # Load core assemblies
        self._load_assemblies()

        self._initialized = True

    def _find_af_sdk_path(self, custom_path: str | Path | None = None) -> Path:
        """Find the AF SDK installation path.

        Args:
            custom_path: Optional custom path to check first

        Returns:
            Path to the AF SDK assemblies

        Raises:
            PIConfigurationError: If AF SDK cannot be found
        """
        paths_to_check = []

        if custom_path:
            paths_to_check.append(Path(custom_path))

        # Check environment variable
        env_path = os.environ.get("PIPOLARS_AF_SDK_PATH")
        if env_path:
            paths_to_check.append(Path(env_path))

        # Check default paths
        paths_to_check.extend(Path(p) for p in self.DEFAULT_AF_SDK_PATHS)

        for path in paths_to_check:
            if path.exists() and (path / "OSIsoft.AFSDK.dll").exists():
                return path

        raise PIConfigurationError(
            "AF SDK not found. Please install OSIsoft PI AF SDK or set "
            "PIPOLARS_AF_SDK_PATH environment variable.",
            details={"searched_paths": [str(p) for p in paths_to_check]},
        )

    def _initialize_clr(self) -> None:
        """Initialize the pythonnet CLR.

        Raises:
            PIAFSDKError: If CLR initialization fails
        """
        try:
            # For pythonnet 3.x, we need to set the runtime before importing clr
            from pythonnet import load

            # Use .NET Framework (netfx) for AF SDK compatibility
            load("netfx")

            import clr

            self._clr = clr

            # Add the AF SDK path to the assembly search path
            if self._af_sdk_path:
                sys.path.append(str(self._af_sdk_path))

        except ImportError as e:
            raise PIAFSDKError(
                "Failed to import pythonnet. Ensure pythonnet is installed.",
                sdk_message=str(e),
            ) from e
        except Exception as e:
            raise PIAFSDKError(
                "Failed to initialize .NET runtime",
                sdk_message=str(e),
            ) from e

    def _load_assemblies(self) -> None:
        """Load required AF SDK assemblies.

        Raises:
            PIAFSDKError: If assembly loading fails
        """
        required_assemblies = [
            "OSIsoft.AFSDK",
        ]

        for assembly in required_assemblies:
            self._load_assembly(assembly)

    def _load_assembly(self, assembly_name: str) -> None:
        """Load a specific assembly.

        Args:
            assembly_name: Name of the assembly to load

        Raises:
            PIAFSDKError: If assembly loading fails
        """
        if assembly_name in self._assemblies_loaded:
            return

        if self._clr is None:
            raise PIAFSDKError("CLR not initialized")

        try:
            # Try loading from the AF SDK path first
            if self._af_sdk_path:
                dll_path = self._af_sdk_path / f"{assembly_name}.dll"
                if dll_path.exists():
                    self._clr.AddReference(str(dll_path))
                    self._assemblies_loaded.add(assembly_name)
                    return

            # Try loading by name
            self._clr.AddReference(assembly_name)
            self._assemblies_loaded.add(assembly_name)

        except Exception as e:
            raise PIAFSDKError(
                f"Failed to load assembly: {assembly_name}",
                sdk_message=str(e),
            ) from e

    def get_type(self, namespace: str, type_name: str) -> Any:
        """Get a type from the AF SDK.

        Args:
            namespace: The .NET namespace (e.g., "OSIsoft.AF.PI")
            type_name: The type name (e.g., "PIServer")

        Returns:
            The .NET type

        Raises:
            PIAFSDKError: If the type cannot be found
        """
        if not self._initialized:
            self.initialize()

        try:
            # Import the namespace and get the type
            module = __import__(namespace, fromlist=[type_name])
            return getattr(module, type_name)
        except (ImportError, AttributeError) as e:
            raise PIAFSDKError(
                f"Failed to get type {namespace}.{type_name}",
                sdk_message=str(e),
            ) from e

    def import_namespace(self, namespace: str) -> ModuleType:
        """Import an AF SDK namespace.

        Args:
            namespace: The .NET namespace to import

        Returns:
            The imported module

        Raises:
            PIAFSDKError: If the namespace cannot be imported
        """
        if not self._initialized:
            self.initialize()

        try:
            return __import__(namespace)
        except ImportError as e:
            raise PIAFSDKError(
                f"Failed to import namespace: {namespace}",
                sdk_message=str(e),
            ) from e

    @property
    def af_time_class(self) -> Any:
        """Get the AFTime class from the SDK."""
        return self.get_type("OSIsoft.AF.Time", "AFTime")

    @property
    def af_time_range_class(self) -> Any:
        """Get the AFTimeRange class from the SDK."""
        return self.get_type("OSIsoft.AF.Time", "AFTimeRange")

    @property
    def pi_server_class(self) -> Any:
        """Get the PIServer class from the SDK."""
        return self.get_type("OSIsoft.AF.PI", "PIServer")

    @property
    def pi_servers_class(self) -> Any:
        """Get the PIServers class from the SDK."""
        return self.get_type("OSIsoft.AF.PI", "PIServers")

    @property
    def pi_point_class(self) -> Any:
        """Get the PIPoint class from the SDK."""
        return self.get_type("OSIsoft.AF.PI", "PIPoint")

    @property
    def pi_point_list_class(self) -> Any:
        """Get the PIPointList class from the SDK."""
        return self.get_type("OSIsoft.AF.PI", "PIPointList")

    @property
    def af_database_class(self) -> Any:
        """Get the AFDatabase class from the SDK."""
        return self.get_type("OSIsoft.AF", "AFDatabase")

    @property
    def af_element_class(self) -> Any:
        """Get the AFElement class from the SDK."""
        return self.get_type("OSIsoft.AF.Asset", "AFElement")


# Global SDK manager instance
_sdk_manager: PISDKManager | None = None


def get_sdk_manager() -> PISDKManager:
    """Get the global SDK manager instance.

    Returns:
        The global PISDKManager instance
    """
    global _sdk_manager
    if _sdk_manager is None:
        _sdk_manager = PISDKManager()
    return _sdk_manager
