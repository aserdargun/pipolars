"""Digital state mapping for PI data.

This module provides utilities for handling PI digital states,
which are enumerated values with specific meanings in PI System.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Any

import polars as pl

from pipolars.connection.sdk import get_sdk_manager
from pipolars.core.types import DigitalState


class SystemDigitalState(IntEnum):
    """System digital state codes used by PI.

    These are the standard system digital states that indicate
    data quality or system conditions.
    """

    NO_DATA = 248
    BAD_INPUT = 249
    CALC_OFF = 250
    COMM_FAIL = 251
    CONFIGURE = 252
    I_O_TIMEOUT = 253
    NO_SAMPLE = 254
    SHUTDOWN = 255
    SCAN_OFF = 256
    OVER_RANGE = 257
    UNDER_RANGE = 258


class DigitalStateMapper:
    """Maps PI digital states to readable values and vice versa.

    Digital states in PI are enumerated values that represent
    specific conditions or statuses. This class provides utilities
    for converting between numeric codes and string representations.

    Example:
        >>> mapper = DigitalStateMapper(connection)
        >>> mapper.load_state_set("Modes")
        >>> name = mapper.code_to_name("Modes", 1)  # "Auto"
        >>> code = mapper.name_to_code("Modes", "Manual")  # 2
    """

    def __init__(self) -> None:
        """Initialize the digital state mapper."""
        self._sdk = get_sdk_manager()
        self._state_sets: dict[str, dict[int, str]] = {}
        self._reverse_sets: dict[str, dict[str, int]] = {}

        # Pre-populate system states
        self._system_states: dict[int, str] = {
            SystemDigitalState.NO_DATA: "No Data",
            SystemDigitalState.BAD_INPUT: "Bad Input",
            SystemDigitalState.CALC_OFF: "Calc Off",
            SystemDigitalState.COMM_FAIL: "Comm Fail",
            SystemDigitalState.CONFIGURE: "Configure",
            SystemDigitalState.I_O_TIMEOUT: "I/O Timeout",
            SystemDigitalState.NO_SAMPLE: "No Sample",
            SystemDigitalState.SHUTDOWN: "Shutdown",
            SystemDigitalState.SCAN_OFF: "Scan Off",
            SystemDigitalState.OVER_RANGE: "Over Range",
            SystemDigitalState.UNDER_RANGE: "Under Range",
        }

    def load_state_set(self, state_set_name: str, server: Any = None) -> dict[int, str]:
        """Load a digital state set from the PI server.

        Args:
            state_set_name: Name of the digital state set
            server: Optional PI Server connection

        Returns:
            Dictionary mapping codes to names
        """
        if state_set_name in self._state_sets:
            return self._state_sets[state_set_name]

        try:
            # Get state set from PI
            AFDatabase = self._sdk.af_database_class

            # This would normally query the PI server for the state set
            # For now, return a placeholder that would be populated
            # when actually connected to PI

            state_map: dict[int, str] = {}
            reverse_map: dict[str, int] = {}

            # Store for future use
            self._state_sets[state_set_name] = state_map
            self._reverse_sets[state_set_name] = reverse_map

            return state_map

        except Exception:
            return {}

    def code_to_name(
        self,
        state_set: str,
        code: int,
        default: str = "Unknown",
    ) -> str:
        """Convert a digital state code to its name.

        Args:
            state_set: Name of the digital state set
            code: Numeric code
            default: Default value if not found

        Returns:
            String name of the digital state
        """
        # Check system states first
        if code in self._system_states:
            return self._system_states[code]

        # Check loaded state sets
        if state_set in self._state_sets:
            return self._state_sets[state_set].get(code, default)

        return default

    def name_to_code(
        self,
        state_set: str,
        name: str,
        default: int = -1,
    ) -> int:
        """Convert a digital state name to its code.

        Args:
            state_set: Name of the digital state set
            name: String name
            default: Default value if not found

        Returns:
            Numeric code of the digital state
        """
        # Check system states first
        for code, state_name in self._system_states.items():
            if state_name.lower() == name.lower():
                return code

        # Check loaded state sets
        if state_set in self._reverse_sets:
            return self._reverse_sets[state_set].get(name, default)

        return default

    def is_system_state(self, code: int) -> bool:
        """Check if a code is a system digital state.

        Args:
            code: Numeric code to check

        Returns:
            True if it's a system state
        """
        return code in self._system_states

    def is_bad_state(self, code: int) -> bool:
        """Check if a digital state code indicates bad data.

        Args:
            code: Numeric code to check

        Returns:
            True if the state indicates bad data
        """
        bad_states = {
            SystemDigitalState.NO_DATA,
            SystemDigitalState.BAD_INPUT,
            SystemDigitalState.COMM_FAIL,
            SystemDigitalState.I_O_TIMEOUT,
            SystemDigitalState.NO_SAMPLE,
        }
        return code in bad_states

    def decode_column(
        self,
        df: pl.DataFrame,
        column: str,
        state_set: str,
        output_column: str | None = None,
    ) -> pl.DataFrame:
        """Decode a column of digital state codes to names.

        Args:
            df: Input DataFrame
            column: Column containing digital state codes
            state_set: Name of the digital state set
            output_column: Name for the output column (default: column + "_name")

        Returns:
            DataFrame with decoded column added
        """
        output_column = output_column or f"{column}_name"

        # Build mapping expression
        states = self._state_sets.get(state_set, {})
        states.update({int(k): v for k, v in self._system_states.items()})

        # Use when/then for mapping
        expr = pl.col(column)

        for code, name in states.items():
            expr = pl.when(pl.col(column) == code).then(pl.lit(name)).otherwise(expr)

        return df.with_columns(expr.alias(output_column))

    def encode_column(
        self,
        df: pl.DataFrame,
        column: str,
        state_set: str,
        output_column: str | None = None,
    ) -> pl.DataFrame:
        """Encode a column of digital state names to codes.

        Args:
            df: Input DataFrame
            column: Column containing digital state names
            state_set: Name of the digital state set
            output_column: Name for the output column (default: column + "_code")

        Returns:
            DataFrame with encoded column added
        """
        output_column = output_column or f"{column}_code"

        # Build reverse mapping
        reverse_states = self._reverse_sets.get(state_set, {})
        reverse_states.update({v: int(k) for k, v in self._system_states.items()})

        # Use when/then for mapping
        expr = pl.lit(-1)  # Default value

        for name, code in reverse_states.items():
            expr = pl.when(pl.col(column) == name).then(pl.lit(code)).otherwise(expr)

        return df.with_columns(expr.alias(output_column))

    def filter_good_values(
        self,
        df: pl.DataFrame,
        value_column: str = "value",
    ) -> pl.DataFrame:
        """Filter out rows with bad digital states.

        Args:
            df: Input DataFrame
            value_column: Column containing values to check

        Returns:
            DataFrame with bad values removed
        """
        bad_codes = [int(s) for s in SystemDigitalState]

        # Filter out rows where value matches bad state codes
        return df.filter(~pl.col(value_column).is_in(bad_codes))

    def replace_bad_with_null(
        self,
        df: pl.DataFrame,
        value_column: str = "value",
    ) -> pl.DataFrame:
        """Replace bad digital state values with null.

        Args:
            df: Input DataFrame
            value_column: Column containing values

        Returns:
            DataFrame with bad values replaced by null
        """
        bad_codes = [int(s) for s in SystemDigitalState]

        return df.with_columns(
            pl.when(pl.col(value_column).is_in(bad_codes))
            .then(None)
            .otherwise(pl.col(value_column))
            .alias(value_column)
        )
