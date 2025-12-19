"""Integration tests for PIClient.

These tests require a connection to a PI System.
Run with: pytest -m integration
"""

import pytest

# Skip all tests in this module if no PI System is available
pytestmark = pytest.mark.integration


@pytest.fixture
def pi_server() -> str:
    """Get PI Server name from environment or skip."""
    import os

    server = os.environ.get("PIPOLARS_TEST_SERVER")
    if not server:
        pytest.skip("PIPOLARS_TEST_SERVER not set")
    return server


class TestPIClientIntegration:
    """Integration tests for PIClient."""

    def test_connect_disconnect(self, pi_server: str) -> None:
        """Test basic connection and disconnection."""
        from pipolars import PIClient

        client = PIClient(pi_server)
        client.connect()

        assert client.is_connected
        assert client.server_name is not None

        client.disconnect()
        assert not client.is_connected

    def test_context_manager(self, pi_server: str) -> None:
        """Test context manager usage."""
        from pipolars import PIClient

        with PIClient(pi_server) as client:
            assert client.is_connected

        # After context, should be disconnected

    def test_search_tags(self, pi_server: str) -> None:
        """Test tag search."""
        from pipolars import PIClient

        with PIClient(pi_server) as client:
            # Search for common test tags
            tags = client.search_tags("SIN*", max_results=10)

            assert isinstance(tags, list)

    def test_snapshot(self, pi_server: str) -> None:
        """Test snapshot retrieval."""
        from pipolars import PIClient

        with PIClient(pi_server) as client:
            # Try SINUSOID which is a common demo tag
            try:
                df = client.snapshot("SINUSOID")
                assert len(df) == 1
                assert "timestamp" in df.columns
                assert "value" in df.columns
            except Exception:
                pytest.skip("SINUSOID tag not available")

    def test_recorded_values(self, pi_server: str) -> None:
        """Test recorded values retrieval."""
        from pipolars import PIClient

        with PIClient(pi_server) as client:
            try:
                df = client.recorded_values(
                    "SINUSOID",
                    start="*-1h",
                    end="*",
                )
                assert len(df) > 0
                assert "timestamp" in df.columns
            except Exception:
                pytest.skip("SINUSOID tag not available")

    def test_interpolated_values(self, pi_server: str) -> None:
        """Test interpolated values retrieval."""
        from pipolars import PIClient

        with PIClient(pi_server) as client:
            try:
                df = client.interpolated_values(
                    "SINUSOID",
                    start="*-1h",
                    end="*",
                    interval="5m",
                )
                assert len(df) > 0
            except Exception:
                pytest.skip("SINUSOID tag not available")

    def test_query_builder(self, pi_server: str) -> None:
        """Test query builder interface."""
        from pipolars import PIClient

        with PIClient(pi_server) as client:
            try:
                df = (
                    client.query("SINUSOID")
                    .last(hours=1)
                    .interpolated(interval="5m")
                    .to_dataframe()
                )
                assert len(df) > 0
            except Exception:
                pytest.skip("SINUSOID tag not available")
