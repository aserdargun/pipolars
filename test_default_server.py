"""Quick test script for default server behavior."""
import sys
sys.path.insert(0, r"C:\Users\serdar.gundogdu\pi_data_extractor\pipolars\src")

from pipolars.core.config import PIServerConfig, PIConfig
from pipolars.api.client import PIClient

def test_config_default():
    """Test PIServerConfig default host."""
    config = PIServerConfig()
    assert config.host == "localhost", f"Expected 'localhost', got '{config.host}'"
    print("PASS: PIServerConfig() defaults to 'localhost'")

def test_client_empty_string():
    """Test PIClient with empty string."""
    client = PIClient("")
    assert client.config.server.host == "localhost", f"Expected 'localhost', got '{client.config.server.host}'"
    print("PASS: PIClient('') defaults to 'localhost'")

def test_client_whitespace():
    """Test PIClient with whitespace."""
    client = PIClient("   ")
    assert client.config.server.host == "localhost", f"Expected 'localhost', got '{client.config.server.host}'"
    print("PASS: PIClient('   ') defaults to 'localhost'")

def test_client_none():
    """Test PIClient with None."""
    client = PIClient(None)
    assert client.config.server.host == "localhost", f"Expected 'localhost', got '{client.config.server.host}'"
    print("PASS: PIClient(None) defaults to 'localhost'")

def test_client_no_args():
    """Test PIClient with no arguments."""
    client = PIClient()
    assert client.config.server.host == "localhost", f"Expected 'localhost', got '{client.config.server.host}'"
    print("PASS: PIClient() defaults to 'localhost'")

def test_client_explicit():
    """Test PIClient with explicit server name."""
    client = PIClient("my-pi-server")
    assert client.config.server.host == "my-pi-server", f"Expected 'my-pi-server', got '{client.config.server.host}'"
    print("PASS: PIClient('my-pi-server') uses 'my-pi-server'")

if __name__ == "__main__":
    print("=" * 60)
    print("Testing default server behavior")
    print("=" * 60)

    try:
        test_config_default()
        test_client_empty_string()
        test_client_whitespace()
        test_client_none()
        test_client_no_args()
        test_client_explicit()
        print("=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)
    except AssertionError as e:
        print(f"FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        sys.exit(1)
