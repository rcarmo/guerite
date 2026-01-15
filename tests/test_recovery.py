"""Tests for recovery naming parsing/generation."""

from guerite.monitor import _parse_recovery_info_from_name, _generate_recovery_name


def test_recovery_parsing():
    """Test recovery information parsing from container names"""

    # Generated name round-trip
    name = _generate_recovery_name("myapp", "old", "abc123", 2)
    info = _parse_recovery_info_from_name(name)
    assert info is not None
    assert info["base_name"] == "myapp"
    assert info["recovery_type"] == "old"
    assert info["suffix"] == "abc123"
    assert info["fail_count"] == 2

    # Test parsing manual name
    manual_name = "webserver-guerite-new-def456-1640000000-3"
    manual_info = _parse_recovery_info_from_name(manual_name)
    assert manual_info is not None
    assert manual_info["base_name"] == "webserver"
    assert manual_info["recovery_type"] == "new"
    assert manual_info["suffix"] == "def456"
    assert manual_info["fail_count"] == 3
    assert manual_info["timestamp"] == 1640000000

    # Test non-guerite names
    non_guerite = _parse_recovery_info_from_name("normal-container")
    assert non_guerite is None
