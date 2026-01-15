"""Tests for timeout handling in critical scenarios."""

from datetime import timedelta
from unittest.mock import MagicMock
import pytest

from docker.errors import DockerException
from requests.exceptions import ReadTimeout, RequestException
from urllib3.exceptions import ReadTimeoutError

from guerite.config import load_settings
from guerite.monitor import (
    pull_image,
    _register_restart_failure,
    _restart_allowed,
    _RESTART_FAIL_COUNT,
    _RESTART_BACKOFF,
)
from guerite.utils import now_utc


class TestPullImageTimeoutHandling:
    """Test timeout exception handling in pull_image function."""

    def test_pull_image_read_timeout(self):
        """Test pull_image handles ReadTimeout gracefully."""
        client = MagicMock()
        client.images.pull.side_effect = ReadTimeout("Connection timed out")

        result = pull_image(client, "test:latest")

        assert result is None
        client.images.pull.assert_called_once_with("test:latest")

    def test_pull_image_urllib3_timeout(self):
        """Test pull_image handles urllib3 ReadTimeoutError."""
        client = MagicMock()
        client.images.pull.side_effect = ReadTimeoutError(
            pool=None, url="https://example.com", message="Connection timed out"
        )

        result = pull_image(client, "test:latest")

        assert result is None
        client.images.pull.assert_called_once_with("test:latest")

    def test_pull_image_request_exception(self):
        """Test pull_image handles RequestException."""
        client = MagicMock()
        client.images.pull.side_effect = RequestException("Connection error")

        result = pull_image(client, "test:latest")

        assert result is None
        client.images.pull.assert_called_once_with("test:latest")

    def test_pull_image_docker_exception(self):
        """Test pull_image handles DockerException."""
        client = MagicMock()
        client.images.pull.side_effect = DockerException("Docker error")

        result = pull_image(client, "test:latest")

        assert result is None
        client.images.pull.assert_called_once_with("test:latest")


class TestBackoffMechanism:
    """Test backoff calculation and retry logic."""

    def test_restart_backoff_calculation_basic(self):
        """Test basic backoff calculation increases with failure count."""
        settings = load_settings()
        event_log = []
        notify = True

        container_id = "test123"
        original_name = "test-container"

        # First failure
        _register_restart_failure(
            container_id, original_name, notify, event_log, settings, Exception("test")
        )
        assert _RESTART_FAIL_COUNT[container_id] == 1
        first_backoff = _RESTART_BACKOFF[container_id]

        # Second failure should have longer backoff
        _register_restart_failure(
            container_id, original_name, notify, event_log, settings, Exception("test2")
        )
        assert _RESTART_FAIL_COUNT[container_id] == 2
        second_backoff = _RESTART_BACKOFF[container_id]

        # Backoff should increase
        assert second_backoff > first_backoff

    def test_restart_backoff_calculation_capped_at_3600(self):
        """Test backoff is capped at 3600 seconds per attempt."""
        settings = load_settings()
        event_log = []
        notify = True

        container_id = "test123"
        original_name = "test-container"

        # Simulate many failures to test capping
        for i in range(15):  # This should exceed the cap
            _register_restart_failure(
                container_id,
                original_name,
                notify,
                event_log,
                settings,
                Exception(f"test{i}"),
            )

        # Backoff should not exceed the per-attempt cap of 3600 seconds
        backoff_duration = (_RESTART_BACKOFF[container_id] - now_utc()).total_seconds()
        assert backoff_duration <= 3600

    def test_restart_backoff_retry_limit_enforcement(self):
        """Test retry limit enforcement increases backoff significantly."""
        settings = load_settings()  # Default retry_limit = 3
        event_log = []
        notify = True

        container_id = "test123"
        original_name = "test-container"

        # Reach retry limit
        for i in range(settings.restart_retry_limit):
            _register_restart_failure(
                container_id,
                original_name,
                notify,
                event_log,
                settings,
                Exception(f"test{i}"),
            )

        # After reaching limit, backoff should be significantly longer
        backoff_duration = (_RESTART_BACKOFF[container_id] - now_utc()).total_seconds()
        expected_minimum = (
            settings.health_backoff_seconds * settings.restart_retry_limit
        )
        # Allow small timing differences (1 second tolerance)
        assert backoff_duration >= expected_minimum - 1

    def test_restart_allowed_during_backoff(self):
        """Test restart is not allowed during backoff period."""
        settings = load_settings()
        now = now_utc()

        container_id = "test123"

        # Set a backoff that's still active
        future_time = now + timedelta(seconds=300)
        _RESTART_BACKOFF[container_id] = future_time

        # Should not be allowed
        assert not _restart_allowed(container_id, "test", now, settings)

    def test_restart_allowed_after_backoff(self):
        """Test restart is allowed after backoff expires."""
        settings = load_settings()
        now = now_utc()

        container_id = "test123"

        # Set a backoff that's already expired
        past_time = now - timedelta(seconds=300)
        _RESTART_BACKOFF[container_id] = past_time

        # Should be allowed
        assert _restart_allowed(container_id, "test", now, settings)

    def test_restart_allowed_no_backoff(self):
        """Test restart is allowed when no backoff exists."""
        settings = load_settings()
        now = now_utc()

        container_id = "test123"

        # Ensure no backoff exists
        _RESTART_BACKOFF.pop(container_id, None)

        # Should be allowed
        assert _restart_allowed(container_id, "test", now, settings)


class TestTimeoutConfigurationDefaults:
    """Test that default timeout configurations are reasonable for backoff scenarios."""

    def test_default_backoff_settings_are_reasonable(self):
        """Test that default timeout settings make sense for timeout scenarios."""
        settings = load_settings()

        # Check that default values are reasonable
        assert settings.health_backoff_seconds == 300  # 5 minutes base backoff
        assert (
            settings.health_check_timeout_seconds == 60
        )  # 1 minute health check timeout
        assert settings.restart_retry_limit == 3  # 3 retries before extended backoff

        # These values should provide good balance between reliability and responsiveness

    def test_backoff_growth_with_defaults(self):
        """Test backoff growth with default settings."""
        settings = load_settings()
        event_log = []
        notify = True

        container_id = "test123"
        original_name = "test-container"

        # Test backoff progression with defaults
        backoffs = []
        for i in range(5):
            _register_restart_failure(
                container_id,
                original_name,
                notify,
                event_log,
                settings,
                Exception(f"test{i}"),
            )
            backoff_duration = (
                _RESTART_BACKOFF[container_id] - now_utc()
            ).total_seconds()
            backoffs.append(backoff_duration)

        # Backoff should increase: 300s, 600s, 900s, 1200s, 1500s (before retry limit)
        expected = [300, 600, 900]
        for i, expected_duration in enumerate(expected):
            assert abs(backoffs[i] - expected_duration) < 1  # Allow 1 second tolerance

    def test_retry_limit_enforcement_with_defaults(self):
        """Test retry limit enforcement with default settings."""
        settings = load_settings()
        event_log = []
        notify = True

        container_id = "test123"
        original_name = "test-container"

        # Exceed retry limit (3 failures)
        for i in range(4):  # One beyond the limit
            _register_restart_failure(
                container_id,
                original_name,
                notify,
                event_log,
                settings,
                Exception(f"test{i}"),
            )

        # After exceeding limit, backoff should be at least 3 * 300 = 900 seconds
        backoff_duration = (_RESTART_BACKOFF[container_id] - now_utc()).total_seconds()
        assert backoff_duration >= 900


# Cleanup after tests
@pytest.fixture(autouse=True)
def cleanup_backoff_state():
    """Clean up backoff state between tests."""
    yield
    _RESTART_FAIL_COUNT.clear()
    _RESTART_BACKOFF.clear()
