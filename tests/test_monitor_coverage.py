#!/usr/bin/env python3
"""Additional tests for monitor.py coverage."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

from guerite.monitor import (
    _get_upgrade_state,
    _should_notify,
    _is_unhealthy,
    _started_recently,
    current_image_id,
    _short_id,
    _strip_guerite_suffix,
    _base_name,
    _health_allowed,
    _restart_allowed,
    needs_update,
    _action_allowed,
    _prune_due,
    next_prune_time,
    _normalize_links_value,
    _image_display_name,
    _link_targets,
    _label_dependencies,
    UpgradeState,
)


class TestGetUpgradeState:
    """Test _get_upgrade_state function."""

    def test_returns_none_when_no_labels(self):
        """Return None when container has no labels."""
        container = Mock()
        container.labels = None
        assert _get_upgrade_state(container) is None

    def test_returns_none_when_no_upgrade_status(self):
        """Return None when container has no upgrade status label."""
        container = Mock()
        container.labels = {"some.other.label": "value"}
        assert _get_upgrade_state(container) is None

    def test_extracts_basic_status(self):
        """Extract basic upgrade status from labels."""
        container = Mock()
        container.labels = {"guerite.upgrade.status": "failed"}
        state = _get_upgrade_state(container)
        assert state is not None
        assert state.status == "failed"

    def test_extracts_all_fields(self):
        """Extract all upgrade state fields from labels."""
        container = Mock()
        container.labels = {
            "guerite.upgrade.status": "in-progress",
            "guerite.upgrade.original-image": "sha256:abc123",
            "guerite.upgrade.target-image": "sha256:def456",
            "guerite.upgrade.started": "2025-01-01T12:00:00+00:00",
        }
        state = _get_upgrade_state(container)
        assert state is not None
        assert state.status == "in-progress"
        assert state.original_image_id == "sha256:abc123"
        assert state.target_image_id == "sha256:def456"
        assert state.started_at is not None

    def test_handles_invalid_timestamp(self):
        """Handle invalid timestamp gracefully."""
        container = Mock()
        container.labels = {
            "guerite.upgrade.status": "failed",
            "guerite.upgrade.started": "invalid-timestamp",
        }
        state = _get_upgrade_state(container)
        assert state is not None
        assert state.status == "failed"
        assert state.started_at is None


class TestShouldNotify:
    """Test _should_notify function."""

    def test_returns_false_when_notifications_none(self):
        """Return False when notifications is None."""
        settings = Mock()
        settings.notifications = None
        assert _should_notify(settings, "restart") is False

    def test_returns_true_when_event_in_notifications(self):
        """Return True when event is in notifications."""
        settings = Mock()
        settings.notifications = ["restart", "update"]
        assert _should_notify(settings, "restart") is True

    def test_returns_false_when_event_not_in_notifications(self):
        """Return False when event not in notifications."""
        settings = Mock()
        settings.notifications = ["update"]
        assert _should_notify(settings, "restart") is False

    def test_handles_missing_notifications_attr(self):
        """Handle missing notifications attribute."""
        settings = Mock(spec=[])
        assert _should_notify(settings, "restart") is False


class TestIsUnhealthy:
    """Test _is_unhealthy function."""

    def test_returns_false_when_no_health_info(self):
        """Return False when container has no health info."""
        container = Mock()
        container.attrs = {"State": {}}
        assert _is_unhealthy(container) is False

    def test_returns_false_when_healthy(self):
        """Return False when container is healthy."""
        container = Mock()
        container.attrs = {"State": {"Health": {"Status": "healthy"}}}
        assert _is_unhealthy(container) is False

    def test_returns_false_when_starting(self):
        """Return False when container health is starting."""
        container = Mock()
        container.attrs = {"State": {"Health": {"Status": "starting"}}}
        assert _is_unhealthy(container) is False

    def test_returns_true_when_unhealthy(self):
        """Return True when container is unhealthy."""
        container = Mock()
        container.name = "test-container"
        container.attrs = {"State": {"Health": {"Status": "unhealthy"}}}
        assert _is_unhealthy(container) is True


class TestStartedRecently:
    """Test _started_recently function."""

    def test_returns_false_when_no_started_at(self):
        """Return False when container has no StartedAt."""
        container = Mock()
        container.attrs = {"State": {}}
        now = datetime.now(timezone.utc)
        assert _started_recently(container, now, 60) is False

    def test_returns_true_when_started_recently(self):
        """Return True when container started within grace period."""
        container = Mock()
        now = datetime.now(timezone.utc)
        started = now.isoformat()
        container.attrs = {"State": {"StartedAt": started}}
        assert _started_recently(container, now, 60) is True

    def test_returns_false_when_started_long_ago(self):
        """Return False when container started before grace period."""
        container = Mock()
        now = datetime.now(timezone.utc)
        old_start = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
        container.attrs = {"State": {"StartedAt": old_start}}
        assert _started_recently(container, now, 60) is False


class TestCurrentImageId:
    """Test current_image_id function."""

    def test_returns_image_id(self):
        """Return image ID from container."""
        container = Mock()
        container.image.id = "sha256:abc123"
        assert current_image_id(container) == "sha256:abc123"

    def test_returns_none_on_exception(self):
        """Return None when Docker exception occurs."""
        from docker.errors import DockerException

        container = Mock()
        container.name = "test"
        container.image.id = property(lambda self: (_ for _ in ()).throw(DockerException("error")))
        type(container).image = property(lambda self: Mock(id=None))
        # Simulate exception
        container = Mock()
        container.name = "test"
        type(container.image).id = property(lambda s: (_ for _ in ()).throw(DockerException("error")))
        result = current_image_id(container)
        assert result is None


class TestHelperFunctions:
    """Test various helper functions."""

    def test_short_id_truncates(self):
        """_short_id truncates long IDs."""
        # _short_id splits by ':' and takes last 12 chars
        assert _short_id("sha256:abcdef123456789") == "abcdef123456"
        assert _short_id("abcd") == "abcd"
        assert _short_id(None) == "unknown"

    def test_strip_guerite_suffix_removes_suffix(self):
        """_strip_guerite_suffix removes -guerite-old-{hash} suffix."""
        # Pattern is: name-guerite-(old|new)-{8 hex chars}
        assert _strip_guerite_suffix("myapp-guerite-old-12345678") == "myapp"
        assert _strip_guerite_suffix("myapp-guerite-new-abcdef12") == "myapp"
        assert _strip_guerite_suffix("myapp") == "myapp"
        # Nested suffixes
        assert _strip_guerite_suffix("app-guerite-old-12345678-guerite-new-abcdef12") == "app"

    def test_base_name_strips_slash(self):
        """_base_name calls _strip_guerite_suffix on container name."""
        container = Mock()
        container.name = "myapp-guerite-old-12345678"
        assert _base_name(container) == "myapp"

        container.name = "myapp"
        assert _base_name(container) == "myapp"

        container.name = None
        assert _base_name(container) == "unknown"


class TestUpgradeStateDataclass:
    """Test UpgradeState dataclass."""

    def test_default_values(self):
        """Test default values for UpgradeState."""
        state = UpgradeState()
        assert state.original_image_id is None
        assert state.target_image_id is None
        assert state.started_at is None
        assert state.status == "unknown"
        assert state.base_name is None

    def test_with_all_fields(self):
        """Test UpgradeState with all fields set."""
        now = datetime.now(timezone.utc)
        state = UpgradeState(
            original_image_id="old",
            target_image_id="new",
            started_at=now,
            status="in-progress",
            base_name="myapp",
        )
        assert state.original_image_id == "old"
        assert state.target_image_id == "new"
        assert state.started_at == now
        assert state.status == "in-progress"
        assert state.base_name == "myapp"


class TestNeedsUpdate:
    """Test needs_update function."""

    def test_returns_true_when_ids_differ(self):
        """Return True when container image ID differs from pulled image."""
        container = Mock()
        container.image.id = "sha256:old"
        pulled_image = Mock()
        pulled_image.id = "sha256:new"
        assert needs_update(container, pulled_image) is True

    def test_returns_false_when_ids_match(self):
        """Return False when container image ID matches pulled image."""
        container = Mock()
        container.image.id = "sha256:same"
        pulled_image = Mock()
        pulled_image.id = "sha256:same"
        assert needs_update(container, pulled_image) is False

    def test_returns_false_on_exception(self):
        """Return False when Docker exception occurs."""
        from docker.errors import DockerException

        container = Mock()
        container.name = "test"
        type(container.image).id = property(
            lambda s: (_ for _ in ()).throw(DockerException("error"))
        )
        pulled_image = Mock()
        pulled_image.id = "sha256:new"
        assert needs_update(container, pulled_image) is False


class TestHealthAllowed:
    """Test _health_allowed function."""

    def setup_method(self):
        """Clear health backoff state."""
        from guerite import monitor
        monitor._HEALTH_BACKOFF.clear()

    def test_returns_true_when_no_backoff(self):
        """Return True when no backoff is set."""
        settings = Mock()
        now = datetime.now(timezone.utc)
        assert _health_allowed("container123", "app", now, settings) is True

    def test_returns_true_when_backoff_expired(self):
        """Return True when backoff has expired."""
        from guerite import monitor

        settings = Mock()
        now = datetime.now(timezone.utc)
        # Set backoff in the past
        monitor._HEALTH_BACKOFF["container123"] = now - timedelta(seconds=10)
        assert _health_allowed("container123", "app", now, settings) is True

    def test_returns_false_during_backoff(self):
        """Return False when still in backoff period."""
        from guerite import monitor

        settings = Mock()
        now = datetime.now(timezone.utc)
        # Set backoff in the future
        monitor._HEALTH_BACKOFF["container123"] = now + timedelta(seconds=60)
        assert _health_allowed("container123", "app", now, settings) is False


class TestRestartAllowed:
    """Test _restart_allowed function."""

    def setup_method(self):
        """Clear restart backoff state."""
        from guerite import monitor
        monitor._RESTART_BACKOFF.clear()

    def test_returns_true_when_no_backoff(self):
        """Return True when no backoff is set."""
        settings = Mock()
        now = datetime.now(timezone.utc)
        assert _restart_allowed("container123", "app", now, settings) is True

    def test_returns_true_when_backoff_expired(self):
        """Return True when backoff has expired."""
        from guerite import monitor

        settings = Mock()
        now = datetime.now(timezone.utc)
        # Set backoff in the past
        monitor._RESTART_BACKOFF["container123"] = now - timedelta(seconds=10)
        assert _restart_allowed("container123", "app", now, settings) is True

    def test_returns_false_during_backoff(self):
        """Return False when still in backoff period."""
        from guerite import monitor

        settings = Mock()
        now = datetime.now(timezone.utc)
        # Set backoff in the future
        monitor._RESTART_BACKOFF["container123"] = now + timedelta(seconds=60)
        assert _restart_allowed("container123", "app", now, settings) is False


class TestActionAllowed:
    """Test _action_allowed function."""

    def setup_method(self):
        """Clear action state."""
        from guerite import monitor
        monitor._LAST_ACTION.clear()
        monitor._IN_FLIGHT.clear()

    def test_returns_true_when_no_prior_action(self):
        """Return True when no prior action recorded."""
        settings = Mock()
        settings.action_cooldown_seconds = 60
        now = datetime.now(timezone.utc)
        assert _action_allowed("app", now, settings) is True

    def test_returns_false_during_cooldown(self):
        """Return False when action is within cooldown."""
        from guerite import monitor

        settings = Mock()
        settings.action_cooldown_seconds = 60
        now = datetime.now(timezone.utc)
        # Set last action recently
        monitor._LAST_ACTION["app"] = now - timedelta(seconds=10)
        assert _action_allowed("app", now, settings) is False

    def test_returns_true_after_cooldown(self):
        """Return True when cooldown has expired."""
        from guerite import monitor

        settings = Mock()
        settings.action_cooldown_seconds = 60
        now = datetime.now(timezone.utc)
        # Set last action long ago
        monitor._LAST_ACTION["app"] = now - timedelta(seconds=120)
        assert _action_allowed("app", now, settings) is True

    def test_returns_false_when_inflight(self):
        """Return False when action is in-flight."""
        from guerite import monitor

        settings = Mock()
        settings.action_cooldown_seconds = 60
        now = datetime.now(timezone.utc)
        # Mark as in-flight
        monitor._IN_FLIGHT.add("app")
        assert _action_allowed("app", now, settings) is False


class TestPruneDue:
    """Test _prune_due function."""

    def setup_method(self):
        """Reset cron invalid flag."""
        from guerite import monitor
        monitor._PRUNE_CRON_INVALID = False

    def test_returns_false_when_no_cron(self):
        """Return False when no prune cron is set."""
        settings = Mock()
        settings.prune_cron = None
        now = datetime.now(timezone.utc)
        assert _prune_due(settings, now) is False

    def test_returns_true_when_cron_matches(self):
        """Return True when cron expression matches current time."""
        settings = Mock()
        settings.prune_cron = "* * * * *"  # Every minute
        now = datetime.now(timezone.utc)
        assert _prune_due(settings, now) is True

    def test_returns_false_when_cron_invalid(self):
        """Return False when cron expression is invalid."""
        from guerite import monitor

        settings = Mock()
        settings.prune_cron = "invalid cron"
        now = datetime.now(timezone.utc)
        assert _prune_due(settings, now) is False
        assert monitor._PRUNE_CRON_INVALID is True


class TestNextPruneTime:
    """Test next_prune_time function."""

    def setup_method(self):
        """Reset cron invalid flag."""
        from guerite import monitor
        monitor._PRUNE_CRON_INVALID = False

    def test_returns_none_when_no_cron(self):
        """Return None when no prune cron is set."""
        settings = Mock()
        settings.prune_cron = None
        now = datetime.now(timezone.utc)
        assert next_prune_time(settings, now) is None

    def test_returns_next_time_when_cron_valid(self):
        """Return next time when cron expression is valid."""
        settings = Mock()
        settings.prune_cron = "0 * * * *"  # Every hour
        now = datetime(2025, 6, 15, 14, 30, tzinfo=timezone.utc)
        result = next_prune_time(settings, now)
        assert result is not None
        assert result > now


class TestNormalizeLinksValue:
    """Test _normalize_links_value function."""

    def test_returns_none_for_none(self):
        """Return None for None input."""
        assert _normalize_links_value(None) is None

    def test_returns_none_for_false(self):
        """Return None for False input."""
        assert _normalize_links_value(False) is None

    def test_returns_dict_as_is(self):
        """Return dict as-is."""
        links = {"container1": "alias1"}
        assert _normalize_links_value(links) == links

    def test_converts_list_with_colons(self):
        """Convert list of 'container:alias' strings to dict."""
        links = ["container1:alias1", "container2:alias2"]
        result = _normalize_links_value(links)
        assert result == {"container1": "alias1", "container2": "alias2"}

    def test_converts_list_without_colons(self):
        """Convert list of container names to dict with name as alias."""
        links = ["container1", "container2"]
        result = _normalize_links_value(links)
        assert result == {"container1": "container1", "container2": "container2"}

    def test_returns_none_for_empty_list(self):
        """Return None for empty list."""
        assert _normalize_links_value([]) is None


class TestImageDisplayName:
    """Test _image_display_name function."""

    def test_returns_image_ref_when_provided(self):
        """Return image_ref when provided."""
        result = _image_display_name(image_ref="nginx:latest")
        assert result == "nginx:latest"

    def test_returns_unknown_when_nothing_provided(self):
        """Return 'unknown' when no info provided."""
        result = _image_display_name()
        assert result == "unknown"


class TestLinkTargets:
    """Test _link_targets function."""

    def test_returns_empty_set_when_no_links(self):
        """Return empty set when no links defined."""
        container = Mock()
        container.attrs = {"HostConfig": {}}
        assert _link_targets(container) == set()

    def test_extracts_link_targets(self):
        """Extract container names from links."""
        container = Mock()
        container.attrs = {"HostConfig": {"Links": ["/db:/app/db", "/cache:/app/cache"]}}
        result = _link_targets(container)
        assert result == {"db", "cache"}

    def test_handles_non_string_links(self):
        """Handle non-string entries in links."""
        container = Mock()
        container.attrs = {"HostConfig": {"Links": ["/db:/app/db", None, 123]}}
        result = _link_targets(container)
        assert result == {"db"}

    def test_strips_guerite_suffix(self):
        """Strip guerite suffix from link targets."""
        container = Mock()
        container.attrs = {"HostConfig": {"Links": ["/db-guerite-old-12345678:/app/db"]}}
        result = _link_targets(container)
        assert result == {"db"}


class TestLabelDependencies:
    """Test _label_dependencies function."""

    def test_returns_empty_set_when_no_labels(self):
        """Return empty set when no labels."""
        container = Mock()
        container.labels = None
        settings = Mock()
        settings.depends_label = "guerite.depends_on"
        assert _label_dependencies(container, settings) == set()

    def test_extracts_dependencies_from_label(self):
        """Extract dependencies from depends_on label."""
        container = Mock()
        container.labels = {"guerite.depends_on": "db,cache,redis"}
        settings = Mock()
        settings.depends_label = "guerite.depends_on"
        result = _label_dependencies(container, settings)
        assert result == {"db", "cache", "redis"}

    def test_returns_empty_when_label_missing(self):
        """Return empty set when depends_on label is missing."""
        container = Mock()
        container.labels = {"other.label": "value"}
        settings = Mock()
        settings.depends_label = "guerite.depends_on"
        assert _label_dependencies(container, settings) == set()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
