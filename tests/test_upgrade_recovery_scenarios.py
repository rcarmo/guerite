#!/usr/bin/env python3
"""Tests for upgrade recovery scenarios."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from guerite.monitor import (
    run_once,
    _track_upgrade_state,
    _get_tracked_upgrade_state,
    UpgradeState,
)
from guerite.config import Settings


class TestUpgradeRecoveryScenarios:
    """Test various upgrade recovery scenarios."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

    @patch("guerite.monitor._recover_stalled_upgrades")
    @patch("guerite.monitor.select_monitored_containers")
    @patch("guerite.monitor._order_by_compose")
    @patch("guerite.monitor._track_new_containers")
    @patch("guerite.monitor._flush_detect_notifications")
    @patch("guerite.monitor.prune_images")
    def test_upgrade_recovery_called_during_run_once(
        self, mock_prune, mock_flush, mock_track, mock_order, mock_select, mock_recover
    ):
        """Test that upgrade recovery is called during run_once."""
        # Setup
        mock_select.return_value = []
        mock_order.return_value = []

        client = Mock()
        settings = Settings(
            docker_host="unix:///var/run/docker.sock",
            update_label="guerite.update",
            restart_label="guerite.restart",
            recreate_label="guerite.recreate",
            health_label="guerite.health_check",
            health_backoff_seconds=300,
            health_check_timeout_seconds=60,
            prune_timeout_seconds=None,
            notifications=set(),
            timezone="UTC",
            pushover_token=None,
            pushover_user=None,
            pushover_api="https://api.pushover.net/1/messages.json",
            webhook_url=None,
            dry_run=False,
            log_level="INFO",
            state_file="/tmp/test_state.json",
            prune_cron=None,
            rollback_grace_seconds=3600,
            restart_retry_limit=3,
            depends_label="guerite.depends_on",
            action_cooldown_seconds=60,
            monitor_only=False,
            no_pull=False,
            no_restart=False,
            monitor_only_label="guerite.monitor_only",
            no_pull_label="guerite.no_pull",
            no_restart_label="guerite.no_restart",
            scope_label="guerite.scope",
            scope=None,
            include_containers=set(),
            exclude_containers=set(),
            rolling_restart=False,
            stop_timeout_seconds=None,
            lifecycle_hooks_enabled=False,
            hook_timeout_seconds=60,
            pre_check_label="guerite.lifecycle.pre_check",
            pre_update_label="guerite.lifecycle.pre_update",
            post_update_label="guerite.lifecycle.post_update",
            post_check_label="guerite.lifecycle.post_check",
            pre_update_timeout_label="guerite.lifecycle.pre_update_timeout_seconds",
            post_update_timeout_label="guerite.lifecycle.post_update_timeout_seconds",
            http_api_enabled=False,
            http_api_host="0.0.0.0",
            http_api_port=8080,
            http_api_token=None,
            http_api_metrics=False,
            run_once=False,
        )

        # Execute
        run_once(client, settings)

        # Verify recovery was called
        mock_recover.assert_called_once()

    @patch("guerite.monitor._recover_stalled_upgrades")
    @patch("guerite.monitor.select_monitored_containers")
    @patch("guerite.monitor._order_by_compose")
    @patch("guerite.monitor._track_new_containers")
    @patch("guerite.monitor._flush_detect_notifications")
    @patch("guerite.monitor.prune_images")
    def test_upgrade_recovery_with_custom_timestamp(
        self, mock_prune, mock_flush, mock_track, mock_order, mock_select, mock_recover
    ):
        """Test upgrade recovery with custom timestamp."""
        # Setup
        mock_select.return_value = []
        mock_order.return_value = []

        client = Mock()
        settings = Mock()  # Mock to avoid complex setup
        settings.scope = None
        settings.include_containers = set()
        settings.exclude_containers = set()
        settings.rolling_restart = False
        settings.lifecycle_hooks_enabled = False
        settings.hook_timeout_seconds = 60
        settings.monitor_only = False
        settings.no_pull = False
        settings.no_restart = False
        settings.monitor_only_label = "guerite.monitor_only"
        settings.no_pull_label = "guerite.no_pull"
        settings.no_restart_label = "guerite.no_restart"
        settings.pre_check_label = "guerite.lifecycle.pre_check"
        settings.post_check_label = "guerite.lifecycle.post_check"
        settings.pre_update_label = "guerite.lifecycle.pre_update"
        settings.post_update_label = "guerite.lifecycle.post_update"
        settings.pre_update_timeout_label = "guerite.lifecycle.pre_update_timeout_seconds"
        settings.post_update_timeout_label = "guerite.lifecycle.post_update_timeout_seconds"
        settings.health_backoff_seconds = 300
        settings.health_check_timeout_seconds = 60
        settings.update_label = "guerite.update"
        settings.restart_label = "guerite.restart"
        settings.recreate_label = "guerite.recreate"
        settings.health_label = "guerite.health_check"
        settings.depends_label = "guerite.depends_on"
        settings.dry_run = False
        settings.action_cooldown_seconds = 60
        settings.state_file = "/tmp/test_state.json"
        settings.prune_cron = None
        settings.notifications = set()
        settings.rollback_grace_seconds = 3600
        settings.restart_retry_limit = 3
        settings.stop_timeout_seconds = None
        timestamp = datetime.now(timezone.utc)

        # Execute
        run_once(client, settings, timestamp=timestamp)

        # Verify recovery was called with correct settings
        mock_recover.assert_called_once()

    @patch("guerite.monitor._recover_stalled_upgrades")
    @patch("guerite.monitor.select_monitored_containers")
    @patch("guerite.monitor._order_by_compose")
    @patch("guerite.monitor._track_new_containers")
    @patch("guerite.monitor._flush_detect_notifications")
    @patch("guerite.monitor.prune_images")
    def test_upgrade_recovery_with_custom_containers(
        self, mock_prune, mock_flush, mock_track, mock_order, mock_select, mock_recover
    ):
        """Test upgrade recovery with custom containers list."""
        # Setup
        custom_containers = []
        mock_order.return_value = custom_containers

        client = Mock()
        settings = Mock()
        settings.scope = None
        settings.include_containers = set()
        settings.exclude_containers = set()
        settings.rolling_restart = False
        settings.lifecycle_hooks_enabled = False
        settings.hook_timeout_seconds = 60
        settings.monitor_only = False
        settings.no_pull = False
        settings.no_restart = False
        settings.monitor_only_label = "guerite.monitor_only"
        settings.no_pull_label = "guerite.no_pull"
        settings.no_restart_label = "guerite.no_restart"
        settings.pre_check_label = "guerite.lifecycle.pre_check"
        settings.post_check_label = "guerite.lifecycle.post_check"
        settings.pre_update_label = "guerite.lifecycle.pre_update"
        settings.post_update_label = "guerite.lifecycle.post_update"
        settings.pre_update_timeout_label = "guerite.lifecycle.pre_update_timeout_seconds"
        settings.post_update_timeout_label = "guerite.lifecycle.post_update_timeout_seconds"
        settings.health_backoff_seconds = 300
        settings.health_check_timeout_seconds = 60
        settings.update_label = "guerite.update"
        settings.restart_label = "guerite.restart"
        settings.recreate_label = "guerite.recreate"
        settings.health_label = "guerite.health_check"
        settings.depends_label = "guerite.depends_on"
        settings.dry_run = False
        settings.action_cooldown_seconds = 60
        settings.state_file = "/tmp/test_state.json"
        settings.prune_cron = None
        settings.notifications = set()
        settings.rollback_grace_seconds = 3600
        settings.restart_retry_limit = 3
        settings.stop_timeout_seconds = None

        # Execute
        run_once(client, settings, containers=custom_containers)

        # Verify recovery was called and custom containers were used
        mock_recover.assert_called_once()
        mock_select.assert_not_called()  # Should not call select when containers provided


class TestUpgradeStatePersistence:
    """Test upgrade state persistence scenarios."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

    def test_multiple_concurrent_upgrades(self):
        """Test tracking multiple concurrent upgrades."""
        # Setup multiple upgrade states
        upgrade1 = UpgradeState(
            original_image_id="old111", target_image_id="new111", status="in-progress"
        )
        upgrade2 = UpgradeState(
            original_image_id="old222", target_image_id="new222", status="in-progress"
        )
        upgrade3 = UpgradeState(
            original_image_id="old333", target_image_id="new333", status="completed"
        )

        # Track multiple containers
        _track_upgrade_state("container1", upgrade1)
        _track_upgrade_state("container2", upgrade2)
        _track_upgrade_state("container3", upgrade3)

        # Verify all tracked
        assert _get_tracked_upgrade_state("container1") == upgrade1
        assert _get_tracked_upgrade_state("container2") == upgrade2
        assert _get_tracked_upgrade_state("container3") == upgrade3

        # Verify counts
        from guerite import monitor

        assert len(monitor._UPGRADE_STATE) == 3

    def test_upgrade_state_updates(self):
        """Test updating upgrade state for same container."""
        container_id = "container123"

        # Initial state
        initial_state = UpgradeState(
            original_image_id="old123", target_image_id="new123", status="in-progress"
        )
        _track_upgrade_state(container_id, initial_state)

        # Update state
        updated_state = UpgradeState(
            original_image_id="old123", target_image_id="new123", status="completed"
        )
        _track_upgrade_state(container_id, updated_state)

        # Verify updated state
        retrieved = _get_tracked_upgrade_state(container_id)
        assert retrieved is not None
        assert retrieved.status == "completed"
        assert retrieved.original_image_id == "old123"
        assert retrieved.target_image_id == "new123"

        # Verify only one entry
        from guerite import monitor

        assert len(monitor._UPGRADE_STATE) == 1

    @patch("guerite.monitor.now_utc")
    def test_upgrade_state_with_timestamps(self, mock_now):
        """Test upgrade state with proper timestamps."""
        started_time = datetime.now(timezone.utc)
        mock_now.return_value = started_time

        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new123",
            started_at=started_time,
            status="in-progress",
        )

        _track_upgrade_state("container123", upgrade_state)

        retrieved = _get_tracked_upgrade_state("container123")
        assert retrieved is not None
        assert retrieved.started_at == started_time
        assert isinstance(retrieved.started_at, datetime)


class TestUpgradeErrorHandling:
    """Test error handling in upgrade scenarios."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

    @patch("guerite.monitor._recover_stalled_upgrades")
    @patch("guerite.monitor.select_monitored_containers")
    @patch("guerite.monitor._order_by_compose")
    def test_upgrade_recovery_error_doesnt_crash_run_once(
        self, mock_order, mock_select, mock_recover
    ):
        """Test that errors in upgrade recovery don't crash run_once."""
        # Setup recovery to raise exception
        mock_recover.side_effect = Exception("Recovery error")

        mock_select.return_value = []
        mock_order.return_value = []

        client = Mock()
        settings = Mock()

        # Execute - should not raise
        try:
            run_once(client, settings)
        except Exception as e:
            pytest.fail(f"run_once should not crash on upgrade recovery error: {e}")

    @patch("guerite.monitor.now_utc")
    @patch("guerite.monitor._short_id")
    def test_upgrade_recovery_handles_missing_container(self, mock_short_id, mock_now):
        """Test upgrade recovery handling when container is missing."""
        from guerite.monitor import _recover_stalled_upgrades

        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(minutes=35)
        now_time = datetime.now(timezone.utc)
        mock_now.return_value = now_time
        mock_short_id.return_value = "abc123"

        # Create stalled upgrade for non-existent container
        container_id = "missing_container"
        upgrade_state = UpgradeState(started_at=start_time, status="in-progress")
        _track_upgrade_state(container_id, upgrade_state)

        # Mock container.get to raise exception
        client = Mock()
        client.containers.get.side_effect = Exception("Container not found")

        settings = Mock()
        settings.upgrade_stall_timeout_seconds = 1800
        event_log = []

        # Execute - should not crash
        _recover_stalled_upgrades(client, settings, event_log, True)

        # Should handle error gracefully
        assert len(event_log) == 0

    def test_upgrade_state_with_none_values(self):
        """Test upgrade state with None values."""
        upgrade_state = UpgradeState(
            original_image_id=None,
            target_image_id=None,
            started_at=None,
            status="in-progress",
        )

        _track_upgrade_state("container123", upgrade_state)

        retrieved = _get_tracked_upgrade_state("container123")
        assert retrieved is not None
        assert retrieved.original_image_id is None
        assert retrieved.target_image_id is None
        assert retrieved.started_at is None
        assert retrieved.status == "in-progress"


class TestUpgradeConfiguration:
    """Test upgrade-related configuration."""

    def test_default_stall_timeout(self):
        """Test default stall timeout behavior."""
        from guerite.monitor import _recover_stalled_upgrades

        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(minutes=35)

        upgrade_state = UpgradeState(started_at=start_time, status="in-progress")
        _track_upgrade_state("container123", upgrade_state)

        client = Mock()
        container = Mock()
        container.id = "container123"
        container.name = "test-app"
        client.containers.get.return_value = container

        # Settings without explicit stall timeout
        settings = Mock()
        # Deliberately not setting upgrade_stall_timeout_seconds
        delattr(settings, "upgrade_stall_timeout_seconds")

        event_log = []

        # Execute - should use default
        with patch("guerite.monitor.now_utc") as mock_now:
            mock_now.return_value = datetime.now(timezone.utc)
            _recover_stalled_upgrades(client, settings, event_log, True)

        # Should use default timeout (getattr should return 1800)
        assert len(event_log) == 1

    def test_custom_stall_timeout(self):
        """Test custom stall timeout behavior."""
        from guerite.monitor import _recover_stalled_upgrades

        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(
            minutes=10
        )  # 10 minutes ago

        upgrade_state = UpgradeState(started_at=start_time, status="in-progress")
        _track_upgrade_state("container123", upgrade_state)

        client = Mock()
        container = Mock()
        container.id = "container123"
        container.name = "test-app"
        client.containers.get.return_value = container

        # Settings with short custom timeout (5 minutes)
        settings = Mock()
        settings.upgrade_stall_timeout_seconds = 300  # 5 minutes

        event_log = []

        # Execute
        with patch("guerite.monitor.now_utc") as mock_now:
            mock_now.return_value = datetime.now(timezone.utc)
            _recover_stalled_upgrades(client, settings, event_log, True)

        # Should detect as stalled due to short timeout
        assert len(event_log) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
