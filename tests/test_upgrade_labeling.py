#!/usr/bin/env python3
"""Tests for upgrade labeling and recovery functionality."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, MagicMock, patch

from guerite.monitor import (
    UpgradeState,
    _track_upgrade_state,
    _get_tracked_upgrade_state,
    _clear_tracked_upgrade_state,
    _recover_stalled_upgrades,
    restart_container,
    _strip_guerite_suffix,
)
from guerite.config import Settings


class TestUpgradeState:
    """Test UpgradeState dataclass."""

    def test_upgrade_state_creation(self):
        """Test creating an UpgradeState."""
        now = datetime.now(timezone.utc)
        state = UpgradeState(
            original_image_id="sha256:old123",
            target_image_id="sha256:new456",
            started_at=now,
            status="in-progress",
        )

        assert state.original_image_id == "sha256:old123"
        assert state.target_image_id == "sha256:new456"
        assert state.started_at == now
        assert state.status == "in-progress"

    def test_upgrade_state_defaults(self):
        """Test UpgradeState default values."""
        state = UpgradeState()

        assert state.original_image_id is None
        assert state.target_image_id is None
        assert state.started_at is None
        assert state.status == "unknown"


class TestUpgradeStateTracking:
    """Test upgrade state tracking functions."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        # Clear the global upgrade state dict
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

    def test_track_and_get_upgrade_state(self):
        """Test tracking and retrieving upgrade state."""
        container_id = "abc123"
        state = UpgradeState(
            original_image_id="old123", target_image_id="new456", status="in-progress"
        )

        _track_upgrade_state(container_id, state)
        retrieved = _get_tracked_upgrade_state(container_id)

        assert retrieved is not None
        assert retrieved.original_image_id == "old123"
        assert retrieved.target_image_id == "new456"
        assert retrieved.status == "in-progress"

    def test_get_nonexistent_upgrade_state(self):
        """Test getting state for non-existent container."""
        retrieved = _get_tracked_upgrade_state("nonexistent")
        assert retrieved is None

    def test_clear_upgrade_state(self):
        """Test clearing upgrade state."""
        container_id = "abc123"
        state = UpgradeState(status="in-progress")

        _track_upgrade_state(container_id, state)
        assert _get_tracked_upgrade_state(container_id) is not None

        _clear_tracked_upgrade_state(container_id)
        assert _get_tracked_upgrade_state(container_id) is None

    def test_track_with_none_container_id(self):
        """Test tracking with None container_id."""
        state = UpgradeState(status="in-progress")

        # Should not crash when container_id is None
        _track_upgrade_state(None, state)
        # Should not be found
        retrieved = _get_tracked_upgrade_state(None)
        assert retrieved is None


class TestStalledUpgradeRecovery:
    """Test stalled upgrade recovery functionality."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

    @patch("guerite.monitor.now_utc")
    def test_no_stalled_upgrades(self, mock_now):
        """Test when there are no stalled upgrades."""
        mock_now.return_value = datetime.now(timezone.utc)

        client = Mock()
        settings = Mock()
        event_log = []

        _recover_stalled_upgrades(client, settings, event_log, True)

        # Should not process anything
        assert len(event_log) == 0

    @patch("guerite.monitor.now_utc")
    @patch("guerite.monitor._short_id")
    def test_detect_stalled_upgrade(self, mock_short_id, mock_now):
        """Test detecting a stalled upgrade."""
        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(
            minutes=35
        )  # 35 minutes ago
        now_time = datetime.now(timezone.utc)
        mock_now.return_value = now_time
        mock_short_id.return_value = "abc123"

        # Create stalled upgrade state
        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            started_at=start_time,
            status="in-progress",
        )
        _track_upgrade_state(container_id, upgrade_state)

        # Mock container
        container = Mock()
        container.id = container_id
        container.name = "test-app"

        client = Mock()
        client.containers.get.return_value = container

        settings = Mock()
        settings.upgrade_stall_timeout_seconds = 1800  # 30 minutes

        event_log = []

        # Execute
        _recover_stalled_upgrades(client, settings, event_log, True)

        # Verify
        assert len(event_log) == 1
        assert "stalled upgrade" in event_log[0].lower()

        # Check that upgrade was marked as failed
        retrieved_state = _get_tracked_upgrade_state(container_id)
        assert retrieved_state is not None
        assert retrieved_state.status == "failed"

        client.containers.get.assert_called_once_with(container_id)

    @patch("guerite.monitor.now_utc")
    def test_not_stalled_if_within_threshold(self, mock_now):
        """Test that upgrade is not considered stalled if within threshold."""
        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(
            minutes=10
        )  # 10 minutes ago
        now_time = datetime.now(timezone.utc)
        mock_now.return_value = now_time

        # Create recent upgrade state
        container_id = "container123"
        upgrade_state = UpgradeState(started_at=start_time, status="in-progress")
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        settings = Mock()
        settings.upgrade_stall_timeout_seconds = 1800  # 30 minutes

        event_log = []

        # Execute
        _recover_stalled_upgrades(client, settings, event_log, True)

        # Verify - should not detect as stalled
        assert len(event_log) == 0

        # Status should remain in-progress
        retrieved_state = _get_tracked_upgrade_state(container_id)
        assert retrieved_state is not None
        assert retrieved_state.status == "in-progress"

    @patch("guerite.monitor.now_utc")
    def test_ignore_completed_upgrades(self, mock_now):
        """Test that completed upgrades are ignored."""
        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(hours=2)
        mock_now.return_value = datetime.now(timezone.utc)

        # Create completed upgrade state
        container_id = "container123"
        upgrade_state = UpgradeState(started_at=start_time, status="completed")
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        settings = Mock()
        settings.upgrade_stall_timeout_seconds = 1800

        event_log = []

        # Execute
        _recover_stalled_upgrades(client, settings, event_log, True)

        # Verify - should not process completed upgrades
        assert len(event_log) == 0

    @patch("guerite.monitor.now_utc")
    @patch("guerite.monitor._short_id")
    def test_handle_docker_error_during_recovery(self, mock_short_id, mock_now):
        """Test handling Docker errors during recovery."""
        # Setup
        start_time = datetime.now(timezone.utc) - timedelta(minutes=35)
        now_time = datetime.now(timezone.utc)
        mock_now.return_value = now_time
        mock_short_id.return_value = "abc123"

        # Create stalled upgrade state
        container_id = "container123"
        upgrade_state = UpgradeState(started_at=start_time, status="in-progress")
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        client.containers.get.side_effect = Exception("Docker error")

        settings = Mock()
        settings.upgrade_stall_timeout_seconds = 1800

        event_log = []

        # Execute - should not crash
        _recover_stalled_upgrades(client, settings, event_log, True)

        # Should handle error gracefully
        assert len(event_log) == 0


class TestUpgradeIntegration:
    """Test upgrade integration with restart_container."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

    @patch("guerite.monitor._register_restart_failure")
    @patch("guerite.monitor._rollback_container_recreation")
    @patch("guerite.monitor.remove_old_image")
    @patch("guerite.monitor._mark_action")
    @patch("guerite.monitor._preflight_mounts")
    def test_successful_upgrade_tracking(
        self, mock_preflight, mock_mark, mock_remove, mock_rollback, mock_register
    ):
        """Test successful upgrade tracks state correctly."""
        # Setup mocks
        client = Mock()
        container = Mock()
        container.id = "container123"
        container.name = "test-app"
        container.image.id = "old123"

        # Mock successful recreation
        client.api.rename.return_value = None
        client.api.create_container.return_value = {"Id": "new456"}
        client.api.start.return_value = None
        container.remove.return_value = None

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
        )

        event_log = []

        # Execute with is_upgrade=True
        result = restart_container(
            client,
            container,
            "test:latest",
            "new456",
            settings,
            event_log,
            True,
            is_upgrade=True,
        )

        # Verify upgrade state was tracked
        upgrade_state = _get_tracked_upgrade_state("container123")
        assert upgrade_state is not None
        assert upgrade_state.status == "completed"
        assert upgrade_state.original_image_id == "old123"
        assert upgrade_state.target_image_id == "new456"
        assert upgrade_state.started_at is not None

    @patch("guerite.monitor._register_restart_failure")
    @patch("guerite.monitor._rollback_container_recreation")
    @patch("guerite.monitor._mark_action")
    @patch("guerite.monitor._preflight_mounts")
    def test_failed_upgrade_tracking(
        self, mock_preflight, mock_mark, mock_rollback, mock_register
    ):
        """Test failed upgrade tracks state correctly."""
        # Setup mocks
        client = Mock()
        container = Mock()
        container.id = "container123"
        container.name = "test-app"
        container.image.id = "old123"

        # Mock failed recreation
        client.api.rename.side_effect = Exception("Rename failed")
        mock_rollback.return_value = True

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
        )

        event_log = []

        # Execute with is_upgrade=True
        result = restart_container(
            client,
            container,
            "test:latest",
            "new456",
            settings,
            event_log,
            True,
            is_upgrade=True,
        )

        # Verify result is False (failed)
        assert result is False

        # Verify upgrade state was tracked as failed
        upgrade_state = _get_tracked_upgrade_state("container123")
        assert upgrade_state is not None
        assert upgrade_state.status == "failed"
        assert upgrade_state.original_image_id == "old123"
        assert upgrade_state.target_image_id == "new456"

    def test_non_upgrade_does_not_track_state(self):
        """Test that non-upgrade restarts don't track upgrade state."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()

        # Setup minimal mocks
        client = Mock()
        container = Mock()
        container.id = "container123"
        container.name = "test-app"

        settings = Mock()
        event_log = []

        # Execute with is_upgrade=False (default)
        # This will likely fail due to insufficient mocking, but we're checking state tracking
        try:
            restart_container(
                client,
                container,
                "test:latest",
                "new456",
                settings,
                event_log,
                True,
                is_upgrade=False,
            )
        except Exception:
            pass  # Expected due to minimal mocking

        # Verify no upgrade state was tracked
        upgrade_state = _get_tracked_upgrade_state("container123")
        assert upgrade_state is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
