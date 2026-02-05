#!/usr/bin/env python3
"""Tests for upgrade labeling and recovery functionality."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from guerite.monitor import (
    UpgradeState,
    _track_upgrade_state,
    _get_tracked_upgrade_state,
    _clear_tracked_upgrade_state,
    _recover_stalled_upgrades,
    _reconcile_failed_upgrades,
    _check_for_manual_intervention,
    restart_container,
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

        event_log = []

        # Execute with is_upgrade=True
        restart_container(
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


class TestFailedUpgradeReconciliation:
    """Test failed upgrade reconciliation with manual updates."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()
        monitor._UPGRADE_STATE_NOTIFIED.clear()
        monitor._RESTART_BACKOFF.clear()
        monitor._RESTART_FAIL_COUNT.clear()

    def test_manual_upgrade_clears_failed_state(self):
        """Clear failed upgrade state when container image changes."""
        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"
        container.image.id = "new456"

        client = Mock()
        client.containers.get.return_value = container

        event_log = []
        _reconcile_failed_upgrades(client, {"app": container}, event_log, True)

        assert _get_tracked_upgrade_state(container_id) is None
        assert len(event_log) == 1
        assert "manual upgrade" in event_log[0].lower()

    def test_no_change_keeps_failed_state(self):
        """Retain failed upgrade state if image unchanged."""
        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"
        container.image.id = "old123"

        client = Mock()
        client.containers.get.return_value = container

        event_log = []
        _reconcile_failed_upgrades(client, {"app": container}, event_log, True)

        assert _get_tracked_upgrade_state(container_id) is not None
        assert event_log == []

    def test_container_found_by_base_name_after_recreate(self):
        """Find container by base_name when ID lookup fails (container recreated)."""
        from docker.errors import NotFound
        from guerite import monitor

        old_container_id = "old_container_id"
        new_container_id = "new_container_id"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(old_container_id, upgrade_state)
        monitor._RESTART_BACKOFF[old_container_id] = 100.0

        new_container = Mock()
        new_container.id = new_container_id
        new_container.name = "app"
        new_container.image.id = "new456"

        client = Mock()
        client.containers.get.side_effect = NotFound("not found")

        event_log = []
        _reconcile_failed_upgrades(client, {"app": new_container}, event_log, True)

        assert _get_tracked_upgrade_state(old_container_id) is None
        assert old_container_id not in monitor._RESTART_BACKOFF
        assert new_container_id not in monitor._RESTART_BACKOFF

    def test_base_name_backfill_persists_state(self):
        """Backfill base_name when missing and persist to state file."""
        from guerite import monitor
        from unittest.mock import patch

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name=None,
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "myapp"
        container.image.id = "old123"  # Still on original, no clear

        client = Mock()
        client.containers.get.return_value = container

        with patch.object(monitor, "_save_upgrade_state") as mock_save:
            _reconcile_failed_upgrades(
                client, {"myapp": container}, [], True, state_file="/tmp/state.json"
            )
            mock_save.assert_called_once_with("/tmp/state.json")

        state = _get_tracked_upgrade_state(container_id)
        assert state is not None
        assert state.base_name == "myapp"

    def test_backoff_cleanup_for_both_ids(self):
        """Clear backoff for both tracked ID and current container ID."""
        from guerite import monitor

        old_id = "old_container_id"
        new_id = "new_container_id"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(old_id, upgrade_state)
        monitor._RESTART_BACKOFF[old_id] = 100.0
        monitor._RESTART_BACKOFF[new_id] = 200.0
        monitor._RESTART_FAIL_COUNT[old_id] = 3
        monitor._RESTART_FAIL_COUNT[new_id] = 2

        container = Mock()
        container.id = new_id
        container.name = "app"
        container.image.id = "new456"

        client = Mock()
        client.containers.get.return_value = container

        _reconcile_failed_upgrades(client, {"app": container}, [], True)

        assert old_id not in monitor._RESTART_BACKOFF
        assert new_id not in monitor._RESTART_BACKOFF
        assert old_id not in monitor._RESTART_FAIL_COUNT
        assert new_id not in monitor._RESTART_FAIL_COUNT

    def test_notify_false_no_event_log(self):
        """No event_log entry when notify is False."""
        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"
        container.image.id = "new456"

        client = Mock()
        client.containers.get.return_value = container

        event_log = []
        _reconcile_failed_upgrades(client, {"app": container}, event_log, False)

        assert _get_tracked_upgrade_state(container_id) is None
        assert event_log == []

    def test_non_failed_status_skipped(self):
        """Upgrades with non-failed status are ignored."""
        from guerite import monitor

        for status in ["in-progress", "completed", "unknown"]:
            monitor._UPGRADE_STATE.clear()
            container_id = f"container_{status}"
            upgrade_state = UpgradeState(
                original_image_id="old123",
                target_image_id="new456",
                status=status,
                base_name="app",
            )
            _track_upgrade_state(container_id, upgrade_state)

            container = Mock()
            container.id = container_id
            container.name = "app"
            container.image.id = "new456"

            client = Mock()
            client.containers.get.return_value = container

            _reconcile_failed_upgrades(client, {"app": container}, [], True)

            assert _get_tracked_upgrade_state(container_id) is not None

    def test_no_original_id_with_target_mismatch_skipped(self):
        """Skip when no original_id but current doesn't match target."""
        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id=None,
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"
        container.image.id = "some_other_image"

        client = Mock()
        client.containers.get.return_value = container

        event_log = []
        _reconcile_failed_upgrades(client, {"app": container}, event_log, True)

        assert _get_tracked_upgrade_state(container_id) is not None
        assert event_log == []

    def test_current_image_id_none_skipped(self):
        """Skip when current_image_id returns None."""
        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"
        container.image.id = None  # Will cause current_image_id to return None

        client = Mock()
        client.containers.get.return_value = container

        event_log = []
        _reconcile_failed_upgrades(client, {"app": container}, event_log, True)

        assert _get_tracked_upgrade_state(container_id) is not None

    def test_container_not_found_anywhere_skipped(self):
        """Skip when container not found by ID or base_name."""
        from docker.errors import NotFound

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        client.containers.get.side_effect = NotFound("not found")

        event_log = []
        _reconcile_failed_upgrades(client, {}, event_log, True)  # Empty base_map

        assert _get_tracked_upgrade_state(container_id) is not None

    def test_clears_upgrade_state_notified(self):
        """Verify _UPGRADE_STATE_NOTIFIED is cleared on reconciliation."""
        from guerite import monitor

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)
        monitor._UPGRADE_STATE_NOTIFIED.add(container_id)

        container = Mock()
        container.id = container_id
        container.name = "app"
        container.image.id = "new456"

        client = Mock()
        client.containers.get.return_value = container

        _reconcile_failed_upgrades(client, {"app": container}, [], True)

        assert container_id not in monitor._UPGRADE_STATE_NOTIFIED


class TestCheckForManualIntervention:
    """Test _check_for_manual_intervention function."""

    def setup_method(self):
        """Clear upgrade state before each test."""
        from guerite import monitor

        monitor._UPGRADE_STATE.clear()
        monitor._UPGRADE_STATE_NOTIFIED.clear()

    def test_notifies_failed_upgrade(self):
        """Notify about failed upgrade requiring manual intervention."""
        from guerite import monitor

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"

        client = Mock()
        client.containers.get.return_value = container

        settings = Mock()
        event_log = []
        _check_for_manual_intervention(client, settings, event_log, True)

        assert container_id in monitor._UPGRADE_STATE_NOTIFIED
        assert len(event_log) == 1
        assert "manual intervention" in event_log[0].lower()

    def test_skips_already_notified(self):
        """Skip containers already notified."""
        from guerite import monitor

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)
        monitor._UPGRADE_STATE_NOTIFIED.add(container_id)

        client = Mock()
        settings = Mock()
        event_log = []
        _check_for_manual_intervention(client, settings, event_log, True)

        assert event_log == []

    def test_skips_non_failed_status(self):
        """Skip upgrades with non-failed status."""
        from guerite import monitor

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="in-progress",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        settings = Mock()
        event_log = []
        _check_for_manual_intervention(client, settings, event_log, True)

        assert container_id not in monitor._UPGRADE_STATE_NOTIFIED
        assert event_log == []

    def test_clears_state_for_missing_container(self):
        """Clear upgrade state when container no longer exists."""
        from docker.errors import NotFound

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        client.containers.get.side_effect = NotFound("not found")

        settings = Mock()
        event_log = []
        _check_for_manual_intervention(client, settings, event_log, True)

        assert _get_tracked_upgrade_state(container_id) is None

    def test_notify_false_no_event_log(self):
        """No event_log entry when notify is False."""
        from guerite import monitor

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="app",
        )
        _track_upgrade_state(container_id, upgrade_state)

        container = Mock()
        container.id = container_id
        container.name = "app"

        client = Mock()
        client.containers.get.return_value = container

        settings = Mock()
        event_log = []
        _check_for_manual_intervention(client, settings, event_log, False)

        assert container_id in monitor._UPGRADE_STATE_NOTIFIED
        assert event_log == []

    def test_uses_base_name_from_state(self):
        """Use base_name from upgrade state when container lookup fails initially."""
        from docker.errors import NotFound

        container_id = "container123"
        upgrade_state = UpgradeState(
            original_image_id="old123",
            target_image_id="new456",
            status="failed",
            base_name="myapp",
        )
        _track_upgrade_state(container_id, upgrade_state)

        client = Mock()
        # First call for name lookup fails, second call for existence check also fails
        client.containers.get.side_effect = NotFound("not found")

        settings = Mock()
        event_log = []
        _check_for_manual_intervention(client, settings, event_log, True)

        assert len(event_log) == 1
        assert "myapp" in event_log[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
