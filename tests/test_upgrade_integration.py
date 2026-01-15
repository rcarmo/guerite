"""Upgrade integration tests (pytest style)."""


def test_upgrade_state_creation():
    from guerite.monitor import UpgradeState

    state = UpgradeState(original_image_id="old123", target_image_id="new456", status="in-progress")
    assert state.original_image_id == "old123"
    assert state.target_image_id == "new456"
    assert state.status == "in-progress"
    assert state.started_at is None


def test_upgrade_state_tracking():
    from guerite.monitor import UpgradeState, _track_upgrade_state, _get_tracked_upgrade_state
    from guerite import monitor

    monitor._UPGRADE_STATE.clear()
    state = UpgradeState(original_image_id="old123", target_image_id="new456", status="in-progress")
    _track_upgrade_state("container123", state)
    retrieved = _get_tracked_upgrade_state("container123")
    assert retrieved is not None
    assert retrieved.original_image_id == "old123"
    assert retrieved.target_image_id == "new456"
    assert retrieved.status == "in-progress"


def test_multiple_upgrade_states():
    from guerite.monitor import UpgradeState, _track_upgrade_state, _get_tracked_upgrade_state
    from guerite import monitor

    monitor._UPGRADE_STATE.clear()
    state1 = UpgradeState(status="in-progress", original_image_id="old1")
    state2 = UpgradeState(status="completed", original_image_id="old2")
    state3 = UpgradeState(status="failed", original_image_id="old3")
    _track_upgrade_state("container1", state1)
    _track_upgrade_state("container2", state2)
    _track_upgrade_state("container3", state3)
    assert _get_tracked_upgrade_state("container1").status == "in-progress"
    assert _get_tracked_upgrade_state("container2").status == "completed"
    assert _get_tracked_upgrade_state("container3").status == "failed"
    assert len(monitor._UPGRADE_STATE) == 3


def test_configuration_update():
    from guerite.config import load_settings, DEFAULT_UPGRADE_STALL_TIMEOUT_SECONDS

    assert DEFAULT_UPGRADE_STALL_TIMEOUT_SECONDS == 1800
    settings = load_settings()
    assert getattr(settings, "upgrade_stall_timeout_seconds", None) == 1800
