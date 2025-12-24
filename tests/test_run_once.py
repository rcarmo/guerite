from datetime import datetime, timezone

import pytest

from guerite import monitor
from guerite.config import Settings


class FakeImage:
    def __init__(self, image_id: str):
        self.id = image_id


class DummyContainer:
    def __init__(self, name: str, labels: dict | None = None, running: bool = True):
        self.name = name
        self.id = f"{name}-id"
        self.labels = labels or {}
        self.attrs = {
            "Config": {"Image": "repo:tag"},
            "HostConfig": {"Links": []},
            "NetworkSettings": {},
            "State": {"Running": running},
        }
        self.image = FakeImage("old")

    def restart(self) -> None:  # pragma: no cover - only used when not monkeypatched
        raise RuntimeError("restart should be monkeypatched")


class DummyClient:
    pass


@pytest.fixture(autouse=True)
def reset_state(tmp_path):
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    monitor._KNOWN_CONTAINERS.clear()
    monitor._KNOWN_INITIALIZED = False
    monitor._PENDING_DETECTS.clear()
    yield
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    monitor._KNOWN_CONTAINERS.clear()
    monitor._KNOWN_INITIALIZED = False
    monitor._PENDING_DETECTS.clear()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        notifications={"update"},
        timezone="UTC",
        pushover_token=None,
        pushover_user=None,
        pushover_api="https://example",
        webhook_url=None,
        dry_run=False,
        log_level="INFO",
        state_file=str(tmp_path / "state.json"),
        prune_cron=None,
        rollback_grace_seconds=3600,
        restart_retry_limit=3,
        depends_label="guerite.depends_on",
        action_cooldown_seconds=60,
    )


def test_run_once_triggers_update_and_restart(monkeypatch, settings: Settings):
    container = DummyContainer(
        "app",
        labels={settings.update_label: "* * * * *"},
    )

    calls: list[tuple] = []

    monkeypatch.setattr(monitor, "select_monitored_containers", lambda client, cfg: [container])
    monkeypatch.setattr(monitor, "pull_image", lambda client, image_ref: FakeImage("new"))
    monkeypatch.setattr(monitor, "needs_update", lambda cont, img: True)

    def fake_restart(client, cont, image_ref, new_image_id, cfg, event_log, notify):
        calls.append((cont.name, image_ref, new_image_id))
        return True

    monkeypatch.setattr(monitor, "restart_container", fake_restart)

    def fake_remove(client, old_image_id, new_image_id, event_log, notify):
        calls.append(("remove", old_image_id, new_image_id))

    monkeypatch.setattr(monitor, "remove_old_image", fake_remove)
    monkeypatch.setattr(monitor, "notify_pushover", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_webhook", lambda *args, **kwargs: None)

    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor.run_once(DummyClient(), settings, timestamp=now)

    assert ("app", "repo:tag", "new") in calls
    assert ("remove", "old", "new") in calls


def test_run_once_skips_when_in_cooldown(monkeypatch, settings: Settings):
    container = DummyContainer(
        "app",
        labels={settings.update_label: "* * * * *"},
    )

    monkeypatch.setattr(monitor, "select_monitored_containers", lambda client, cfg: [container])
    monkeypatch.setattr(monitor, "pull_image", lambda client, image_ref: FakeImage("new"))
    monkeypatch.setattr(monitor, "needs_update", lambda cont, img: True)

    called = False

    def fake_restart(*args, **kwargs):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(monitor, "restart_container", fake_restart)
    monkeypatch.setattr(monitor, "remove_old_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_pushover", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_webhook", lambda *args, **kwargs: None)

    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._LAST_ACTION[container.name] = now
    monitor.run_once(DummyClient(), settings, timestamp=now)

    assert called is False


def test_run_once_skips_due_to_dependency(monkeypatch, settings: Settings):
    dep = DummyContainer(
        "db",
        labels={"com.docker.compose.project": "stack"},
        running=False,
    )
    app = DummyContainer(
        "app",
        labels={
            settings.update_label: "* * * * *",
            settings.depends_label: "db",
            "com.docker.compose.project": "stack",
        },
    )

    monkeypatch.setattr(monitor, "select_monitored_containers", lambda client, cfg: [app, dep])
    monkeypatch.setattr(monitor, "pull_image", lambda client, image_ref: FakeImage("new"))
    monkeypatch.setattr(monitor, "needs_update", lambda cont, img: True)

    called = False

    def fake_restart(*args, **kwargs):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(monitor, "restart_container", fake_restart)
    monkeypatch.setattr(monitor, "remove_old_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_pushover", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_webhook", lambda *args, **kwargs: None)

    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor.run_once(DummyClient(), settings, timestamp=now)

    assert called is False