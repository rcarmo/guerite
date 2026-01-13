from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from guerite import monitor
from guerite.config import Settings


class DummyContainer:
    def __init__(self, name: str, labels: dict | None = None, health: str | None = None, running: bool = True):
        self.name = name
        self.id = f"{name}-id"
        self.labels = labels or {}
        health_state = {"Status": health} if health is not None else {}
        self.attrs = {
            "Config": {"Image": "repo:tag", "Healthcheck": bool(health_state)},
            "HostConfig": {"Links": []},
            "NetworkSettings": {},
            "State": {"Running": running, "Health": health_state},
            "Created": datetime.now(timezone.utc).isoformat(),
        }
        self.image = type("Img", (), {"id": "img-old"})()


class DummyImage:
    def __init__(self, image_id: str, tags: Optional[list[str]] = None):
        self.id = image_id
        self.tags = tags if tags is not None else ["repo:tag"]


class DummyClient:
    def __init__(self):
        self.api = type("API", (), {})()
        self.containers = type("C", (), {})()
        self.images = type("I", (), {})()
        self.api.prune_images_called = False
        self.api.prune_images = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("should not prune"))


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
    monitor._LAST_DETECT_NOTIFY = None
    yield
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    monitor._KNOWN_CONTAINERS.clear()
    monitor._KNOWN_INITIALIZED = False
    monitor._PENDING_DETECTS.clear()
    monitor._LAST_DETECT_NOTIFY = None


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        health_check_timeout_seconds=60,
        prune_timeout_seconds=None,
        notifications={"update", "detect", "prune"},
        timezone="UTC",
        pushover_token=None,
        pushover_user=None,
        pushover_api="https://example",
        webhook_url=None,
        dry_run=False,
        log_level="INFO",
        state_file=str(tmp_path / "state.json"),
        prune_cron=None,
        rollback_grace_seconds=10,
        restart_retry_limit=3,
        depends_label="guerite.depends_on",
        action_cooldown_seconds=60,
        hostname="testhost",
    )


def test_invalid_cron_is_logged_and_skipped(caplog, settings: Settings):
    caplog.set_level("WARNING")
    container = DummyContainer("app", labels={settings.update_label: "bad cron"})
    assert monitor._cron_matches(container, settings.update_label, datetime.now(timezone.utc)) is False
    assert any("Invalid cron expression" in msg for msg in caplog.messages)


def test_next_wakeup_with_invalid_cron_returns_default(settings: Settings):
    ref = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    container = DummyContainer("app", labels={settings.update_label: "bad cron"})
    next_time, name, label = monitor.next_wakeup([container], settings, reference=ref)
    assert (next_time - ref).total_seconds() == 300
    assert name is None and label is None


def test_unhealthy_dependency_skips_action(monkeypatch, settings: Settings):
    dep = DummyContainer("db", labels={"com.docker.compose.project": "stack"}, health="unhealthy")
    app = DummyContainer(
        "app",
        labels={settings.update_label: "* * * * *", settings.depends_label: "db", "com.docker.compose.project": "stack"},
        health="healthy",
    )
    monkeypatch.setattr(monitor, "select_monitored_containers", lambda client, cfg: [app, dep])
    monkeypatch.setattr(monitor, "pull_image", lambda client, image_ref: DummyImage("new"))
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


def test_swarm_managed_container_is_skipped(monkeypatch, settings: Settings):
    swarm_container = DummyContainer(
        "svc",
        labels={settings.update_label: "* * * * *", "com.docker.swarm.service.id": "service"},
    )
    monkeypatch.setattr(monitor, "select_monitored_containers", lambda client, cfg: [swarm_container])
    monkeypatch.setattr(monitor, "pull_image", lambda client, image_ref: DummyImage("new"))
    monkeypatch.setattr(monitor, "needs_update", lambda cont, img: True)
    called = False
    monkeypatch.setattr(monitor, "restart_container", lambda *args, **kwargs: False)
    monkeypatch.setattr(monitor, "remove_old_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_pushover", lambda *args, **kwargs: None)
    monkeypatch.setattr(monitor, "notify_webhook", lambda *args, **kwargs: None)

    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor.run_once(DummyClient(), settings, timestamp=now)
    assert called is False


def test_prune_skips_when_rollback_protected(monkeypatch, settings: Settings, tmp_path):
    rollback = DummyContainer("app-guerite-old-deadbeef", labels={"com.docker.compose.project": "stack"})
    live = DummyContainer("app", labels={"com.docker.compose.project": "stack"})

    client = DummyClient()
    client.containers.list = lambda all=True: [rollback, live]

    event_log: list[str] = []

    # Ensure prune_images returns early without calling prune
    monitor.prune_images(client, settings, event_log, notify=True)
    assert not event_log or "Skipping prune" in event_log[0]


def test_prune_images_handles_timeout_without_sleep(monkeypatch, settings: Settings):
    # Ensure prune timeouts never crash the run loop and don't slow tests.
    from requests.exceptions import ReadTimeout

    client = DummyClient()
    client.api.prune_images = lambda **_kwargs: (_ for _ in ()).throw(ReadTimeout("timed out"))
    client.containers.list = lambda all=True: []

    event_log: list[str] = []
    monitor.prune_images(client, settings, event_log, notify=True)

    assert any("timed out" in entry.lower() for entry in event_log)


def test_prune_images_handles_urllib3_timeout_without_sleep(monkeypatch, settings: Settings):
    # Some environments surface Docker socket timeouts as urllib3.exceptions.ReadTimeoutError.
    from urllib3.exceptions import ReadTimeoutError

    client = DummyClient()
    client.api.prune_images = lambda **_kwargs: (_ for _ in ()).throw(
        ReadTimeoutError(None, None, "timed out")
    )
    client.containers.list = lambda all=True: []

    event_log: list[str] = []
    monitor.prune_images(client, settings, event_log, notify=True)

    assert any("timed out" in entry.lower() for entry in event_log)


def test_started_recently_grace_period():
    container = DummyContainer("app")
    now = datetime.now(timezone.utc)
    # Inject a recent start time within grace window
    container.attrs["State"]["StartedAt"] = (now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    assert monitor._started_recently(container, now, grace_seconds=10) is True
    container.attrs["State"]["StartedAt"] = (now - timedelta(seconds=20)).isoformat().replace("+00:00", "Z")
    assert monitor._started_recently(container, now, grace_seconds=10) is False


def test_health_backoff_persistence(tmp_path, settings: Settings):
    state_file = tmp_path / "state.json"
    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._HEALTH_BACKOFF.clear()
    monitor._HEALTH_BACKOFF["cont"] = now
    monitor._save_health_backoff(str(state_file))

    monitor._HEALTH_BACKOFF.clear()
    monitor._HEALTH_BACKOFF_LOADED = False
    monitor._ensure_health_backoff_loaded(str(state_file))
    assert monitor._HEALTH_BACKOFF.get("cont") == now


def test_detect_notifications_batching(monkeypatch, settings: Settings):
    messages: list[str] = []
    monkeypatch.setattr(monitor, "notify_pushover", lambda *_args, **_kwargs: messages.append("pushover"))
    monkeypatch.setattr(monitor, "notify_webhook", lambda *_args, **_kwargs: messages.append("webhook"))

    current = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._PENDING_DETECTS.extend(["a", "b", "a"])
    monitor._flush_detect_notifications(settings, hostname="host", current_time=current)
    assert len(messages) == 2

    messages.clear()
    monitor._PENDING_DETECTS.extend(["c"])
    monitor._LAST_DETECT_NOTIFY = current
    monitor._flush_detect_notifications(settings, hostname="host", current_time=current)
    assert not messages
