from types import SimpleNamespace

import pytest

from guerite import monitor
from guerite.config import Settings
from guerite import notifier


class FakeResponse:
    def __init__(self, status=200, reason="OK"):
        self.status = status
        self.reason = reason


class FakeConnection:
    def __init__(self, status=200, reason="OK"):
        self.calls = []
        self.status = status
        self.reason = reason

    def request(self, method, path, body=None, headers=None):
        self.calls.append((method, path, body, headers))

    def getresponse(self):
        return FakeResponse(self.status, self.reason)

    def close(self):
        self.calls.append("closed")


@pytest.fixture
def settings() -> Settings:
    return Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        notifications={"update"},
        timezone="UTC",
        pushover_token="token",
        pushover_user="user",
        pushover_api="https://api.example/endpoint",
        webhook_url="https://hook.example/hit",
        dry_run=False,
        log_level="INFO",
        state_file="/tmp/state",
        prune_cron=None,
        rollback_grace_seconds=3600,
        restart_retry_limit=3,
        depends_label="guerite.depends_on",
        action_cooldown_seconds=60,
    )


def test_notify_pushover_skips_when_missing(settings: Settings, caplog):
    settings = settings.__class__(**{**settings.__dict__, "pushover_token": None})
    caplog.set_level("DEBUG")
    notifier.notify_pushover(settings, "title", "body")
    assert "Pushover disabled" in caplog.text


def test_notify_pushover_sends_and_warns(monkeypatch, settings: Settings, caplog):
    fake = FakeConnection(status=500, reason="boom")
    monkeypatch.setattr(notifier, "HTTPSConnection", lambda netloc: fake)
    caplog.set_level("WARNING")
    notifier.notify_pushover(settings, "title", "body")
    assert any("Pushover returned" in msg for msg in caplog.messages)
    assert fake.calls and fake.calls[-1] == "closed"


def test_notify_webhook_skips_when_missing(settings: Settings, caplog):
    settings = settings.__class__(**{**settings.__dict__, "webhook_url": None})
    caplog.set_level("DEBUG")
    notifier.notify_webhook(settings, "title", "body")
    assert "Webhook disabled" in caplog.text


def test_notify_webhook_sends(monkeypatch, settings: Settings, caplog):
    fake = FakeConnection(status=200)
    monkeypatch.setattr(notifier, "HTTPSConnection", lambda netloc: fake)
    notifier.notify_webhook(settings, "title", "body")
    assert fake.calls and fake.calls[-1] == "closed"


class DummyAPI:
    def __init__(self):
        self.calls = []
        self.fail_connect = False

    def rename(self, cid, name):
        self.calls.append(("rename", cid, name))

    def create_container(self, **kwargs):
        self.calls.append(("create_container", kwargs))
        return {"Id": "new-id"}

    def create_endpoint_config(self, **kwargs):  # pragma: no cover - unused in these tests
        return {}

    def create_networking_config(self, *args, **kwargs):  # pragma: no cover - unused
        return {}

    def connect_container_to_network(self, *args, **kwargs):  # pragma: no cover - unused
        if self.fail_connect:
            raise monitor.APIError("fail", None, None)  # type: ignore[arg-type]
        self.calls.append(("connect", args, kwargs))

    def start(self, cid):
        self.calls.append(("start", cid))

    def stop(self, cid):
        self.calls.append(("stop", cid))

    def remove_container(self, cid, force=False):
        self.calls.append(("remove", cid, force))


class DummyImages:
    def __init__(self):
        self.calls = []

    def remove(self, image):  # pragma: no cover - only used indirectly
        self.calls.append(("remove", image))


class DummyContainer:
    def __init__(self, name: str, healthcheck=False):
        self.name = name
        self.id = f"{name}-id"
        self.labels = {}
        self.attrs = {
            "Config": {"Image": "repo:tag", "Healthcheck": {"Test": ["CMD-SHELL", "true"]} if healthcheck else None},
            "HostConfig": {},
            "NetworkSettings": {"Networks": {}},
            "State": {"Health": {"Status": "healthy"}},
        }
        self.image = SimpleNamespace(id="old-img")
        self.stopped = False
        self.removed = False
        self.started = False

    def stop(self):
        self.stopped = True

    def start(self):
        self.started = True

    def remove(self):
        self.removed = True


class DummyClient:
    def __init__(self):
        self.api = DummyAPI()
        self.images = DummyImages()


@pytest.fixture
def restart_settings(tmp_path) -> Settings:
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


def test_restart_container_happy_path(monkeypatch, restart_settings: Settings):
    client = DummyClient()
    container = DummyContainer("app")

    event_log = []
    assert monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=True,
    ) is True
    # rename old, create new, start new, remove old
    first_call = client.api.calls[0]
    assert first_call[0] == "rename" and first_call[1] == container.id
    assert "app-guerite-old-" in first_call[2]
    assert any(call[0] == "start" for call in client.api.calls)
    assert container.removed is True


def test_restart_container_health_rollback(monkeypatch, restart_settings: Settings):
    client = DummyClient()
    container = DummyContainer("app", healthcheck=True)

    # Force health wait to fail
    monkeypatch.setattr(monitor, "_wait_for_healthy", lambda *args, **kwargs: (False, "unhealthy"))

    event_log = []
    result = monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=True,
    )
    assert result is False
    assert any("Rolled back" in entry or "Failed health rollback" in entry for entry in event_log)


def test_save_health_backoff_errors_are_handled(tmp_path, caplog):
    caplog.set_level("DEBUG")
    monitor._HEALTH_BACKOFF.clear()
    monitor._HEALTH_BACKOFF["cont"] = monitor.now_utc()
    bad_path = tmp_path / "dir" / "state.json"
    # directory does not exist; should log and not raise
    monitor._save_health_backoff(str(bad_path))
    assert any("Failed to persist health backoff" in msg for msg in caplog.messages)
