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
        health_check_timeout_seconds=60,
        prune_timeout_seconds=None,
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
    monkeypatch.setattr(notifier, "HTTPSConnection", lambda netloc, timeout=None: fake)
    caplog.set_level("WARNING")
    notifier.notify_pushover(settings, "title", "body")
    assert any("Pushover returned" in msg for msg in caplog.messages)
    assert fake.calls and fake.calls[-1] == "closed"
    method, path, body, headers = fake.calls[0]
    assert method == "POST"
    assert "title=title" in body.decode()
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"


def test_notify_webhook_skips_when_missing(settings: Settings, caplog):
    settings = settings.__class__(**{**settings.__dict__, "webhook_url": None})
    caplog.set_level("DEBUG")
    notifier.notify_webhook(settings, "title", "body")
    assert "Webhook disabled" in caplog.text


def test_notify_webhook_sends(monkeypatch, settings: Settings, caplog):
    fake = FakeConnection(status=200)
    monkeypatch.setattr(notifier, "HTTPSConnection", lambda netloc, timeout=None: fake)
    notifier.notify_webhook(settings, "title", "body")
    assert fake.calls and fake.calls[-1] == "closed"
    method, path, body, headers = fake.calls[0]
    assert method == "POST"
    assert b"title" in body and b"body" in body
    assert headers["Content-Type"] == "application/json"


class DummyAPI:
    def __init__(self):
        self.calls = []
        self.fail_connect = False
        self.raise_priority_type_error = False
        self.endpoint_kwargs = {}
        self.remove_failures_remaining = 0

    def rename(self, cid, name):
        self.calls.append(("rename", cid, name))

    def create_container(self, **kwargs):
        self.calls.append(("create_container", kwargs))
        return {"Id": "new-id"}

    def create_endpoint_config(self, **kwargs):
        if self.raise_priority_type_error and "priority" in kwargs:
            raise TypeError("unexpected priority")
        self.endpoint_kwargs = kwargs
        return {}

    def create_networking_config(self, *args, **kwargs):  # pragma: no cover - unused
        return {}

    def connect_container_to_network(
        self, *args, **kwargs
    ):  # pragma: no cover - unused
        if self.fail_connect:
            raise monitor.APIError("fail", None, None)  # type: ignore[arg-type]
        self.calls.append(("connect", args, kwargs))

    def start(self, cid):
        self.calls.append(("start", cid))

    def stop(self, cid):
        self.calls.append(("stop", cid))

    def remove_container(self, cid, force=False):
        self.calls.append(("remove", cid, force))
        if self.remove_failures_remaining > 0:
            self.remove_failures_remaining -= 1
            raise monitor.DockerException("remove failed")


class DummyImages:
    def __init__(self):
        self.calls = []
        self.fail_remove = False

    def remove(self, image):  # pragma: no cover - only used indirectly
        if self.fail_remove:
            raise monitor.DockerException("boom")
        self.calls.append(("remove", image))


class DummyContainer:
    def __init__(
        self,
        name: str,
        healthcheck: bool = False,
        stop_raises: bool = False,
        remove_raises: Exception | None = None,
    ):
        self.name = name
        self.id = f"{name}-id"
        self.labels = {}
        self.attrs = {
            "Config": {
                "Image": "repo:tag",
                "Healthcheck": {"Test": ["CMD-SHELL", "true"]} if healthcheck else None,
            },
            "HostConfig": {},
            "NetworkSettings": {"Networks": {}},
            "State": {"Health": {"Status": "healthy"}},
        }
        self.image = SimpleNamespace(id="old-img")
        self.stopped = False
        self.removed = False
        self.started = False
        self.stop_raises = stop_raises
        self.remove_raises = remove_raises

    def stop(self, timeout=None):
        if self.stop_raises:
            raise monitor.DockerException("stop failed")
        self.stopped = True

    def start(self):
        self.started = True

    def remove(self):
        if self.remove_raises is not None:
            raise self.remove_raises
        self.removed = True


class DummyClient:
    def __init__(self):
        self.api = DummyAPI()
        self.images = DummyImages()
        self.containers = SimpleNamespace(list=lambda all=True: [])


@pytest.fixture
def restart_settings(tmp_path) -> Settings:
    return Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        health_check_timeout_seconds=60,
        prune_timeout_seconds=None,
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
    assert (
        monitor.restart_container(
            client,
            container,
            image_ref="repo:tag",
            new_image_id="new-img",
            settings=restart_settings,
            event_log=event_log,
            notify=True,
        )
        is True
    )
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
    monkeypatch.setattr(
        monitor, "_wait_for_healthy", lambda *args, **kwargs: (False, "unhealthy")
    )

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
    assert any(
        "Rolled back" in entry or "Failed health rollback" in entry
        for entry in event_log
    )


def test_restart_container_network_priority_fallback(
    monkeypatch, restart_settings: Settings
):
    client = DummyClient()
    container = DummyContainer("app")
    # Provide a network with priority to trigger fallback
    container.attrs["NetworkSettings"]["Networks"] = {
        "net": {
            "IPAMConfig": {},
            "GatewayPriority": 1,
            "MacAddress": None,
            "Aliases": None,
            "Links": {"svc": "alias"},
        }
    }

    event_log = []
    assert (
        monitor.restart_container(
            client,
            container,
            image_ref="repo:tag",
            new_image_id="new-img",
            settings=restart_settings,
            event_log=event_log,
            notify=False,
        )
        is True
    )
    # Ensure priority was stripped and links normalized to dict format
    assert "priority" not in client.api.endpoint_kwargs
    assert client.api.endpoint_kwargs.get("links") == {"svc": "alias"}


def test_restart_container_connect_failure(monkeypatch, restart_settings: Settings):
    client = DummyClient()
    container = DummyContainer("app")
    container.attrs["NetworkSettings"]["Networks"] = {
        "net": {
            "IPAMConfig": {},
            "GatewayPriority": None,
            "MacAddress": "aa:bb:cc:dd:ee:ff",
            "Aliases": None,
            "Links": None,
        }
    }
    client.api.fail_connect = True

    event_log = []
    result = monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=False,
    )
    assert result is False


def test_restart_container_connect_success(monkeypatch, restart_settings: Settings):
    client = DummyClient()
    container = DummyContainer("app")
    container.attrs["NetworkSettings"]["Networks"] = {
        "net": {
            "IPAMConfig": {},
            "GatewayPriority": None,
            "MacAddress": "aa:bb:cc:dd:ee:ff",
            "Aliases": None,
            "Links": None,
        }
    }

    event_log = []
    result = monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=False,
    )
    assert result is True
    assert any(call[0] == "connect" for call in client.api.calls)


def test_restart_container_rollback_removes_new_before_restoring_old_on_late_failure(
    restart_settings: Settings,
):
    client = DummyClient()
    container = DummyContainer("app", remove_raises=RuntimeError("boom"))

    event_log: list[str] = []
    result = monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=False,
    )
    assert result is False

    # Validate rollback ordering: free production name first (remove new) then restore old
    calls = client.api.calls
    idx_rename_new_back = next(
        i
        for i, call in enumerate(calls)
        if call[0] == "rename" and call[1] == "new-id" and "-guerite-new-" in call[2]
    )
    idx_remove_new = next(
        i
        for i, call in enumerate(calls)
        if call[0] == "remove" and call[1] == "new-id" and call[2] is True
    )
    idx_restore_old_name = next(
        i
        for i, call in enumerate(calls)
        if call[0] == "rename" and call[1] == container.id and call[2] == "app"
    )
    assert idx_rename_new_back < idx_remove_new < idx_restore_old_name


def test_restart_container_rollback_remove_failure_triggers_rename_away_and_retry(
    restart_settings: Settings,
):
    client = DummyClient()
    client.api.remove_failures_remaining = 1
    container = DummyContainer("app", remove_raises=RuntimeError("boom"))

    event_log: list[str] = []
    result = monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=False,
    )
    assert result is False

    # First rollback attempt to remove fails, then we rename-away and retry remove
    calls = client.api.calls
    assert ("rename", "new-id", "app-guerite-failed-new-id") in calls
    remove_calls = [
        call for call in calls if call[0] == "remove" and call[1] == "new-id"
    ]
    assert len(remove_calls) == 2


def test_restart_container_rollback_starts_old_container_even_if_stop_failed(
    restart_settings: Settings,
):
    client = DummyClient()
    container = DummyContainer(
        "app", stop_raises=True, remove_raises=RuntimeError("boom")
    )

    event_log: list[str] = []
    result = monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=False,
    )
    assert result is False

    # Rollback should attempt to start the original container regardless of stop outcome
    assert ("start", container.id) in client.api.calls


def test_save_health_backoff_errors_are_handled(tmp_path, caplog):
    caplog.set_level("DEBUG")
    monitor._HEALTH_BACKOFF.clear()
    monitor._HEALTH_BACKOFF["cont"] = monitor.now_utc()
    bad_path = tmp_path / "dir" / "state.json"
    # directory does not exist; should log and not raise
    monitor._save_health_backoff(str(bad_path))
    assert any("Failed to persist health backoff" in msg for msg in caplog.messages)


def test_remove_old_image_warns_on_failure(caplog):
    caplog.set_level("WARNING")
    client = DummyClient()
    client.images.fail_remove = True
    event_log: list[str] = []
    monitor.remove_old_image(client, "old-img", "new-img", event_log, notify=True)
    assert any("Failed to remove image" in entry for entry in event_log)
    assert any("Could not remove old image" in msg for msg in caplog.messages)


def test_prune_success_logs(monkeypatch, restart_settings: Settings):
    client = DummyClient()
    client.api.prune_images_called = False

    def fake_prune_images(**kwargs):
        client.api.prune_images_called = True
        return {"SpaceReclaimed": 123, "ImagesDeleted": ["sha256:abc"]}

    client.api.prune_images = fake_prune_images
    client.containers.list = lambda all=True: []
    event_log: list[str] = []
    monitor.prune_images(client, restart_settings, event_log, notify=True)
    assert client.api.prune_images_called is True
    assert any("Pruned images" in entry for entry in event_log)


def test_prune_list_containers_failure(caplog, restart_settings: Settings):
    caplog.set_level("WARNING")
    client = DummyClient()

    def broken_list(all=True):
        raise monitor.DockerException("list failed")

    client.containers.list = broken_list
    event_log: list[str] = []
    monitor.prune_images(client, restart_settings, event_log, notify=True)
    assert any("Skipping prune" in entry for entry in event_log)
    assert any(
        "Skipping prune; could not list containers" in msg for msg in caplog.messages
    )
