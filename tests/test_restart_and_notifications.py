import pytest

from guerite import monitor
from guerite.config import Settings
from guerite import notifier
from tests.conftest import DummyClient, DummyContainer, FakeConnection


def test_notify_pushover_skips_when_missing(settings: Settings, caplog):
    settings = settings.__class__(**{**settings.__dict__, "pushover_token": None})
    caplog.set_level("DEBUG")
    notifier.notify_pushover(settings, "title", "body")
    assert "Pushover disabled" in caplog.text


def test_notify_pushover_sends_and_warns(monkeypatch, notifier_settings: Settings, caplog):
    fake = FakeConnection(status=500, reason="boom")
    monkeypatch.setattr(notifier, "HTTPSConnection", lambda netloc, timeout=None: fake)
    caplog.set_level("WARNING")
    notifier.notify_pushover(notifier_settings, "title", "body")
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


def test_notify_webhook_sends(monkeypatch, notifier_settings: Settings, caplog):
    fake = FakeConnection(status=200)
    monkeypatch.setattr(notifier, "HTTPSConnection", lambda netloc, timeout=None: fake)
    notifier.notify_webhook(notifier_settings, "title", "body")
    assert fake.calls and fake.calls[-1] == "closed"
    method, path, body, headers = fake.calls[0]
    assert method == "POST"
    assert b"title" in body and b"body" in body
    assert headers["Content-Type"] == "application/json"


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


def test_restart_container_network_priority_fallback(monkeypatch, restart_settings: Settings):
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
    assert monitor.restart_container(
        client,
        container,
        image_ref="repo:tag",
        new_image_id="new-img",
        settings=restart_settings,
        event_log=event_log,
        notify=False,
    ) is True
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
    assert any("Skipping prune; could not list containers" in msg for msg in caplog.messages)
