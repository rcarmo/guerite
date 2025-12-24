from datetime import datetime, timezone

from guerite import monitor
from guerite.config import Settings
from tests.conftest import DummyClient, DummyContainer, DummyImage


def test_run_once_triggers_update_and_restart(monkeypatch, settings: Settings):
    container = DummyContainer(
        "app",
        labels={settings.update_label: "* * * * *"},
    )

    calls: list[tuple] = []

    monkeypatch.setattr(monitor, "select_monitored_containers", lambda client, cfg: [container])
    monkeypatch.setattr(monitor, "pull_image", lambda client, image_ref: DummyImage("new"))
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