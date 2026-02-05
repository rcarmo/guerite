from threading import Event
from datetime import datetime, timezone

from guerite import monitor
import guerite.__main__ as main_mod
from guerite.__main__ import is_monitored_event, start_event_listener
from guerite.config import Settings
from tests.conftest import DummyClient


def _event(action: str, label_key: str | None = None, label_value: str | None = None):
    labels = {label_key: label_value} if label_key else {}
    return {
        "Type": "container",
        "Action": action,
        "id": "abc123",
        "Actor": {"Attributes": labels | {"name": "app"}},
    }


def test_is_monitored_event_filters(settings: Settings):
    assert is_monitored_event(_event("start", settings.update_label, "*"), settings) is True
    assert is_monitored_event(_event("start", "other", "*"), settings) is False
    assert is_monitored_event({"Type": "image", "Action": "pull"}, settings) is False


def test_event_listener_sets_wake_signal(monkeypatch, settings: Settings):
    wake = Event()
    events = [_event("start", settings.update_label, "*"), KeyboardInterrupt()]
    client = DummyClient()
    client.events_iter = events

    # Avoid sleeping if loop hits the exception handler
    monkeypatch.setattr(main_mod, "sleep", lambda *_args, **_kwargs: None, raising=False)

    start_event_listener(client, settings, wake)
    assert wake.wait(timeout=1.0) is True


def test_event_listener_respects_cooldown(monkeypatch, settings: Settings):
    wake = Event()
    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._LAST_ACTION["app"] = now

    events = [_event("start", settings.update_label, "*"), KeyboardInterrupt()]
    client = DummyClient()
    client.events_iter = events

    monkeypatch.setattr(main_mod, "now_tz", lambda tz: now)
    monkeypatch.setattr(main_mod, "sleep", lambda *_args, **_kwargs: None, raising=False)

    start_event_listener(client, settings, wake)
    assert wake.wait(timeout=0.5) is False
