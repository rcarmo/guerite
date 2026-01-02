from threading import Event
from datetime import datetime, timezone

import pytest

from guerite import monitor
import guerite.__main__ as main_mod
from guerite.__main__ import is_monitored_event, start_event_listener
from guerite.config import Settings


class FakeClient:
    def __init__(self, events_iterable):
        self._events_iterable = events_iterable

    def events(self, decode=True):
        yield from self._events_iterable


@pytest.fixture(autouse=True)
def reset_state():
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
        pushover_token=None,
        pushover_user=None,
        pushover_api="https://example",
        webhook_url=None,
        dry_run=False,
        log_level="INFO",
        state_file="/tmp/state",
        prune_cron=None,
        rollback_grace_seconds=3600,
        restart_retry_limit=3,
        depends_label="guerite.depends_on",
        action_cooldown_seconds=60,
        hostname="testhost",
    )


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
    client = FakeClient(events)

    # Avoid sleeping if loop hits the exception handler
    monkeypatch.setattr(main_mod, "sleep", lambda *_args, **_kwargs: None, raising=False)

    start_event_listener(client, settings, wake)
    assert wake.wait(timeout=1.0) is True


def test_event_listener_respects_cooldown(monkeypatch, settings: Settings):
    wake = Event()
    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._LAST_ACTION["app"] = now

    events = [_event("start", settings.update_label, "*"), KeyboardInterrupt()]
    client = FakeClient(events)

    monkeypatch.setattr(main_mod, "now_tz", lambda tz: now)
    monkeypatch.setattr(main_mod, "sleep", lambda *_args, **_kwargs: None, raising=False)

    start_event_listener(client, settings, wake)
    assert wake.wait(timeout=0.5) is False
