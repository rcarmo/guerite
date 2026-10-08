from collections.abc import Iterator
from threading import Event
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from docker.errors import DockerException
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout

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

    start_event_listener(settings, wake, client)
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

    start_event_listener(settings, wake, client)
    assert wake.wait(timeout=0.5) is False


@pytest.mark.parametrize("error_type", [DockerException, ConnectionError, ReadTimeout, ChunkedEncodingError])
@pytest.mark.parametrize("during_iteration", [False, True])
@pytest.mark.parametrize("provided_client", [False, True])
def test_event_listener_recovers_from_stream_errors(
    monkeypatch, settings: Settings, error_type: type[Exception],
    during_iteration: bool, provided_client: bool,
) -> None:
    wake = Event()
    error = error_type("daemon interrupted")

    def failed_stream() -> Iterator[dict]:
        yield {"Type": "image"}
        raise error

    def recovered_stream() -> Iterator[dict]:
        yield _event("start", settings.update_label, "*")
        raise RuntimeError("stop listener test")

    failure = failed_stream() if during_iteration else error
    failed_client = Mock()
    recovered_client = failed_client if provided_client else Mock()
    if provided_client:
        failed_client.events.side_effect = [failure, recovered_stream()]
    else:
        failed_client.events.side_effect = [failure]
        recovered_client.events.return_value = recovered_stream()
    factory = Mock(side_effect=[failed_client, recovered_client])
    thread = Mock()
    sleep = Mock()
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    monkeypatch.setattr(main_mod, "Thread", thread)
    monkeypatch.setattr(main_mod, "sleep", sleep)

    start_event_listener(settings, wake, failed_client if provided_client else None)
    thread.return_value.start.assert_called_once_with()
    assert thread.call_args.kwargs["daemon"] is True
    with pytest.raises(RuntimeError, match="stop listener test"):
        thread.call_args.kwargs["target"]()

    assert wake.is_set()
    sleep.assert_called_once_with(5)
    if provided_client:
        factory.assert_not_called()
        failed_client.close.assert_not_called()
        assert failed_client.events.call_count == 2
    else:
        assert factory.call_count == 2
        failed_client.close.assert_called_once_with()
        recovered_client.close.assert_not_called()
    recovered_client.events.assert_called_with(decode=True)


def test_event_listener_retries_client_creation(monkeypatch, settings: Settings) -> None:
    factory = Mock(side_effect=[ConnectionError("daemon unavailable")] * 6 + [RuntimeError("stop listener test")])
    thread = Mock()
    sleep = Mock()
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    monkeypatch.setattr(main_mod, "Thread", thread)
    monkeypatch.setattr(main_mod, "sleep", sleep)

    start_event_listener(settings, Event())
    with pytest.raises(RuntimeError, match="stop listener test"):
        thread.call_args.kwargs["target"]()

    assert [call.args[0] for call in sleep.call_args_list] == [5, 10, 20, 40, 60, 60]
    assert factory.call_count == 7
    factory.assert_called_with(base_url=settings.docker_host)
