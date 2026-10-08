from __future__ import annotations

from datetime import datetime, timezone
from threading import Event
from unittest.mock import Mock, call

import pytest
from docker.errors import DockerException
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ConnectTimeout,
    ContentDecodingError,
    InvalidURL,
    ReadTimeout,
)

import guerite.__main__ as main_mod
from guerite import monitor
from guerite.config import Settings


class StopListener(BaseException):
    """Stop deterministic listener tests without being caught as a retry error."""


def _event(action: str, label_key: str | None = None, label_value: str | None = None):
    labels = {label_key: label_value} if label_key else {}
    return {
        "Type": "container",
        "Action": action,
        "id": "abc123",
        "Actor": {"Attributes": labels | {"name": "app"}},
    }


def _stream(*items):
    for item in items:
        if isinstance(item, BaseException):
            raise item
        yield item


@pytest.fixture
def listener(monkeypatch):
    """Capture the actual daemon target; no leaked threads or real retry sleeps."""
    thread = Mock()
    sleep = Mock()
    monkeypatch.setattr(main_mod, "Thread", thread)
    monkeypatch.setattr(main_mod, "sleep", sleep)

    def run(settings, wake, client=None, error=StopListener):
        main_mod.start_event_listener(settings, wake, client)
        thread.return_value.start.assert_called_once_with()
        assert thread.call_args.kwargs["daemon"] is True
        with pytest.raises(error):
            thread.call_args.kwargs["target"]()

    return run, sleep


def test_is_monitored_event_filters(settings: Settings):
    assert main_mod.is_monitored_event(_event("start", settings.update_label, "*"), settings)
    assert not main_mod.is_monitored_event(_event("start", "other", "*"), settings)
    assert not main_mod.is_monitored_event({"Type": "image", "Action": "pull"}, settings)


def test_event_listener_sets_wake_signal(listener, settings):
    run, sleep = listener
    wake = Event()
    client = Mock()
    client.events.return_value = _stream(
        None, {"Type": "image"}, _event("start", settings.update_label, "*"), StopListener(),
    )
    run(settings, wake, client)
    assert wake.is_set()
    sleep.assert_not_called()
    client.close.assert_not_called()


def test_event_listener_respects_cooldown(monkeypatch, listener, settings):
    run, _ = listener
    wake = Event()
    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._LAST_ACTION["app"] = now
    monkeypatch.setattr(main_mod, "now_tz", lambda tz: now)
    client = Mock()
    client.events.return_value = _stream(_event("start", settings.update_label, "*"), StopListener())
    run(settings, wake, client)
    assert not wake.is_set()


TRANSPORT_ERRORS = [DockerException, ConnectionError, ReadTimeout, ConnectTimeout,
                    ChunkedEncodingError, ContentDecodingError]


@pytest.mark.parametrize("error_type", TRANSPORT_ERRORS)
@pytest.mark.parametrize("during_iteration", [False, True])
@pytest.mark.parametrize("provided_client", [False, True])
def test_recovers_from_stream_errors(monkeypatch, listener, settings, error_type,
                                    during_iteration, provided_client):
    run, sleep = listener
    wake = Event()
    failed = Mock()
    recovered = failed if provided_client else Mock()
    failure = error_type("daemon interrupted")
    if during_iteration:
        failure = _stream(failure)
    success = _stream(_event("start", settings.update_label, "*"), StopListener())
    if provided_client:
        failed.events.side_effect = [failure, success]
    else:
        failed.events.side_effect = [failure]
        recovered.events.return_value = success
    factory = Mock(side_effect=[failed, recovered])
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    run(settings, wake, failed if provided_client else None)
    assert wake.is_set()
    sleep.assert_called_once_with(5)
    if provided_client:
        factory.assert_not_called()
        failed.close.assert_not_called()
        assert failed.events.call_count == 2
    else:
        assert factory.call_args_list == [call(base_url=settings.docker_host)] * 2
        failed.close.assert_called_once_with()
        recovered.close.assert_called_once_with()
    recovered.events.assert_called_with(decode=True)


@pytest.mark.parametrize("error_type", TRANSPORT_ERRORS)
def test_retries_client_creation(monkeypatch, listener, settings, error_type):
    run, sleep = listener
    factory = Mock(side_effect=[error_type("offline")] * 6 + [StopListener()])
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    run(settings, Event())
    assert sleep.call_args_list == [call(n) for n in [5, 10, 20, 40, 60, 60]]
    assert factory.call_count == 7


@pytest.mark.parametrize("mode", ["events-call", "iteration", "eof"])
@pytest.mark.parametrize("provided_client", [False, True])
def test_repeated_disconnects_back_off(monkeypatch, listener, settings, mode, provided_client):
    run, sleep = listener
    clients = []
    failures = []
    for _ in range(6):
        failure = ConnectionError("offline")
        if mode == "iteration":
            failure = _stream(failure)
        elif mode == "eof":
            failure = _stream()
        failures.append(failure)
        c = Mock()
        c.events.side_effect = [failure]
        clients.append(c)
    c = Mock()
    c.events.side_effect = StopListener()
    clients.append(c)
    if provided_client:
        c.events.side_effect = failures + [StopListener()]
    factory = Mock(side_effect=clients)
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    run(settings, Event(), c if provided_client else None)
    assert sleep.call_args_list == [call(n) for n in [5, 10, 20, 40, 60, 60]]
    if provided_client:
        factory.assert_not_called()
        c.close.assert_not_called()
    else:
        assert factory.call_count == 7
        for c in clients:
            c.close.assert_called_once_with()


def test_backoff_resets_on_event_not_client_creation(listener, settings):
    run, sleep = listener
    client = Mock()
    client.events.side_effect = [
        ConnectionError("offline"), ConnectionError("offline"),
        _stream({"Type": "image"}, ConnectionError("offline")),
        _stream(_event("start", settings.update_label, "*"), StopListener()),
    ]
    wake = Event()
    run(settings, wake, client)
    assert sleep.call_args_list == [call(5), call(10), call(5)]
    assert wake.is_set()


@pytest.mark.parametrize("cleanup_error", [DockerException, ConnectionError, RuntimeError])
def test_cleanup_failure_does_not_prevent_recovery(monkeypatch, listener, settings, cleanup_error, caplog):
    run, sleep = listener
    stream = Mock()
    stream.__iter__ = Mock(return_value=_stream(ConnectionError("stream failed")))
    stream.close.side_effect = cleanup_error("stream cleanup failed")
    failed = Mock()
    failed.events.return_value = stream
    failed.close.side_effect = cleanup_error("client cleanup failed")
    recovered = Mock()
    recovered.events.return_value = _stream(_event("start", settings.update_label, "*"), StopListener())
    factory = Mock(side_effect=[failed, recovered])
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    wake = Event()
    run(settings, wake)
    assert wake.is_set()
    stream.close.assert_called_once_with()
    failed.close.assert_called_once_with()
    sleep.assert_called_once_with(5)
    assert "Unable to close Docker event stream" in caplog.text
    assert "Unable to close Docker event client" in caplog.text


@pytest.mark.parametrize("error_type", [RuntimeError, ValueError, InvalidURL])
def test_unrelated_errors_are_not_retried(monkeypatch, listener, settings, error_type):
    run, sleep = listener
    client = Mock()
    client.events.side_effect = error_type("not a transport error")
    monkeypatch.setattr(main_mod, "DockerClient", Mock(return_value=client))
    run(settings, Event(), error=error_type)
    sleep.assert_not_called()
    client.close.assert_called_once_with()


def test_real_sdk_refused_private_socket(tmp_path, monkeypatch, listener, settings):
    """Prove exception hierarchy with the SDK, never using the Docker daemon."""
    import socket

    from docker import DockerClient

    path = str(tmp_path / "offline.sock")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.bind(path)  # Bound but not listening: deterministic ECONNREFUSED.
        client = DockerClient(base_url="unix://" + path, version="1.41", timeout=1)
        try:
            with pytest.raises(ConnectionError) as caught:
                client.events(decode=True)
            assert not isinstance(caught.value, DockerException)
            recovered = Mock()
            recovered.events.return_value = _stream(_event("start", settings.update_label, "*"), StopListener())
            factory = Mock(side_effect=[client, recovered])
            monkeypatch.setattr(main_mod, "DockerClient", factory)
            wake = Event()
            run, sleep = listener
            run(settings, wake)
            assert wake.is_set()
            sleep.assert_called_once_with(5)
            assert factory.call_count == 2
        finally:
            client.close()


def test_main_loop_survives_listener_failure_and_wakes(monkeypatch, settings, caplog):
    """Run the real daemon target beside main, with bounded synchronization."""
    from datetime import timedelta
    from threading import Thread

    retry_seen = Event()
    resume_listener = Event()
    listener_finished = Event()
    threads = []
    errors = []

    class CapturedThread(Thread):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            threads.append(self)

        def run(self):
            try:
                super().run()
            except StopListener:
                pass
            except Exception as error:  # noqa: BLE001 - surface thread failures in the test
                errors.append(error)
            finally:
                listener_finished.set()

    def retry_sleep(delay):
        assert delay == 5
        retry_seen.set()
        assert resume_listener.wait(timeout=2)

    failed = Mock()
    failed.events.side_effect = ConnectionError("daemon interrupted")
    recovered = Mock()
    recovered.events.return_value = _stream(_event("start", settings.update_label, "*"), StopListener())
    main_client = Mock()
    monkeypatch.setattr(main_mod, "Thread", CapturedThread)
    monkeypatch.setattr(main_mod, "sleep", retry_sleep)
    monkeypatch.setattr(main_mod, "DockerClient", Mock(side_effect=[failed, recovered]))
    monkeypatch.setattr(main_mod, "load_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", lambda level: None)
    monkeypatch.setattr(main_mod, "build_client_with_retry", lambda cfg: main_client)
    monkeypatch.setattr(main_mod, "select_monitored_containers", lambda *args: [])
    monkeypatch.setattr(main_mod, "schedule_summary", lambda *args, **kwargs: [])
    monkeypatch.setattr(main_mod, "next_prune_time", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_mod, "next_wakeup", lambda *args, reference: (
        reference + timedelta(seconds=60), None, None,
    ))
    checks = []

    def check(client, cfg, **kwargs):
        assert client is main_client
        checks.append(client)
        if len(checks) == 1:
            assert retry_seen.wait(timeout=2)
            resume_listener.set()
            assert listener_finished.wait(timeout=2)
        else:
            raise StopListener()

    monkeypatch.setattr(main_mod, "run_once", check)
    caplog.set_level("INFO", logger=main_mod.__name__)
    try:
        with pytest.raises(StopListener):
            main_mod.main()
    finally:
        resume_listener.set()
        for thread in threads:
            thread.join(timeout=2)
    assert len(checks) == 2
    assert not errors
    assert len(threads) == 1 and threads[0].daemon and not threads[0].is_alive()
    assert "Running checks due to docker event" in caplog.text
    failed.close.assert_called_once_with()
    recovered.close.assert_called_once_with()
    main_client.close.assert_not_called()


@pytest.mark.parametrize("provided_client", [False, True])
def test_eof_closes_stream_and_recovers(monkeypatch, listener, settings, provided_client):
    run, sleep = listener
    ended = Mock()
    ended.__iter__ = Mock(return_value=iter([]))
    failed = Mock()
    recovered = failed if provided_client else Mock()
    success = _stream(_event("start", settings.update_label, "*"), StopListener())
    if provided_client:
        failed.events.side_effect = [ended, success]
    else:
        failed.events.return_value = ended
        recovered.events.return_value = success
    factory = Mock(side_effect=[failed, recovered])
    monkeypatch.setattr(main_mod, "DockerClient", factory)
    wake = Event()
    run(settings, wake, failed if provided_client else None)
    assert wake.is_set()
    ended.close.assert_called_once_with()
    sleep.assert_called_once_with(5)
    if provided_client:
        failed.close.assert_not_called()
        factory.assert_not_called()
    else:
        failed.close.assert_called_once_with()
        assert factory.call_count == 2
