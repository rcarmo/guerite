import pytest
from datetime import datetime, timezone
from types import SimpleNamespace

import guerite.__main__ as main_mod
from guerite.config import Settings
from tests.conftest import DummyClient


@pytest.mark.parametrize("action,expected", [
    ("start", True),
    ("restart", True),
    ("update", True),
    ("stop", True),
    ("die", True),
    ("exec", False),
])
def test_is_monitored_event_actions(settings: Settings, action: str, expected: bool):
    attrs = {settings.update_label: "*"}
    event = {"Type": "container", "Action": action, "Actor": {"Attributes": attrs}}
    assert main_mod.is_monitored_event(event, settings) is expected


def test_build_client_failure(monkeypatch, settings: Settings):
    def fake_client(*args, **kwargs):
        raise main_mod.DockerException("boom")
    monkeypatch.setattr(main_mod, "DockerClient", fake_client)
    with pytest.raises(SystemExit):
        main_mod.build_client(settings)


def test_build_client_with_retry_success(monkeypatch, settings: Settings):
    """Test successful connection on first attempt."""
    class FakeClient:
        pass
    monkeypatch.setattr(main_mod, "DockerClient", lambda **kwargs: FakeClient())
    client = main_mod.build_client_with_retry(settings)
    assert isinstance(client, FakeClient)


def test_build_client_with_retry_eventual_success(monkeypatch, settings: Settings):
    """Test successful connection after retries."""
    retry_settings = Settings(
        **{**settings.__dict__, "docker_connect_retries": 2, "docker_connect_backoff_seconds": 1}
    )

    attempt = {"count": 0}

    class FakeClient:
        pass

    def fake_connect(**kwargs):
        attempt["count"] += 1
        if attempt["count"] < 2:
            raise main_mod.DockerException("connection refused")
        return FakeClient()

    monkeypatch.setattr(main_mod, "DockerClient", fake_connect)
    monkeypatch.setattr(main_mod, "sleep", lambda x: None)
    client = main_mod.build_client_with_retry(retry_settings)
    assert isinstance(client, FakeClient)
    assert attempt["count"] == 2


def test_build_client_with_retry_exhausted(monkeypatch, settings: Settings):
    """Test failure after all retries exhausted."""
    retry_settings = Settings(
        **{**settings.__dict__, "docker_connect_retries": 2, "docker_connect_backoff_seconds": 1}
    )

    def fake_connect(**kwargs):
        raise main_mod.DockerException("connection refused")

    monkeypatch.setattr(main_mod, "DockerClient", fake_connect)
    monkeypatch.setattr(main_mod, "sleep", lambda x: None)
    with pytest.raises(SystemExit) as exc_info:
        main_mod.build_client_with_retry(retry_settings)
    assert "3 attempts" in str(exc_info.value)


@pytest.mark.parametrize("dt,expected", [
    (datetime(2025, 6, 15, 16, 45, tzinfo=timezone.utc), "today 16:45"),
    (datetime(2025, 6, 16, 8, 0, tzinfo=timezone.utc), "tomorrow 08:00"),
    (datetime(2025, 6, 20, 10, 15, tzinfo=timezone.utc), "2025-06-20 10:15"),
])
def test_format_human_local(dt: datetime, expected: str):
    """Test _format_human_local for various dates."""
    now = datetime(2025, 6, 15, 14, 30, tzinfo=timezone.utc)
    result = main_mod._format_human_local(dt, now)
    assert result == expected


@pytest.mark.parametrize("label,expected", [
    (None, "unspecified"),
    ("guerite.update", "update"),
    ("guerite.restart", "restart"),
    ("custom.label", "custom.label"),
])
def test_short_label(label, expected):
    """Test _short_label with various inputs."""
    assert main_mod._short_label(label) == expected


@pytest.mark.parametrize("name,label,expected", [
    ("myapp", "guerite.update", "myapp (update)"),
    (None, None, "unspecified (unspecified)"),
])
def test_format_reason(name, label, expected):
    """Test _format_reason formatting."""
    assert main_mod._format_reason(name, label) == expected


def test_main_loop_runs_single_iteration(monkeypatch, settings: Settings):
    monkeypatch.setattr(main_mod, "load_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", lambda level: None)
    monkeypatch.setattr(main_mod, "build_client_with_retry", lambda cfg: DummyClient())
    monkeypatch.setattr(main_mod, "start_event_listener", lambda client, cfg, signal: None)
    monkeypatch.setattr(main_mod, "HttpServer", lambda *args, **kwargs: SimpleNamespace(start=lambda: None))
    monkeypatch.setattr(main_mod, "select_monitored_containers", lambda client, cfg: [])
    monkeypatch.setattr(main_mod, "schedule_summary", lambda containers, cfg, reference: [])
    monkeypatch.setattr(main_mod, "next_prune_time", lambda cfg, reference: None)
    monkeypatch.setattr(main_mod, "next_wakeup", lambda containers, cfg, reference: (main_mod.now_tz(cfg.timezone), None, None))
    monkeypatch.setattr(main_mod, "notify_pushover", lambda *args, **kwargs: None)

    call_count = {"run": 0}

    def fake_run_once(client, cfg, timestamp=None, containers=None):
        call_count["run"] += 1
        raise KeyboardInterrupt

    monkeypatch.setattr(main_mod, "run_once", fake_run_once)

    with pytest.raises(KeyboardInterrupt):
        main_mod.main()
    assert call_count["run"] == 1
