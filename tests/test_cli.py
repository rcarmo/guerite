import pytest
from datetime import datetime, timezone

import guerite.__main__ as main_mod
from guerite.config import Settings


class DummyClient:
    def __init__(self):
        self.events_iter = iter([])

    def events(self, decode=True):
        yield from self.events_iter


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
        notifications={"startup"},
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
    )


def test_is_monitored_event_actions(settings: Settings):
    attrs = {settings.update_label: "*"}
    base_event = {"Type": "container", "Actor": {"Attributes": attrs}}
    for action in ["start", "restart", "update", "stop", "die"]:
        event = dict(base_event, Action=action)
        assert main_mod.is_monitored_event(event, settings) is True
    event = dict(base_event, Action="exec")
    assert main_mod.is_monitored_event(event, settings) is False


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


def test_build_client_with_retry_eventual_success(monkeypatch):
    """Test successful connection after retries."""
    settings = Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        health_check_timeout_seconds=60,
        prune_timeout_seconds=None,
        notifications={"startup"},
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
        docker_connect_retries=2,
        docker_connect_backoff_seconds=1,
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
    client = main_mod.build_client_with_retry(settings)
    assert isinstance(client, FakeClient)
    assert attempt["count"] == 2


def test_build_client_with_retry_exhausted(monkeypatch):
    """Test failure after all retries exhausted."""
    settings = Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        health_check_timeout_seconds=60,
        prune_timeout_seconds=None,
        notifications={"startup"},
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
        docker_connect_retries=2,
        docker_connect_backoff_seconds=1,
    )

    def fake_connect(**kwargs):
        raise main_mod.DockerException("connection refused")

    monkeypatch.setattr(main_mod, "DockerClient", fake_connect)
    monkeypatch.setattr(main_mod, "sleep", lambda x: None)
    with pytest.raises(SystemExit) as exc_info:
        main_mod.build_client_with_retry(settings)
    assert "3 attempts" in str(exc_info.value)


def test_format_human_local_today():
    """Test _format_human_local for today."""
    now = datetime(2025, 6, 15, 14, 30, tzinfo=timezone.utc)
    dt = datetime(2025, 6, 15, 16, 45, tzinfo=timezone.utc)
    result = main_mod._format_human_local(dt, now)
    assert result == "today 16:45"


def test_format_human_local_tomorrow():
    """Test _format_human_local for tomorrow."""
    now = datetime(2025, 6, 15, 14, 30, tzinfo=timezone.utc)
    dt = datetime(2025, 6, 16, 8, 0, tzinfo=timezone.utc)
    result = main_mod._format_human_local(dt, now)
    assert result == "tomorrow 08:00"


def test_format_human_local_other_date():
    """Test _format_human_local for other dates."""
    now = datetime(2025, 6, 15, 14, 30, tzinfo=timezone.utc)
    dt = datetime(2025, 6, 20, 10, 15, tzinfo=timezone.utc)
    result = main_mod._format_human_local(dt, now)
    assert result == "2025-06-20 10:15"


def test_short_label_none():
    """Test _short_label with None."""
    assert main_mod._short_label(None) == "unspecified"


def test_short_label_guerite_prefix():
    """Test _short_label strips guerite. prefix."""
    assert main_mod._short_label("guerite.update") == "update"
    assert main_mod._short_label("guerite.restart") == "restart"


def test_short_label_other():
    """Test _short_label with other labels."""
    assert main_mod._short_label("custom.label") == "custom.label"


def test_format_reason():
    """Test _format_reason formatting."""
    assert main_mod._format_reason("myapp", "guerite.update") == "myapp (update)"
    assert main_mod._format_reason(None, None) == "unspecified (unspecified)"


def test_main_loop_runs_single_iteration(monkeypatch, settings: Settings):
    monkeypatch.setattr(main_mod, "load_settings", lambda: settings)
    monkeypatch.setattr(main_mod, "configure_logging", lambda level: None)
    monkeypatch.setattr(main_mod, "build_client_with_retry", lambda cfg: DummyClient())
    monkeypatch.setattr(main_mod, "start_event_listener", lambda client, cfg, signal: None)
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
