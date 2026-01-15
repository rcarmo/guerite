import pytest

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
