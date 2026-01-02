from datetime import timezone

from guerite import utils
from guerite.config import ALL_NOTIFICATION_EVENTS
from guerite.config import Settings
from guerite.config import _env_bool, _env_csv_set, _env_int
from guerite.config import load_settings
from guerite import monitor


def test_env_helpers(monkeypatch):
    monkeypatch.setenv("FLAG_TRUE", "yes")
    monkeypatch.setenv("FLAG_FALSE", "no")
    monkeypatch.setenv("INT_VAL", "42")
    monkeypatch.setenv("CSV_ALL", "all")
    monkeypatch.setenv("CSV_LIST", "update,restart")

    assert _env_bool("FLAG_TRUE", False) is True
    assert _env_bool("FLAG_FALSE", True) is False
    assert _env_int("INT_VAL", 0) == 42
    assert _env_csv_set("CSV_ALL", "update") == set(ALL_NOTIFICATION_EVENTS)
    assert _env_csv_set("CSV_LIST", "update") == {"update", "restart"}


def test_now_tz_falls_back_to_utc(caplog):
    caplog.set_level("WARNING")
    dt = utils.now_tz("Invalid/Zone")
    assert dt.tzinfo == timezone.utc
    assert any("invalid timezone" in message for message in caplog.messages)

    caplog.clear()
    dt_valid = utils.now_tz("UTC")
    assert dt_valid.tzinfo is not None
    assert dt_valid.utcoffset() == timezone.utc.utcoffset(None)
    assert not caplog.messages


def test_load_settings_prune_timeout_defaults_to_180(monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "unix://test")
    monkeypatch.delenv("GUERITE_PRUNE_TIMEOUT_SECONDS", raising=False)
    settings = load_settings()
    assert settings.prune_timeout_seconds == 180


def test_load_settings_prune_timeout_can_be_overridden(monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "unix://test")
    monkeypatch.setenv("GUERITE_PRUNE_TIMEOUT_SECONDS", "300")
    settings = load_settings()
    assert settings.prune_timeout_seconds == 300


def test_load_settings_prune_timeout_ignores_invalid_values(monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "unix://test")
    monkeypatch.setenv("GUERITE_PRUNE_TIMEOUT_SECONDS", "not-an-int")
    settings = load_settings()
    assert settings.prune_timeout_seconds == 180

    monkeypatch.setenv("GUERITE_PRUNE_TIMEOUT_SECONDS", "0")
    settings = load_settings()
    assert settings.prune_timeout_seconds == 180


def test_prune_images_applies_and_restores_timeout():
    class DummyAPI:
        def __init__(self):
            self.timeout = 60

        def prune_images(self, **kwargs):
            return {"SpaceReclaimed": 0, "ImagesDeleted": []}

    class DummyContainers:
        def list(self, all=True):
            return []

    class DummyClient:
        def __init__(self):
            self.api = DummyAPI()
            self.containers = DummyContainers()

    settings = Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        health_check_timeout_seconds=60,
        prune_timeout_seconds=180,
        notifications={"prune"},
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

    client = DummyClient()
    event_log: list[str] = []
    monitor.prune_images(client, settings, event_log, notify=False)
    assert client.api.timeout == 60
