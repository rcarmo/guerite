from datetime import timezone

import pytest

from guerite import utils
from guerite.config import (
    ALL_NOTIFICATION_EVENTS,
    Settings,
    _env_bool,
    _env_csv_set,
    _env_csv_list,
    _env_int,
    load_settings,
)
from guerite import monitor


@pytest.mark.parametrize("env_var,default,expected", [
    ("FLAG_TRUE", False, True),
    ("FLAG_FALSE", True, False),
])
def test_env_bool(monkeypatch, env_var, default, expected):
    monkeypatch.setenv("FLAG_TRUE", "yes")
    monkeypatch.setenv("FLAG_FALSE", "no")
    assert _env_bool(env_var, default) is expected


def test_env_int(monkeypatch):
    monkeypatch.setenv("INT_VAL", "42")
    assert _env_int("INT_VAL", 0) == 42


@pytest.mark.parametrize("env_var,env_value,expected", [
    ("CSV_ALL", "all", set(ALL_NOTIFICATION_EVENTS)),
    ("CSV_LIST", "update,restart", {"update", "restart"}),
])
def test_env_csv_set(monkeypatch, env_var, env_value, expected):
    monkeypatch.setenv(env_var, env_value)
    assert _env_csv_set(env_var, "update") == expected


@pytest.mark.parametrize("env_var,env_value,expected", [
    ("CSV_LIST", "one,two", {"one", "two"}),
    ("CSV_LIST", "one two", {"one", "two"}),
    ("CSV_LIST", "", set()),
])
def test_env_csv_list(monkeypatch, env_var, env_value, expected):
    monkeypatch.setenv(env_var, env_value)
    assert _env_csv_list(env_var, "") == expected


@pytest.mark.parametrize("tz,expect_warning", [
    ("Invalid/Zone", True),
    ("UTC", False),
])
def test_now_tz(caplog, tz, expect_warning):
    caplog.set_level("WARNING")
    dt = utils.now_tz(tz)
    if expect_warning:
        assert dt.tzinfo == timezone.utc
        assert any("invalid timezone" in message for message in caplog.messages)
    else:
        assert dt.tzinfo is not None
        assert dt.utcoffset() == timezone.utc.utcoffset(None)
        assert not caplog.messages


def test_load_settings_prune_timeout_defaults_to_180(monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "unix://test")
    monkeypatch.delenv("GUERITE_PRUNE_TIMEOUT_SECONDS", raising=False)
    settings = load_settings()
    assert settings.prune_timeout_seconds == 180


@pytest.mark.parametrize("env_value,expected", [
    ("300", 300),
    ("not-an-int", 180),
    ("0", 180),
])
def test_load_settings_prune_timeout_values(monkeypatch, env_value, expected):
    monkeypatch.setenv("DOCKER_HOST", "unix://test")
    monkeypatch.setenv("GUERITE_PRUNE_TIMEOUT_SECONDS", env_value)
    settings = load_settings()
    assert settings.prune_timeout_seconds == expected


def test_prune_images_applies_and_restores_timeout(settings: Settings):
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

    prune_settings = Settings(**{**settings.__dict__, "prune_timeout_seconds": 180, "notifications": {"prune"}})
    client = DummyClient()
    event_log: list[str] = []
    monitor.prune_images(client, prune_settings, event_log, notify=False)
    assert client.api.timeout == 60
