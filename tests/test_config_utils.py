import os
from datetime import timezone

from guerite import utils
from guerite.config import ALL_NOTIFICATION_EVENTS, _env_bool, _env_csv_set, _env_int


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
