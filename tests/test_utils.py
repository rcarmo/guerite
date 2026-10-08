from collections.abc import Iterator
from datetime import datetime, timezone
from logging import (
    INFO,
    WARNING,
    Formatter,
    Logger,
    LogRecord,
    StreamHandler,
    getLogger,
)
from time import localtime, strftime, tzset

import pytest

from guerite.config import load_settings
from guerite.utils import LOG_DATE_FORMAT, LOG_FORMAT, configure_logging, now_tz


@pytest.fixture
def logging_root(monkeypatch: pytest.MonkeyPatch) -> Iterator[Logger]:
    root = getLogger()
    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(root, "level", root.level)
    try:
        yield root
    finally:
        for handler in root.handlers:
            handler.close()


@pytest.fixture(params=["Europe/Lisbon", "Etc/GMT-5", "UTC"])
def local_timezone(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    try:
        with monkeypatch.context() as context:
            context.setenv("TZ", request.param)
            tzset()
            yield request.param
    finally:
        tzset()


@pytest.mark.parametrize("month", [10, 1])
def test_configure_logging_uses_utc(
    logging_root: Logger, local_timezone: str, month: int
) -> None:
    logging_root.handlers.clear()
    instant = datetime(2026, month, 5, 13, 39, 23, tzinfo=timezone.utc)
    record = LogRecord("guerite", INFO, __file__, 0, "message", (), None)
    record.created = instant.timestamp()

    configure_logging("INFO")

    handler = logging_root.handlers[0]
    rendered = handler.format(record)
    timestamp = rendered.split()[0]
    assert rendered == instant.strftime(LOG_DATE_FORMAT) + " INFO message"
    assert datetime.fromisoformat(timestamp.replace("Z", "+00:00")) == instant
    assert logging_root.level == INFO


def test_configure_logging_preserves_unrelated_formatters(
    logging_root: Logger, local_timezone: str
) -> None:
    logging_root.handlers.clear()
    formatter = Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    record = LogRecord("other", INFO, __file__, 0, "message", (), None)
    record.created = datetime(2026, 10, 5, 13, 39, 23, tzinfo=timezone.utc).timestamp()
    expected = formatter.format(record)

    configure_logging("INFO")

    assert Formatter.converter is localtime
    assert formatter.converter is localtime
    assert formatter.format(record) == expected
    assert expected == strftime(LOG_DATE_FORMAT, localtime(record.created)) + " INFO message"
    assert Formatter(LOG_FORMAT, LOG_DATE_FORMAT).format(record) == expected


def test_configure_logging_preserves_existing_configuration(logging_root: Logger) -> None:
    logging_root.handlers.clear()
    handler = StreamHandler()
    formatter = Formatter("%(message)s")
    handler.setFormatter(formatter)
    logging_root.addHandler(handler)
    logging_root.setLevel(WARNING)

    configure_logging("INFO")

    assert logging_root.handlers == [handler]
    assert handler.formatter is formatter
    assert logging_root.level == WARNING


def test_configure_logging_preserves_scheduling_timezone(
    logging_root: Logger, monkeypatch: pytest.MonkeyPatch
) -> None:
    logging_root.handlers.clear()
    monkeypatch.setenv("GUERITE_TZ", "Europe/Lisbon")
    settings = load_settings()

    configure_logging("INFO")

    assert settings.timezone == "Europe/Lisbon"
    assert now_tz(settings.timezone).tzinfo.key == "Europe/Lisbon"
