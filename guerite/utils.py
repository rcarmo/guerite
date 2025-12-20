from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from logging import basicConfig, getLogger
from typing import Optional

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def configure_logging(level: str) -> None:
    basicConfig(format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT, level=level)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_tz(tz_name: str) -> datetime:
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


def safe_get(mapping: dict, key: str, default: Optional[str] = None) -> Optional[str]:
    if mapping is None:
        return default
    value = mapping.get(key)
    return value if value is not None else default
