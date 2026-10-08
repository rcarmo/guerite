from datetime import datetime, timezone
from logging import Formatter, StreamHandler, basicConfig, getLogger
from time import gmtime
from zoneinfo import ZoneInfo

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LOG = getLogger(__name__)


def configure_logging(level: str) -> None:
    formatter = Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    formatter.converter = gmtime
    handler = StreamHandler()
    handler.setFormatter(formatter)
    basicConfig(handlers=[handler], level=level)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_tz(tz_name: str) -> datetime:
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception as error:
        LOG.warning("Falling back to UTC; invalid timezone %s: %s", tz_name, error)
        return datetime.now(timezone.utc)
