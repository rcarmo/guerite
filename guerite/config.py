from dataclasses import dataclass
from os import getenv
from typing import Optional

DEFAULT_MONITOR_LABEL = "guerite.monitor"
DEFAULT_CRON_LABEL = "guerite.cron"
DEFAULT_DOCKER_HOST = "unix://var/run/docker.sock"
DEFAULT_PUSHOOVER_API = "https://api.pushover.net/1/messages.json"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_TZ = "UTC"


@dataclass(frozen=True)
class Settings:
    docker_host: str
    monitor_label: str
    cron_label: str
    timezone: str
    pushover_token: Optional[str]
    pushover_user: Optional[str]
    pushover_api: str
    dry_run: bool
    log_level: str


def load_settings() -> Settings:
    return Settings(
        docker_host=getenv("DOCKER_HOST", DEFAULT_DOCKER_HOST),
        monitor_label=getenv("GUERITE_MONITOR_LABEL", DEFAULT_MONITOR_LABEL),
        cron_label=getenv("GUERITE_CRON_LABEL", DEFAULT_CRON_LABEL),
        timezone=getenv("GUERITE_TZ", DEFAULT_TZ),
        pushover_token=getenv("GUERITE_PUSHOVER_TOKEN"),
        pushover_user=getenv("GUERITE_PUSHOVER_USER"),
        pushover_api=getenv("GUERITE_PUSHOVER_API", DEFAULT_PUSHOOVER_API),
        dry_run=_env_bool("GUERITE_DRY_RUN", False),
        log_level=getenv("GUERITE_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
    )


def _env_bool(name: str, default: bool) -> bool:
    value = getenv(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    return lowered in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default
