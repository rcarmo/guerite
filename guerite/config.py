from dataclasses import dataclass
from os import getenv
from socket import gethostname
from typing import Optional, Set

DEFAULT_UPDATE_LABEL = "guerite.update"
DEFAULT_RESTART_LABEL = "guerite.restart"
DEFAULT_RECREATE_LABEL = "guerite.recreate"
DEFAULT_HEALTH_LABEL = "guerite.health_check"
DEFAULT_HEALTH_BACKOFF_SECONDS = 300
DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS = 60
DEFAULT_NOTIFICATIONS = "update"
DEFAULT_DOCKER_HOST = "unix://var/run/docker.sock"
DEFAULT_PUSHOOVER_API = "https://api.pushover.net/1/messages.json"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_TZ = "UTC"
DEFAULT_STATE_FILE = "/tmp/guerite_state.json"
DEFAULT_PRUNE_CRON: str | None = None
DEFAULT_PRUNE_TIMEOUT_SECONDS: int | None = 180
DEFAULT_WEBHOOK_URL: str | None = None
DEFAULT_ROLLBACK_GRACE_SECONDS = 3600
DEFAULT_RESTART_RETRY_LIMIT = 3
DEFAULT_DEPENDS_LABEL = "guerite.depends_on"
DEFAULT_ACTION_COOLDOWN_SECONDS = 60


ALL_NOTIFICATION_EVENTS: Set[str] = {
    "update",
    "restart",
    "recreate",
    "health",
    "health_check",
    "startup",
    "detect",
    "prune",
}


@dataclass(frozen=True)
class Settings:
    docker_host: str
    update_label: str
    restart_label: str
    recreate_label: str
    health_label: str
    health_backoff_seconds: int
    health_check_timeout_seconds: int
    prune_timeout_seconds: Optional[int]
    notifications: Set[str]
    timezone: str
    pushover_token: Optional[str]
    pushover_user: Optional[str]
    pushover_api: str
    webhook_url: Optional[str]
    dry_run: bool
    log_level: str
    state_file: str
    prune_cron: Optional[str]
    rollback_grace_seconds: int
    restart_retry_limit: int
    depends_label: str
    action_cooldown_seconds: int
    hostname: str


def load_settings() -> Settings:
    return Settings(
        docker_host=getenv("DOCKER_HOST", DEFAULT_DOCKER_HOST),
        update_label=getenv("GUERITE_UPDATE_LABEL", DEFAULT_UPDATE_LABEL),
        restart_label=getenv("GUERITE_RESTART_LABEL", DEFAULT_RESTART_LABEL),
        recreate_label=getenv("GUERITE_RECREATE_LABEL", DEFAULT_RECREATE_LABEL),
        health_label=getenv("GUERITE_HEALTH_CHECK_LABEL", DEFAULT_HEALTH_LABEL),
        health_backoff_seconds=_env_int(
            "GUERITE_HEALTH_CHECK_BACKOFF_SECONDS",
            DEFAULT_HEALTH_BACKOFF_SECONDS,
        ),
        health_check_timeout_seconds=_env_int(
            "GUERITE_HEALTH_CHECK_TIMEOUT_SECONDS",
            DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS,
        ),
        prune_timeout_seconds=_env_int_optional(
            "GUERITE_PRUNE_TIMEOUT_SECONDS",
            DEFAULT_PRUNE_TIMEOUT_SECONDS,
        ),
        notifications=_env_csv_set("GUERITE_NOTIFICATIONS", DEFAULT_NOTIFICATIONS),
        timezone=getenv("GUERITE_TZ", DEFAULT_TZ),
        pushover_token=getenv("GUERITE_PUSHOVER_TOKEN"),
        pushover_user=getenv("GUERITE_PUSHOVER_USER"),
        pushover_api=getenv("GUERITE_PUSHOVER_API", DEFAULT_PUSHOOVER_API),
        webhook_url=_env_str("GUERITE_WEBHOOK_URL", DEFAULT_WEBHOOK_URL),
        dry_run=_env_bool("GUERITE_DRY_RUN", False),
        log_level=getenv("GUERITE_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
        state_file=getenv("GUERITE_STATE_FILE", DEFAULT_STATE_FILE),
        prune_cron=_env_str("GUERITE_PRUNE_CRON", DEFAULT_PRUNE_CRON),
        rollback_grace_seconds=_env_int(
            "GUERITE_ROLLBACK_GRACE_SECONDS",
            DEFAULT_ROLLBACK_GRACE_SECONDS,
        ),
        restart_retry_limit=_env_int(
            "GUERITE_RESTART_RETRY_LIMIT",
            DEFAULT_RESTART_RETRY_LIMIT,
        ),
        depends_label=getenv("GUERITE_DEPENDS_LABEL", DEFAULT_DEPENDS_LABEL),
        action_cooldown_seconds=_env_int(
            "GUERITE_ACTION_COOLDOWN_SECONDS",
            DEFAULT_ACTION_COOLDOWN_SECONDS,
        ),
        hostname=getenv("GUERITE_HOSTNAME", gethostname()),
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


def _env_int_optional(name: str, default: Optional[int]) -> Optional[int]:
    value = getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _env_csv_set(name: str, default: str) -> Set[str]:
    raw = getenv(name, default)
    items = raw.split(",") if raw else []
    normalized = {item.strip().lower() for item in items if item.strip()}
    if not normalized:
        return {default}
    if "all" in normalized:
        return set(ALL_NOTIFICATION_EVENTS)
    return normalized


def _env_str(name: str, default: Optional[str]) -> Optional[str]:
    value = getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default
