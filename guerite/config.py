from dataclasses import dataclass
from os import getenv
from typing import Optional, Set

DEFAULT_UPDATE_LABEL = "guerite.update"
DEFAULT_RESTART_LABEL = "guerite.restart"
DEFAULT_RECREATE_LABEL = "guerite.recreate"
DEFAULT_HEALTH_LABEL = "guerite.health_check"
DEFAULT_HEALTH_BACKOFF_SECONDS = 300
DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS = 60
DEFAULT_NOTIFICATIONS = "update"
DEFAULT_DOCKER_HOST = "unix://var/run/docker.sock"
DEFAULT_PUSHOVER_API = "https://api.pushover.net/1/messages.json"
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
DEFAULT_UPGRADE_STALL_TIMEOUT_SECONDS = 1800
DEFAULT_DOCKER_CONNECT_RETRIES = 5
DEFAULT_DOCKER_CONNECT_BACKOFF_SECONDS = 5
DEFAULT_NOTIFICATION_TIMEOUT_SECONDS = 30
DEFAULT_STOP_TIMEOUT_SECONDS = 120


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
    docker_host: str = DEFAULT_DOCKER_HOST
    update_label: str = DEFAULT_UPDATE_LABEL
    restart_label: str = DEFAULT_RESTART_LABEL
    recreate_label: str = DEFAULT_RECREATE_LABEL
    health_label: str = DEFAULT_HEALTH_LABEL
    health_backoff_seconds: int = DEFAULT_HEALTH_BACKOFF_SECONDS
    health_check_timeout_seconds: int = DEFAULT_HEALTH_CHECK_TIMEOUT_SECONDS
    prune_timeout_seconds: Optional[int] = DEFAULT_PRUNE_TIMEOUT_SECONDS
    notifications: Set[str] = frozenset({DEFAULT_NOTIFICATIONS})
    timezone: str = DEFAULT_TZ
    pushover_token: Optional[str] = None
    pushover_user: Optional[str] = None
    pushover_api: str = DEFAULT_PUSHOVER_API
    webhook_url: Optional[str] = DEFAULT_WEBHOOK_URL
    dry_run: bool = False
    log_level: str = DEFAULT_LOG_LEVEL
    state_file: str = DEFAULT_STATE_FILE
    prune_cron: Optional[str] = DEFAULT_PRUNE_CRON
    rollback_grace_seconds: int = DEFAULT_ROLLBACK_GRACE_SECONDS
    restart_retry_limit: int = DEFAULT_RESTART_RETRY_LIMIT
    depends_label: str = DEFAULT_DEPENDS_LABEL
    action_cooldown_seconds: int = DEFAULT_ACTION_COOLDOWN_SECONDS
    upgrade_stall_timeout_seconds: int = DEFAULT_UPGRADE_STALL_TIMEOUT_SECONDS
    docker_connect_retries: int = DEFAULT_DOCKER_CONNECT_RETRIES
    docker_connect_backoff_seconds: int = DEFAULT_DOCKER_CONNECT_BACKOFF_SECONDS
    stop_timeout_seconds: int = DEFAULT_STOP_TIMEOUT_SECONDS


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
        pushover_api=getenv("GUERITE_PUSHOVER_API", DEFAULT_PUSHOVER_API),
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
        upgrade_stall_timeout_seconds=_env_int(
            "GUERITE_UPGRADE_STALL_TIMEOUT_SECONDS",
            DEFAULT_UPGRADE_STALL_TIMEOUT_SECONDS,
        ),
        docker_connect_retries=_env_int(
            "GUERITE_DOCKER_CONNECT_RETRIES", DEFAULT_DOCKER_CONNECT_RETRIES
        ),
        docker_connect_backoff_seconds=_env_int(
            "GUERITE_DOCKER_CONNECT_BACKOFF_SECONDS",
            DEFAULT_DOCKER_CONNECT_BACKOFF_SECONDS,
        ),
        stop_timeout_seconds=_env_int(
            "GUERITE_STOP_TIMEOUT_SECONDS",
            DEFAULT_STOP_TIMEOUT_SECONDS,
        ),
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
