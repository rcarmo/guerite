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
DEFAULT_MONITOR_ONLY = False
DEFAULT_NO_PULL = False
DEFAULT_NO_RESTART = False
DEFAULT_MONITOR_ONLY_LABEL = "guerite.monitor_only"
DEFAULT_NO_PULL_LABEL = "guerite.no_pull"
DEFAULT_NO_RESTART_LABEL = "guerite.no_restart"
DEFAULT_SCOPE_LABEL = "guerite.scope"
DEFAULT_SCOPE: str | None = None
DEFAULT_INCLUDE_CONTAINERS = ""
DEFAULT_EXCLUDE_CONTAINERS = ""
DEFAULT_ROLLING_RESTART = False
DEFAULT_STOP_TIMEOUT_SECONDS: int | None = None
DEFAULT_LIFECYCLE_HOOKS_ENABLED = False
DEFAULT_HOOK_TIMEOUT_SECONDS = 60
DEFAULT_PRE_CHECK_LABEL = "guerite.lifecycle.pre_check"
DEFAULT_PRE_UPDATE_LABEL = "guerite.lifecycle.pre_update"
DEFAULT_POST_UPDATE_LABEL = "guerite.lifecycle.post_update"
DEFAULT_POST_CHECK_LABEL = "guerite.lifecycle.post_check"
DEFAULT_PRE_UPDATE_TIMEOUT_LABEL = "guerite.lifecycle.pre_update_timeout_seconds"
DEFAULT_POST_UPDATE_TIMEOUT_LABEL = "guerite.lifecycle.post_update_timeout_seconds"
DEFAULT_HTTP_API_ENABLED = False
DEFAULT_HTTP_API_HOST = "0.0.0.0"
DEFAULT_HTTP_API_PORT = 8080
DEFAULT_HTTP_API_TOKEN: str | None = None
DEFAULT_HTTP_API_METRICS = False
DEFAULT_RUN_ONCE = False


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
    monitor_only: bool = DEFAULT_MONITOR_ONLY
    no_pull: bool = DEFAULT_NO_PULL
    no_restart: bool = DEFAULT_NO_RESTART
    monitor_only_label: str = DEFAULT_MONITOR_ONLY_LABEL
    no_pull_label: str = DEFAULT_NO_PULL_LABEL
    no_restart_label: str = DEFAULT_NO_RESTART_LABEL
    scope_label: str = DEFAULT_SCOPE_LABEL
    scope: Optional[str] = DEFAULT_SCOPE
    include_containers: Set[str] = frozenset()
    exclude_containers: Set[str] = frozenset()
    rolling_restart: bool = DEFAULT_ROLLING_RESTART
    stop_timeout_seconds: Optional[int] = DEFAULT_STOP_TIMEOUT_SECONDS
    lifecycle_hooks_enabled: bool = DEFAULT_LIFECYCLE_HOOKS_ENABLED
    hook_timeout_seconds: int = DEFAULT_HOOK_TIMEOUT_SECONDS
    pre_check_label: str = DEFAULT_PRE_CHECK_LABEL
    pre_update_label: str = DEFAULT_PRE_UPDATE_LABEL
    post_update_label: str = DEFAULT_POST_UPDATE_LABEL
    post_check_label: str = DEFAULT_POST_CHECK_LABEL
    pre_update_timeout_label: str = DEFAULT_PRE_UPDATE_TIMEOUT_LABEL
    post_update_timeout_label: str = DEFAULT_POST_UPDATE_TIMEOUT_LABEL
    http_api_enabled: bool = DEFAULT_HTTP_API_ENABLED
    http_api_host: str = DEFAULT_HTTP_API_HOST
    http_api_port: int = DEFAULT_HTTP_API_PORT
    http_api_token: Optional[str] = DEFAULT_HTTP_API_TOKEN
    http_api_metrics: bool = DEFAULT_HTTP_API_METRICS
    run_once: bool = DEFAULT_RUN_ONCE


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
        monitor_only=_env_bool("GUERITE_MONITOR_ONLY", DEFAULT_MONITOR_ONLY),
        no_pull=_env_bool("GUERITE_NO_PULL", DEFAULT_NO_PULL),
        no_restart=_env_bool("GUERITE_NO_RESTART", DEFAULT_NO_RESTART),
        monitor_only_label=getenv("GUERITE_MONITOR_ONLY_LABEL", DEFAULT_MONITOR_ONLY_LABEL),
        no_pull_label=getenv("GUERITE_NO_PULL_LABEL", DEFAULT_NO_PULL_LABEL),
        no_restart_label=getenv("GUERITE_NO_RESTART_LABEL", DEFAULT_NO_RESTART_LABEL),
        scope_label=getenv("GUERITE_SCOPE_LABEL", DEFAULT_SCOPE_LABEL),
        scope=_env_str("GUERITE_SCOPE", DEFAULT_SCOPE),
        include_containers=_env_csv_list(
            "GUERITE_INCLUDE_CONTAINERS", DEFAULT_INCLUDE_CONTAINERS
        ),
        exclude_containers=_env_csv_list(
            "GUERITE_EXCLUDE_CONTAINERS", DEFAULT_EXCLUDE_CONTAINERS
        ),
        rolling_restart=_env_bool("GUERITE_ROLLING_RESTART", DEFAULT_ROLLING_RESTART),
        stop_timeout_seconds=_env_int_optional(
            "GUERITE_STOP_TIMEOUT_SECONDS", DEFAULT_STOP_TIMEOUT_SECONDS
        ),
        lifecycle_hooks_enabled=_env_bool(
            "GUERITE_LIFECYCLE_HOOKS", DEFAULT_LIFECYCLE_HOOKS_ENABLED
        ),
        hook_timeout_seconds=_env_int(
            "GUERITE_HOOK_TIMEOUT_SECONDS", DEFAULT_HOOK_TIMEOUT_SECONDS
        ),
        pre_check_label=getenv("GUERITE_PRE_CHECK_LABEL", DEFAULT_PRE_CHECK_LABEL),
        pre_update_label=getenv("GUERITE_PRE_UPDATE_LABEL", DEFAULT_PRE_UPDATE_LABEL),
        post_update_label=getenv("GUERITE_POST_UPDATE_LABEL", DEFAULT_POST_UPDATE_LABEL),
        post_check_label=getenv("GUERITE_POST_CHECK_LABEL", DEFAULT_POST_CHECK_LABEL),
        pre_update_timeout_label=getenv(
            "GUERITE_PRE_UPDATE_TIMEOUT_LABEL", DEFAULT_PRE_UPDATE_TIMEOUT_LABEL
        ),
        post_update_timeout_label=getenv(
            "GUERITE_POST_UPDATE_TIMEOUT_LABEL", DEFAULT_POST_UPDATE_TIMEOUT_LABEL
        ),
        http_api_enabled=_env_bool("GUERITE_HTTP_API", DEFAULT_HTTP_API_ENABLED),
        http_api_host=getenv("GUERITE_HTTP_API_HOST", DEFAULT_HTTP_API_HOST),
        http_api_port=_env_int("GUERITE_HTTP_API_PORT", DEFAULT_HTTP_API_PORT),
        http_api_token=_env_str("GUERITE_HTTP_API_TOKEN", DEFAULT_HTTP_API_TOKEN),
        http_api_metrics=_env_bool(
            "GUERITE_HTTP_API_METRICS", DEFAULT_HTTP_API_METRICS
        ),
        run_once=_env_bool("GUERITE_RUN_ONCE", DEFAULT_RUN_ONCE),
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
    return parsed if parsed >= 0 else default


def _env_csv_set(name: str, default: str) -> Set[str]:
    raw = getenv(name, default)
    items = raw.split(",") if raw else []
    normalized = {item.strip().lower() for item in items if item.strip()}
    if not normalized:
        return {default} if default else set()
    if "all" in normalized:
        return set(ALL_NOTIFICATION_EVENTS)
    return normalized


def _env_csv_list(name: str, default: str) -> Set[str]:
    raw = getenv(name, default)
    if raw is None or not raw.strip():
        return set()
    items = []
    for chunk in raw.replace(",", " ").split():
        if chunk.strip():
            items.append(chunk.strip())
    return set(items)


def _env_str(name: str, default: Optional[str]) -> Optional[str]:
    value = getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default
