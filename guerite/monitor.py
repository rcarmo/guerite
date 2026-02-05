from re import compile as re_compile
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from json import JSONDecodeError
from json import dump
from json import load
from logging import getLogger
from inspect import signature
from os import replace as os_replace
from os.path import exists
from socket import gethostname
from tempfile import NamedTemporaryFile
from threading import Event, Lock, Thread
from typing import Any, Optional
from time import sleep
from dataclasses import dataclass

from time import time as time_time

from croniter import croniter
from docker import DockerClient
from docker.errors import APIError, DockerException
from docker.models.containers import Container
from docker.models.images import Image
from asyncio import new_event_loop, set_event_loop
from aiohttp import web
from requests.exceptions import ReadTimeout, RequestException
try:
    from urllib3.exceptions import ReadTimeoutError
except ImportError:  # pragma: no cover
    ReadTimeoutError = None

from .config import Settings
from .notifier import notify_pushover
from .notifier import notify_webhook
from .utils import now_utc

LOG = getLogger(__name__)

# Global state with thread-safe access via _STATE_LOCK
_STATE_LOCK = Lock()
_PRUNE_LOCK = Lock()  # Lock for Docker API timeout manipulation during prune
_HEALTH_BACKOFF: dict[str, datetime] = {}
_HEALTH_BACKOFF_LOADED = False
_NO_HEALTH_WARNED: set[str] = set()
_PRUNE_CRON_INVALID = False
_KNOWN_CONTAINERS: set[str] = set()
_KNOWN_CONTAINER_NAMES: set[str] = set()
_KNOWN_INITIALIZED = False
_KNOWN_CONTAINERS_LOADED = False
_PENDING_DETECTS: list[str] = []
_LAST_DETECT_NOTIFY: Optional[datetime] = None
_GUERITE_CREATED: set[str] = set()
_RESTART_BACKOFF: dict[str, datetime] = {}
_RESTART_FAIL_COUNT: dict[str, int] = {}
_LAST_ACTION: dict[str, datetime] = {}
_IN_FLIGHT: set[str] = set()
_METRICS: dict[str, int] = {
    "scans_total": 0,
    "scans_skipped": 0,
    "containers_scanned": 0,
    "containers_updated": 0,
    "containers_failed": 0,
}
_METRICS_LOCK = Lock()


def _format_metrics(metrics: dict[str, int]) -> str:
    return "\n".join(
        [
            f"guerite_scans_total {metrics['scans_total']}",
            f"guerite_scans_skipped {metrics['scans_skipped']}",
            f"guerite_containers_scanned {metrics['containers_scanned']}",
            f"guerite_containers_updated {metrics['containers_updated']}",
            f"guerite_containers_failed {metrics['containers_failed']}",
        ]
    ) + "\n"


class HttpServer:
    def __init__(
        self,
        settings: Settings,
        wake_signal: Event,
        trigger_event: Event,
    ) -> None:
        self._settings = settings
        self._wake_signal = wake_signal
        self._trigger_event = trigger_event
        self._loop = new_event_loop()
        self._thread = Thread(target=self._run, daemon=True)
        self._runner: Optional[web.AppRunner] = None

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        set_event_loop(self._loop)
        self._loop.run_until_complete(self._start())
        self._loop.run_forever()

    async def _start(self) -> None:
        app = web.Application()
        app.router.add_post("/v1/update", self._handle_update)
        app.router.add_get("/v1/metrics", self._handle_metrics)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._settings.http_api_host, self._settings.http_api_port)
        await site.start()
        LOG.info("HTTP API listening on %s:%s", self._settings.http_api_host, self._settings.http_api_port)

    def _authorize(self, request: web.Request) -> bool:
        if not self._settings.http_api_token:
            return True
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return False
        token = header.split(" ", 1)[1]
        return token == self._settings.http_api_token

    async def _handle_update(self, request: web.Request) -> web.Response:
        if not self._authorize(request):
            return web.Response(status=401, text="unauthorized")
        self._trigger_event.set()
        self._wake_signal.set()
        return web.Response(status=202, text="scheduled")

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        if not self._settings.http_api_metrics:
            return web.Response(status=404, text="metrics disabled")
        if not self._authorize(request):
            return web.Response(status=401, text="unauthorized")
        metrics = _format_metrics(metrics_snapshot())
        return web.Response(status=200, text=metrics, content_type="text/plain")

    def stop(self) -> None:
        """Gracefully stop the HTTP server."""
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)


def _atomic_write_json(path: str, data: Any) -> None:
    """Write JSON to a file atomically using a temp file and rename."""
    from os.path import dirname
    dir_path = dirname(path) or "."
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=dir_path,
            suffix=".tmp",
            delete=False,
        ) as tmp:
            dump(data, tmp)
            tmp_path = tmp.name
        os_replace(tmp_path, path)
    except OSError as error:
        LOG.debug("Failed to atomically write %s: %s", path, error)


@dataclass
class ContainerRecreateState:
    """Tracks the state of container recreation for proper rollback."""

    old_renamed: bool = False
    new_id: Optional[str] = None
    old_stopped: bool = False
    new_renamed_to_production: bool = False
    original_name: Optional[str] = None
    temp_old_name: Optional[str] = None
    temp_new_name: Optional[str] = None


@dataclass
class UpgradeState:
    """Tracks upgrade state for recovery purposes."""

    original_image_id: Optional[str] = None
    target_image_id: Optional[str] = None
    started_at: Optional[datetime] = None
    status: str = "unknown"  # in-progress, completed, failed
    base_name: Optional[str] = None


def _compose_project(container: Container) -> Optional[str]:
    labels = container.labels or {}
    return labels.get("com.docker.compose.project")


def _base_name(container: Container) -> str:
    name = container.name or "unknown"
    return _strip_guerite_suffix(name)


def _link_targets(container: Container) -> set[str]:
    host_config = container.attrs.get("HostConfig") or {}
    links = host_config.get("Links") or []
    targets: set[str] = set()
    for entry in links:
        if not isinstance(entry, str):
            continue
        target = entry.split(":", 1)[0].lstrip("/")
        if target:
            targets.add(_strip_guerite_suffix(target))
    return targets


def _label_dependencies(container: Container, settings: Settings) -> set[str]:
    labels = container.labels or {}
    raw = labels.get(settings.depends_label)
    if raw is None:
        return set()
    parts = [item.strip() for item in raw.split(",")]
    return {_strip_guerite_suffix(item) for item in parts if item}


def _toposort(names: set[str], deps: dict[str, set[str]]) -> list[str]:
    incoming = {name: set(deps.get(name, set())) & names for name in names}
    result: list[str] = []
    ready = [name for name, incoming_deps in incoming.items() if not incoming_deps]
    while ready:
        current = ready.pop()
        result.append(current)
        for other, incoming_deps in incoming.items():
            if current in incoming_deps:
                incoming_deps.remove(current)
                if not incoming_deps:
                    ready.append(other)
    if len(result) != len(names):
        return sorted(names)
    return result


def _order_by_compose(
    containers: list[Container], settings: Settings
) -> list[Container]:
    grouped: dict[Optional[str], list[Container]] = {}
    for container in containers:
        grouped.setdefault(_compose_project(container), []).append(container)

    ordered: list[Container] = []
    for project, items in grouped.items():
        if len(items) == 1:
            ordered.extend(items)
            continue
        name_map: dict[str, Container] = {}
        deps: dict[str, set[str]] = {}
        for container in items:
            base = _base_name(container)
            name_map[base] = container
            link_deps = _link_targets(container)
            label_deps = _label_dependencies(container, settings)
            deps[base] = link_deps | label_deps
        names = set(name_map.keys())
        for base in list(deps.keys()):
            deps[base] = {dep for dep in deps[base] if dep in names}
        sorted_names = _toposort(names, deps)
        ordered.extend([name_map[name] for name in sorted_names])
    return ordered


def _ensure_health_backoff_loaded(state_file: str) -> None:
    global _HEALTH_BACKOFF_LOADED
    if _HEALTH_BACKOFF_LOADED:
        return
    if not isinstance(state_file, str):
        _HEALTH_BACKOFF_LOADED = True
        return
    try:
        with open(state_file, "r", encoding="utf-8") as handle:
            data = load(handle)
        if isinstance(data, dict):
            for container_id, iso_value in data.items():
                try:
                    _HEALTH_BACKOFF[container_id] = datetime.fromisoformat(iso_value)
                except (ValueError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    except (OSError, JSONDecodeError) as error:
        LOG.debug("Failed to load health backoff state: %s", error)
    _HEALTH_BACKOFF_LOADED = True


def _save_health_backoff(state_file: str) -> None:
    serializable = {
        container_id: value.isoformat()
        for container_id, value in _HEALTH_BACKOFF.items()
    }
    _atomic_write_json(state_file, serializable)


def _ensure_known_containers_loaded(state_file: str) -> None:
    """Load known containers from disk on startup to avoid spurious detect notifications."""
    global _KNOWN_CONTAINERS_LOADED, _KNOWN_INITIALIZED
    if _KNOWN_CONTAINERS_LOADED:
        return
    if not isinstance(state_file, str):
        _KNOWN_CONTAINERS_LOADED = True
        return

    known_state_file = state_file.replace(".json", "_known.json")
    try:
        with open(known_state_file, "r", encoding="utf-8") as handle:
            data = load(handle)
        if isinstance(data, dict):
            ids = data.get("container_ids", [])
            names = data.get("container_names", [])
            if isinstance(ids, list):
                _KNOWN_CONTAINERS.update(ids)
            if isinstance(names, list):
                _KNOWN_CONTAINER_NAMES.update(names)
            # Mark as initialized since we restored from disk
            if _KNOWN_CONTAINERS or _KNOWN_CONTAINER_NAMES:
                _KNOWN_INITIALIZED = True
    except FileNotFoundError:
        pass
    except (OSError, JSONDecodeError) as error:
        LOG.debug("Failed to load known containers state: %s", error)
    _KNOWN_CONTAINERS_LOADED = True


def _save_known_containers(state_file: str) -> None:
    """Save known containers to disk for crash recovery."""
    if not isinstance(state_file, str):
        return
    known_state_file = state_file.replace(".json", "_known.json")
    with _STATE_LOCK:
        serializable = {
            "container_ids": list(_KNOWN_CONTAINERS),
            "container_names": list(_KNOWN_CONTAINER_NAMES),
        }
    _atomic_write_json(known_state_file, serializable)


def select_monitored_containers(
    client: DockerClient, settings: Settings
) -> list[Container]:
    labels = [
        settings.update_label,
        settings.restart_label,
        settings.recreate_label,
        settings.health_label,
    ]
    seen: dict[str, Container] = {}
    for label in labels:
        if label is None:
            continue
        try:
            for container in client.containers.list(filters={"label": label}):
                try:
                    labels_dict = container.labels or {}
                except DockerException:
                    continue
                if label not in labels_dict:
                    continue
                seen[container.id] = container
        except Exception as error:
            LOG.error("Failed to list containers with label %s: %s", label, error)
    filtered = list(seen.values())
    if settings.scope is not None:
        filtered = [
            container
            for container in filtered
            if (container.labels or {}).get(settings.scope_label) == settings.scope
        ]
    if settings.include_containers:
        include = {item for item in settings.include_containers if item}
        filtered = [container for container in filtered if container.name in include]
    if settings.exclude_containers:
        exclude = {item for item in settings.exclude_containers if item}
        filtered = [container for container in filtered if container.name not in exclude]
    return filtered


def pull_image(client: DockerClient, image_ref: str) -> Optional[Image]:
    try:
        return client.images.pull(image_ref)
    except (DockerException, ReadTimeout, RequestException) as error:
        LOG.error("Failed to pull image %s: %s", image_ref, error)
        return None
    except Exception as error:
        # Handle urllib3 ReadTimeoutError which may not be importable
        if ReadTimeoutError is not None and isinstance(error, ReadTimeoutError):
            LOG.error("Failed to pull image %s: %s", image_ref, error)
            return None
        # Log and continue for any other unexpected error to avoid crashing
        LOG.error("Unexpected error pulling image %s: %s", image_ref, error)
        return None


def _supports_is_upgrade(func: Any) -> bool:
    try:
        params = signature(func).parameters
        return "is_upgrade" in params
    except Exception:
        return False


def needs_update(container: Container, pulled_image: Image) -> bool:
    try:
        return container.image.id != pulled_image.id
    except DockerException as error:
        LOG.warning("Could not compare images for %s: %s", container.name, error)
        return False


def current_image_id(container: Container) -> Optional[str]:
    try:
        return container.image.id
    except DockerException as error:
        LOG.warning("Could not read image ID for %s: %s", container.name, error)
        return None


def get_image_reference(container: Container) -> Optional[str]:
    """Get the best image reference for recreating a container.

    Prefers image tags over hashes so docker ps shows readable names.
    """
    try:
        # Try to get a tag from the image object
        if container.image.tags:
            return container.image.tags[0]
    except (DockerException, AttributeError):
        pass

    # Fall back to Config.Image, but only if it's not a hash
    config_image = container.attrs.get("Config", {}).get("Image")
    if config_image and not config_image.startswith("sha256:"):
        return config_image

    return None


def _cron_matches(container: Container, label_key: str, timestamp: datetime) -> bool:
    cron_expression = container.labels.get(label_key)
    if cron_expression is None:
        LOG.debug("%s has no %s; skipping", container.name, label_key)
        return False
    try:
        allowed = croniter.match(cron_expression, timestamp)
        LOG.debug(
            "%s %s %s at %s -> %s",
            container.name,
            label_key,
            cron_expression,
            timestamp.isoformat(),
            allowed,
        )
        return allowed
    except (ValueError, KeyError) as error:
        LOG.warning(
            "Invalid cron expression on %s (%s): %s", container.name, label_key, error
        )
        return False


def _is_unhealthy(container: Container) -> bool:
    try:
        status = container.attrs.get("State", {}).get("Health", {}).get("Status")
    except DockerException as error:
        LOG.warning("Could not read health status for %s: %s", container.name, error)
        return False
    if status is None:
        return False
    lowered = status.lower()
    if lowered in {"healthy", "starting"}:
        return False
    LOG.debug("%s health status %s", container.name, lowered)
    return True


def _started_recently(container: Container, now: datetime, grace_seconds: int) -> bool:
    try:
        started_at = container.attrs.get("State", {}).get("StartedAt")
    except DockerException as error:
        LOG.debug("Could not read StartedAt for %s: %s", container.name, error)
        return False
    if not started_at:
        return False
    try:
        # Handle timestamps like 2025-01-01T00:00:00.000000000Z
        sanitized = started_at.rstrip("Z")
        started_dt = datetime.fromisoformat(sanitized)
        if started_dt.tzinfo is None:
            started_dt = started_dt.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return False
    return (now - started_dt).total_seconds() < grace_seconds


def _has_healthcheck(container: Container) -> bool:
    try:
        health_cfg = container.attrs.get("Config", {}).get("Healthcheck")
    except DockerException as error:
        LOG.warning(
            "Could not read health configuration for %s: %s", container.name, error
        )
        return False
    return bool(health_cfg)


def _is_swarm_managed(container: Container) -> bool:
    return "com.docker.swarm.service.id" in container.labels


def _preflight_mounts(
    name: str, mounts: list[dict], notify: bool, event_log: list[str]
) -> None:
    for mount in mounts:
        mount_type = mount.get("Type")
        if mount_type == "bind":
            source = mount.get("Source")
            if source and not exists(source):
                LOG.debug(
                    "Cannot validate bind source %s for %s; path not visible here; recreate may fail",
                    source,
                    name,
                )
        elif mount_type == "volume":
            driver = mount.get("Driver")
            if driver and driver != "local":
                LOG.warning(
                    "Volume %s uses driver %s for %s; ensure driver is available",
                    mount.get("Name"),
                    driver,
                    name,
                )
                if notify:
                    event_log.append(
                        f"Volume driver {driver} for {name} at {mount.get('Destination')}"
                    )


def _health_allowed(
    container_id: str, base_name: str, now: datetime, settings: Settings
) -> bool:
    with _STATE_LOCK:
        next_time = _HEALTH_BACKOFF.get(container_id)
    if next_time is None:
        return True
    if now >= next_time:
        return True
    remaining = (next_time - now).total_seconds()
    LOG.debug(
        "Skipping unhealthy restart for %s (%s); backoff %.0fs remaining",
        base_name,
        _short_id(container_id),
        remaining,
    )
    return False


def _restart_allowed(
    container_id: str, base_name: str, now: datetime, settings: Settings
) -> bool:
    with _STATE_LOCK:
        next_time = _RESTART_BACKOFF.get(container_id)
    if next_time is None:
        return True
    if now >= next_time:
        return True
    remaining = (next_time - now).total_seconds()
    LOG.debug(
        "Skipping restart for %s (%s); recreate backoff %.0fs remaining",
        base_name,
        _short_id(container_id),
        remaining,
    )
    return False


def _notify_restart_backoff(
    container_name: str,
    container_id: str,
    backoff_until: datetime,
    event_log: list[str],
    settings: Settings,
) -> None:
    key = f"{container_id}-backoff-notified"
    if key in _HEALTH_BACKOFF:
        return
    event_log.append(
        f"Recreate for {container_name} deferred until {backoff_until.isoformat()} after repeated failures"
    )
    _HEALTH_BACKOFF[key] = backoff_until


def _filter_rollback_containers(containers: list[Container]) -> list[Container]:
    rollback: list[Container] = []
    for container in containers:
        name = container.name or ""
        if "-guerite-old-" in name or "-guerite-new-" in name:
            rollback.append(container)
    return rollback


def _rollback_protected_images(rollback_containers: list[Container]) -> set[str]:
    protected: set[str] = set()
    for container in rollback_containers:
        image_id = current_image_id(container)
        if image_id:
            protected.add(image_id)
    return protected


def _wait_for_healthy(
    client: DockerClient, container_id: str, timeout_seconds: int
) -> tuple[bool, Optional[str]]:
    deadline = now_utc() + timedelta(seconds=timeout_seconds)
    last_status: Optional[str] = None
    while now_utc() < deadline:
        try:
            attrs = client.api.inspect_container(container_id)
            health = attrs.get("State", {}).get("Health", {})
            status = health.get("Status")
            last_status = status
            if status is None:
                return True, None
            lowered = status.lower()
            if lowered == "healthy":
                return True, lowered
            if lowered == "starting":
                sleep(2)
                continue
        except DockerException as error:
            LOG.debug(
                "Health inspect failed for %s: %s", _short_id(container_id), error
            )
        sleep(2)
    return False, last_status


def _register_restart_failure(
    container_id: str,
    original_name: str,
    notify: bool,
    event_log: list[str],
    settings: Settings,
    error: Exception,
) -> None:
    with _STATE_LOCK:
        fail_count = _RESTART_FAIL_COUNT.get(container_id, 0) + 1
        _RESTART_FAIL_COUNT[container_id] = fail_count
    backoff_seconds = min(settings.health_backoff_seconds * max(1, fail_count), 3600)
    if fail_count >= settings.restart_retry_limit:
        backoff_seconds = max(
            backoff_seconds,
            settings.health_backoff_seconds * settings.restart_retry_limit,
        )
        LOG.info(
            "Reached restart retry limit for %s (%s failures); deferring",
            original_name,
            fail_count,
        )
    backoff_until = now_utc() + timedelta(seconds=backoff_seconds)
    with _STATE_LOCK:
        _RESTART_BACKOFF[container_id] = backoff_until
    if notify:
        event_log.append(f"Failed to restart {original_name}: {error}")
        _notify_restart_backoff(
            original_name, container_id, backoff_until, event_log, settings
        )


def _cleanup_stale_rollbacks(
    client: DockerClient,
    all_containers: list[Container],
    rollback_containers: list[Container],
    settings: Settings,
    event_log: list[str],
    notify: bool,
) -> list[Container]:
    if not rollback_containers:
        return rollback_containers
    base_names = {container.name for container in all_containers if container.name}
    now = now_utc()
    remaining: list[Container] = []
    for container in rollback_containers:
        name = container.name or "unknown"
        try:
            state = container.attrs.get("State", {})
            running = bool(state.get("Running"))
        except DockerException as error:
            LOG.debug("Could not read state for %s: %s", name, error)
            remaining.append(container)
            continue
        if running:
            remaining.append(container)
            continue
        created_raw = container.attrs.get("Created")
        created_at: Optional[datetime] = None
        if isinstance(created_raw, str):
            try:
                created_at = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            except ValueError:
                created_at = None
        age = (now - created_at).total_seconds() if created_at else None
        base_name = _strip_guerite_suffix(name)
        if base_name not in base_names:
            LOG.info(
                "Keeping rollback container %s; base %s not present", name, base_name
            )
            remaining.append(container)
            continue
        if age is None or age < settings.rollback_grace_seconds:
            remaining.append(container)
            continue
        try:
            container.remove(force=True)
            if notify:
                event_log.append(f"Removed stale rollback container {name}")
            LOG.info(
                "Removed stale rollback container %s after %.0fs",
                name,
                age if age is not None else 0,
            )
        except DockerException as error:
            LOG.debug("Failed to remove rollback container %s: %s", name, error)
            remaining.append(container)
    return remaining


def _should_notify(settings: Settings, event: str) -> bool:
    try:
        notifications = getattr(settings, "notifications", None)
        if notifications is None:
            return False
        return event in notifications
    except Exception:
        return False


def _clean_cron_expression(value: Optional[str]) -> Optional[str]:
    if value is None or not isinstance(value, str):
        return None
    cleaned = value.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].strip()
    if (cleaned.startswith('"') and cleaned.endswith('"')) or (
        cleaned.startswith("'") and cleaned.endswith("'")
    ):
        cleaned = cleaned[1:-1].strip()
    return cleaned or None


def _prune_due(settings: Settings, timestamp: datetime) -> bool:
    global _PRUNE_CRON_INVALID
    cron_expression = _clean_cron_expression(settings.prune_cron)
    if not cron_expression:
        return False
    if _PRUNE_CRON_INVALID:
        return False
    try:
        return croniter.match(cron_expression, timestamp)
    except (ValueError, KeyError) as error:
        LOG.warning("Invalid prune cron expression %s: %s", cron_expression, error)
        _PRUNE_CRON_INVALID = True
        return False


def next_prune_time(settings: Settings, reference: datetime) -> Optional[datetime]:
    global _PRUNE_CRON_INVALID
    cron_expression = _clean_cron_expression(settings.prune_cron)
    if not cron_expression or _PRUNE_CRON_INVALID:
        return None
    try:
        iterator = croniter(cron_expression, reference, ret_type=datetime)
        return iterator.get_next(datetime)
    except (ValueError, KeyError) as error:
        LOG.warning("Invalid prune cron expression %s: %s", cron_expression, error)
        _PRUNE_CRON_INVALID = True
        return None


def _track_new_containers(containers: list[Container]) -> None:
    global _KNOWN_INITIALIZED
    with _STATE_LOCK:
        if not _KNOWN_INITIALIZED:
            for container in containers:
                _KNOWN_CONTAINERS.add(container.id)
                _KNOWN_CONTAINER_NAMES.add(container.name)
            _KNOWN_INITIALIZED = True
            return
        for container in containers:
            if container.id not in _KNOWN_CONTAINERS:
                _KNOWN_CONTAINERS.add(container.id)
                # Only notify if it's a truly new container name (not just a restart)
                if container.name not in _KNOWN_CONTAINER_NAMES:
                    _KNOWN_CONTAINER_NAMES.add(container.name)
                    # Don't notify about containers we created
                    if container.id not in _GUERITE_CREATED:
                        _PENDING_DETECTS.append(container.name)
                    else:
                        _GUERITE_CREATED.discard(container.id)
                else:
                    # Existing container restarted externally - just update tracking
                    if container.id in _GUERITE_CREATED:
                        _GUERITE_CREATED.discard(container.id)


def _short_id(identifier: Optional[str]) -> str:
    if identifier is None:
        return "unknown"
    return identifier.split(":")[-1][:12]


def _image_display_name(
    container: Optional[Container] = None,
    image_ref: Optional[str] = None,
    image_id: Optional[str] = None,
) -> str:
    """Get a human-readable image name, preferring tags over IDs."""
    if image_ref:
        return image_ref
    if container is not None:
        try:
            config_image = container.attrs.get("Config", {}).get("Image")
            if config_image and not config_image.startswith("sha256:"):
                return config_image
            if container.image.tags:
                return container.image.tags[0]
        except (DockerException, AttributeError):
            pass
    if image_id:
        return _short_id(image_id)
    return "unknown"


def _label_bool(labels: dict, key: str) -> Optional[bool]:
    if not key:
        return None
    raw = labels.get(key)
    if raw is None:
        return None
    lowered = str(raw).strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return None


def _effective_setting(labels: dict, key: str, default: bool) -> bool:
    override = _label_bool(labels, key)
    return override if override is not None else default


def _resolve_container_modes(container: Container, settings: Settings) -> dict[str, bool]:
    labels = container.labels or {}
    monitor_only = _effective_setting(labels, settings.monitor_only_label, settings.monitor_only)
    no_pull = _effective_setting(labels, settings.no_pull_label, settings.no_pull)
    no_restart = _effective_setting(labels, settings.no_restart_label, settings.no_restart)
    if monitor_only:
        no_restart = True
    return {"monitor_only": monitor_only, "no_pull": no_pull, "no_restart": no_restart}


def _normalize_links_value(raw_links: Optional[Any]) -> Optional[dict[str, str]]:
    if raw_links in (None, False):
        return None
    if isinstance(raw_links, dict):
        return raw_links
    if isinstance(raw_links, (list, tuple)):
        # Convert list of "container:alias" strings to dict
        result = {}
        for link in raw_links:
            if isinstance(link, str) and ":" in link:
                parts = link.split(":", 1)
                result[parts[0]] = parts[1]
            elif isinstance(link, str):
                result[link] = link
        return result if result else None
    # Unknown shape; skip to avoid docker SDK unpack errors
    return None


def _resolve_hook_timeout(
    container: Container, label_key: str, default_timeout: int
) -> int:
    labels = container.labels or {}
    raw = labels.get(label_key)
    if raw is None:
        return default_timeout
    try:
        parsed = int(str(raw).strip())
    except (ValueError, TypeError):
        return default_timeout
    return parsed if parsed >= 0 else default_timeout


def _run_lifecycle_hook(
    client: DockerClient,
    container: Container,
    hook: str,
    timeout_seconds: int,
    event_log: list[str],
    hook_name: str,
) -> None:
    if not hook:
        return
    container_id = container.id
    if not container_id:
        LOG.warning("Skipping %s hook; missing container id for %s", hook_name, container.name)
        return
    try:
        exec_info = client.api.exec_create(container_id, cmd=["sh", "-c", hook])
        exec_id = exec_info.get("Id")
        if exec_id is None:
            raise DockerException("exec_create returned no Id")
        client.api.exec_start(
            exec_id,
            detach=False,
            tty=False,
            stream=False,
            timeout=timeout_seconds,
        )
        result = client.api.exec_inspect(exec_id)
        exit_code = result.get("ExitCode")
        if exit_code not in (0, 75):
            LOG.warning(
                "%s hook failed for %s with exit code %s", hook_name, container.name, exit_code
            )
            event_log.append(f"{hook_name} hook failed for {container.name} (exit {exit_code})")
    except (DockerException, APIError) as error:
        LOG.warning("%s hook failed for %s: %s", hook_name, container.name, error)
        event_log.append(f"{hook_name} hook failed for {container.name}: {error}")


def _action_allowed(base_name: str, now: datetime, settings: Settings) -> bool:
    with _STATE_LOCK:
        if base_name in _IN_FLIGHT:
            LOG.debug("Skipping %s; action in-flight", base_name)
            return False
        last = _LAST_ACTION.get(base_name)
    if last is None:
        return True
    if (now - last).total_seconds() >= settings.action_cooldown_seconds:
        return True
    remaining = settings.action_cooldown_seconds - (now - last).total_seconds()
    LOG.debug("Skipping %s; action cooldown %.0fs remaining", base_name, remaining)
    return False


def _mark_action(base_name: str, when: datetime) -> None:
    with _STATE_LOCK:
        _LAST_ACTION[base_name] = when
        _IN_FLIGHT.add(base_name)


def _clear_in_flight(base_name: str) -> None:
    with _STATE_LOCK:
        _IN_FLIGHT.discard(base_name)


def _strip_guerite_suffix(name: str) -> str:
    pattern = re_compile(r"^(.*)-guerite-(?:old|new)-[0-9a-f]{8}$")
    current = name
    while True:
        match = pattern.match(current)
        if match is None:
            return current
        current = match.group(1)


def _metric_increment(name: str, amount: int = 1) -> None:
    with _METRICS_LOCK:
        _METRICS[name] = _METRICS.get(name, 0) + amount


def metrics_snapshot() -> dict[str, int]:
    with _METRICS_LOCK:
        return dict(_METRICS)


def _parse_recovery_info_from_name(name: str) -> Optional[dict[str, Any]]:
    """Parse recovery info from a container name.

    Supports formats:
    - <base>-guerite-(old|new)-<suffix>
    - <base>-guerite-(old|new)-<suffix>-<timestamp>-<fail_count>
    where suffix is any non-empty string without dashes (commonly 8-hex chars),
    timestamp and fail_count are integers.
    """

    # Extended format with timestamp and fail_count
    extended_pattern = re_compile(
        r"^(?P<base>.+)-guerite-(?P<kind>old|new)-(?P<suffix>[^-]+)-(?P<ts>\d+)-(?P<count>\d+)$"
    )
    match = extended_pattern.match(name)
    if match:
        try:
            return {
                "base_name": match.group("base"),
                "recovery_type": match.group("kind"),
                "suffix": match.group("suffix"),
                "timestamp": int(match.group("ts")),
                "fail_count": int(match.group("count")),
            }
        except ValueError:
            return None

    # Simple format without timestamp/fail_count
    simple_pattern = re_compile(
        r"^(?P<base>.+)-guerite-(?P<kind>old|new)-(?P<suffix>[^-]+)$"
    )
    match = simple_pattern.match(name)
    if match:
        return {
            "base_name": match.group("base"),
            "recovery_type": match.group("kind"),
            "suffix": match.group("suffix"),
            "timestamp": None,
            "fail_count": None,
        }

    return None


def _generate_recovery_name(
    base_name: str,
    recovery_type: str,
    suffix: str,
    fail_count: int,
    timestamp: Optional[int] = None,
) -> str:
    """Generate a recovery name compatible with parser."""
    ts = int(timestamp if timestamp is not None else time_time())
    return f"{base_name}-guerite-{recovery_type}-{suffix}-{ts}-{fail_count}"


def _add_upgrade_labels(
    client: DockerClient, container: Container, upgrade_state: UpgradeState
) -> bool:
    """Add upgrade tracking labels to a container."""
    if not container.id:
        return False
    try:
        labels = container.labels.copy() if container.labels else {}

        # Add upgrade tracking labels
        labels["guerite.upgrade.status"] = upgrade_state.status
        if upgrade_state.original_image_id:
            labels["guerite.upgrade.original-image"] = upgrade_state.original_image_id
        if upgrade_state.target_image_id:
            labels["guerite.upgrade.target-image"] = upgrade_state.target_image_id
        if upgrade_state.started_at:
            labels["guerite.upgrade.started"] = upgrade_state.started_at.isoformat()

        # Use the correct Docker API method to update labels
        client.api.inspect_container(container.id)  # Verify container exists
        # Note: Docker Python SDK doesn't directly support label updates,
        # so we'll track this in our state management instead
        LOG.debug("Upgrade labels prepared for container %s", _short_id(container.id))
        return True
    except (APIError, DockerException) as error:
        LOG.warning(
            "Failed to prepare upgrade labels for %s: %s",
            _short_id(container.id),
            error,
        )
        return False


# Global upgrade state tracking since Docker doesn't support direct label updates
_UPGRADE_STATE: dict[str, UpgradeState] = {}
_UPGRADE_STATE_LOADED = False
_UPGRADE_STATE_FILE: Optional[str] = None
_UPGRADE_STATE_NOTIFIED: set[str] = set()


def _track_upgrade_state(
    container_id: str,
    upgrade_state: UpgradeState,
    persist: bool = True,
    state_file: Optional[str] = None,
) -> None:
    """Track upgrade state and optionally persist to disk.

    If state_file is provided, it is used immediately and cached as _UPGRADE_STATE_FILE.
    """
    if not container_id:
        return
    _UPGRADE_STATE[container_id] = upgrade_state
    # Cache provided state_file for future saves
    global _UPGRADE_STATE_FILE
    if state_file:
        _UPGRADE_STATE_FILE = state_file
    if persist:
        target = _UPGRADE_STATE_FILE or state_file
        if target:
            _save_upgrade_state(target)


def _get_tracked_upgrade_state(container_id: str) -> Optional[UpgradeState]:
    """Get tracked upgrade state for a container."""
    return _UPGRADE_STATE.get(container_id)


def _clear_tracked_upgrade_state(container_id: str) -> None:
    """Clear tracked upgrade state for a container."""
    _UPGRADE_STATE.pop(container_id, None)


def _ensure_upgrade_state_loaded(state_file: str) -> None:
    """Load upgrade state from disk on startup."""
    global _UPGRADE_STATE_LOADED, _UPGRADE_STATE_FILE
    if _UPGRADE_STATE_LOADED:
        return

    if not isinstance(state_file, str):
        _UPGRADE_STATE_LOADED = True
        return

    _UPGRADE_STATE_FILE = state_file

    # Use same state file as health backoff with upgrade data prefix
    upgrade_state_file = state_file.replace(".json", "_upgrade.json")

    try:
        with open(upgrade_state_file, "r", encoding="utf-8") as handle:
            data = load(handle)
        if isinstance(data, dict):
            for container_id, state_data in data.items():
                try:
                    if isinstance(state_data, dict):
                        upgrade_state = UpgradeState(
                            original_image_id=state_data.get("original_image_id"),
                            target_image_id=state_data.get("target_image_id"),
                            base_name=state_data.get("base_name"),
                            status=state_data.get("status", "unknown"),
                            started_at=datetime.fromisoformat(state_data["started_at"])
                            if state_data.get("started_at")
                            else None,
                        )
                        _UPGRADE_STATE[container_id] = upgrade_state
                except (ValueError, TypeError) as e:
                    LOG.debug(
                        "Failed to parse upgrade state for %s: %s", container_id, e
                    )
                    continue
    except FileNotFoundError:
        pass
    except (OSError, JSONDecodeError) as error:
        LOG.debug("Failed to load upgrade state: %s", error)

    _UPGRADE_STATE_LOADED = True


def _save_upgrade_state(state_file: str) -> None:
    """Save upgrade state to disk for crash recovery."""
    if not isinstance(state_file, str):
        return
    # Use same state file as health backoff with upgrade data prefix
    upgrade_state_file = state_file.replace(".json", "_upgrade.json")

    serializable = {}
    for container_id, upgrade_state in _UPGRADE_STATE.items():
        state_dict = {"status": upgrade_state.status}
        if upgrade_state.original_image_id:
            state_dict["original_image_id"] = upgrade_state.original_image_id
        if upgrade_state.target_image_id:
            state_dict["target_image_id"] = upgrade_state.target_image_id
        if upgrade_state.base_name:
            state_dict["base_name"] = upgrade_state.base_name
        if upgrade_state.started_at:
            state_dict["started_at"] = upgrade_state.started_at.isoformat()

        serializable[container_id] = state_dict

    _atomic_write_json(upgrade_state_file, serializable)
    LOG.debug("Saved upgrade state for %d containers", len(_UPGRADE_STATE))


def _clear_upgrade_labels(client: DockerClient, container_id: str) -> bool:
    """Clear all upgrade-related labels from a container."""
    try:
        container = client.containers.get(container_id)
        labels = container.labels.copy() if container.labels else {}

        # Remove upgrade labels
        upgrade_labels = [
            key for key in labels.keys() if key.startswith("guerite.upgrade.")
        ]
        for label in upgrade_labels:
            labels.pop(label, None)

        client.api.update_container(container_id, labels=labels)
        LOG.debug("Cleared upgrade labels from container %s", _short_id(container_id))
        return True
    except (APIError, DockerException) as error:
        LOG.warning(
            "Failed to clear upgrade labels from %s: %s", _short_id(container_id), error
        )
        return False


def _get_upgrade_state(container: Container) -> Optional[UpgradeState]:
    """Extract upgrade state from container labels."""
    if not container.labels:
        return None

    status = container.labels.get("guerite.upgrade.status")
    if not status:
        return None

    upgrade_state = UpgradeState(status=status)

    original_image = container.labels.get("guerite.upgrade.original-image")
    if original_image:
        upgrade_state.original_image_id = original_image

    target_image = container.labels.get("guerite.upgrade.target-image")
    if target_image:
        upgrade_state.target_image_id = target_image

    started_str = container.labels.get("guerite.upgrade.started")
    if started_str:
        try:
            upgrade_state.started_at = datetime.fromisoformat(
                started_str.replace("Z", "+00:00")
            )
        except ValueError:
            pass

    return upgrade_state


def _find_containers_with_upgrade_status(
    client: DockerClient, status: str
) -> list[Container]:
    """Find containers with a specific upgrade status."""
    try:
        containers = client.containers.list(
            all=True, filters={"label": f"guerite.upgrade.status={status}"}
        )
        return containers
    except DockerException as error:
        LOG.error("Failed to find containers with upgrade status %s: %s", status, error)
        return []


def _recover_stalled_upgrades(
    client: DockerClient, settings: Settings, event_log: list[str], notify: bool
) -> None:
    """Recover from stalled upgrades by checking tracked upgrade state."""
    if not _UPGRADE_STATE:
        return

    now = now_utc()
    stall_threshold = getattr(settings, "upgrade_stall_timeout_seconds", 1800)

    stalled_containers = []
    for container_id, upgrade_state in list(_UPGRADE_STATE.items()):
        if upgrade_state.status != "in-progress":
            continue
        if not upgrade_state.started_at:
            continue

        age_seconds = (now - upgrade_state.started_at).total_seconds()
        if age_seconds > stall_threshold:
            stalled_containers.append((container_id, upgrade_state, age_seconds))

    for container_id, upgrade_state, age_seconds in stalled_containers:
        try:
            container = client.containers.get(container_id)
            base_name = _strip_guerite_suffix(container.name or "unknown")

            LOG.warning(
                "Detected stalled upgrade for %s (in-progress for %.0fs)",
                base_name,
                age_seconds,
            )

            if notify:
                event_log.append(
                    f"Detected stalled upgrade for {base_name}; marking as failed"
                )

            # Mark as failed
            upgrade_state.status = "failed"
            _track_upgrade_state(
                container_id,
                upgrade_state,
                state_file=getattr(settings, "state_file", None),
            )

            # Log for manual intervention
            LOG.error(
                "Upgrade stalled for %s - manual intervention may be required. "
                "Original image: %s, Target image: %s",
                base_name,
                upgrade_state.original_image_id,
                upgrade_state.target_image_id,
            )

        except Exception as error:
            LOG.warning(
                "Failed to check stalled upgrade container %s: %s",
                _short_id(container_id),
                error,
            )
            # If container cannot be inspected, clear state to avoid repeat noise
            _clear_tracked_upgrade_state(container_id)


def _reconcile_failed_upgrades(
    client: DockerClient,
    base_map: dict[str, Container],
    event_log: list[str],
    notify: bool,
    state_file: Optional[str] = None,
) -> None:
    """Clear failed upgrade state if a container was updated externally.

    Detects manual upgrades by checking if the container's current image differs
    from the original image that failed to upgrade. If the image changed, we assume
    someone manually resolved the issue and clear the failed state.
    """
    if not _UPGRADE_STATE:
        return
    for container_id, upgrade_state in list(_UPGRADE_STATE.items()):
        if upgrade_state.status != "failed":
            continue
        container = None
        try:
            container = client.containers.get(container_id)
        except DockerException:
            pass
        # If container ID changed (recreated), try to find by base name
        if container is None and upgrade_state.base_name:
            container = base_map.get(upgrade_state.base_name)
        if container is None:
            continue
        # Backfill base_name if missing (for upgrades tracked before this field existed)
        if upgrade_state.base_name is None:
            upgrade_state.base_name = _strip_guerite_suffix(container.name or "")
            _track_upgrade_state(container_id, upgrade_state, state_file=state_file)
        current_id = current_image_id(container)
        if current_id is None:
            continue
        original_id = upgrade_state.original_image_id
        target_id = upgrade_state.target_image_id
        # Still on original image - no manual upgrade occurred
        if original_id and current_id == original_id:
            continue
        # No original recorded but current doesn't match target - ambiguous, skip
        if not original_id and target_id and current_id != target_id:
            continue
        resolved_name = _strip_guerite_suffix(
            container.name or upgrade_state.base_name or _short_id(container_id)
        )
        LOG.info(
            "Detected manual upgrade for %s; clearing failed upgrade state",
            resolved_name,
        )
        if notify:
            event_log.append(
                f"Detected manual upgrade for {resolved_name}; clearing failed upgrade state"
            )
        _clear_tracked_upgrade_state(container_id)
        _UPGRADE_STATE_NOTIFIED.discard(container_id)
        # Clear backoff for both original tracked ID and current container ID
        with _STATE_LOCK:
            for cid in {container_id, container.id}:
                if cid:
                    _RESTART_BACKOFF.pop(cid, None)
                    _RESTART_FAIL_COUNT.pop(cid, None)


def _check_for_manual_intervention(
    client: DockerClient, settings: Settings, event_log: list[str], notify: bool
) -> None:
    """Notify about failed upgrades that may require manual intervention.

    To avoid repeated notifications, track notified containers in _UPGRADE_STATE_NOTIFIED.
    """
    if not _UPGRADE_STATE:
        return

    for container_id, upgrade_state in list(_UPGRADE_STATE.items()):
        if upgrade_state.status != "failed":
            continue
        if container_id in _UPGRADE_STATE_NOTIFIED:
            continue

        base_name = upgrade_state.base_name or _short_id(container_id)
        try:
            container = client.containers.get(container_id)
            base_name = _strip_guerite_suffix(container.name or base_name)
        except DockerException:
            # If container is gone, we still notify once and then clear state
            pass

        message = (
            f"Upgrade failed for {base_name}; manual intervention may be required. "
            f"Original: {upgrade_state.original_image_id or 'unknown'}, "
            f"Target: {upgrade_state.target_image_id or 'unknown'}"
        )
        LOG.warning(message)
        if notify:
            event_log.append(message)

        _UPGRADE_STATE_NOTIFIED.add(container_id)
        # Clear state for non-existent containers to reduce noise
        try:
            client.containers.get(container_id)
        except DockerException:
            _clear_tracked_upgrade_state(container_id)


def _rollback_container_recreation(
    client: DockerClient, state: ContainerRecreateState, container: Container
) -> bool:
    """Rollback container recreation by cleaning up new container and restoring old one.

    CRITICAL: Always remove new container FIRST to free up the name before renaming old container.
    This prevents name conflicts if new container was renamed to production name before failure.
    """
    rollback_success = True

    try:
        # Step 1: Remove new container if it was created
        # MUST happen first to free up the production name if new container took it
        # force=True will stop it if running and disconnect from all networks automatically
        if state.new_id:
            # If new container was renamed to production name, rename it back to temp first
            if state.new_renamed_to_production and state.temp_new_name:
                try:
                    client.api.rename(state.new_id, state.temp_new_name)
                    LOG.debug(
                        "Renamed new container back to temp name %s for cleanup",
                        state.temp_new_name,
                    )
                except DockerException as e:
                    LOG.warning(
                        "Could not rename new container back to temp name: %s", e
                    )

            try:
                client.api.remove_container(state.new_id, force=True)
                LOG.debug(
                    "Removed new container %s during rollback", _short_id(state.new_id)
                )
            except DockerException as e:
                # If removal fails, try renaming it away to free up any name it might have
                LOG.warning(
                    "Failed to remove new container %s during rollback: %s",
                    _short_id(state.new_id),
                    e,
                )

                if state.original_name:
                    try:
                        # Use a unique failed name to avoid conflicts
                        temp_remove_name = (
                            f"{state.original_name}-guerite-failed-{state.new_id[:8]}"
                        )
                        client.api.rename(state.new_id, temp_remove_name)
                        LOG.debug("Renamed stuck new container to %s", temp_remove_name)
                        # Try removal again
                        try:
                            client.api.remove_container(state.new_id, force=True)
                            LOG.debug("Successfully removed new container after rename")
                        except DockerException as e2:
                            LOG.error(
                                "Could not remove new container even after rename: %s",
                                e2,
                            )
                            rollback_success = False
                    except DockerException as e2:
                        LOG.error("Could not rename new container away: %s", e2)
                        rollback_success = False
                else:
                    rollback_success = False

        # Step 2: Restore old container if it was renamed
        if state.old_renamed and state.original_name and container.id:
            try:
                # Rename back to original name
                client.api.rename(container.id, state.original_name)
                LOG.debug("Restored old container name to %s", state.original_name)

                # Always start the container (idempotent if already running)
                # This ensures the container is running regardless of state tracking accuracy
                try:
                    client.api.start(container.id)
                    LOG.debug("Started old container %s", state.original_name)
                except DockerException as e:
                    # Start failure might be due to container already running or dead state
                    LOG.warning(
                        "Could not start old container %s after rollback: %s",
                        state.original_name,
                        e,
                    )
                    # Don't mark rollback as failed - rename succeeded which is most important

            except DockerException as e:
                LOG.error(
                    "Critical rollback failed - could not restore %s: %s",
                    state.original_name,
                    e,
                )
                rollback_success = False

        return rollback_success

    except Exception as e:
        LOG.error("Rollback operation failed with unexpected error: %s", e)
        return False


def _attach_to_networks_safely(
    client: DockerClient, new_id: str, networking: dict, original_name: str
) -> bool:
    """Attach container to networks with proper error handling and rollback."""
    attached_networks = []

    try:
        for network_name, network_cfg in networking.items():
            mac_address = network_cfg.get("MacAddress")
            ipam_cfg = network_cfg.get("IPAMConfig") or {}
            links = _normalize_links_value(network_cfg.get("Links"))

            try:
                client.api.connect_container_to_network(
                    new_id,
                    network_name,
                    aliases=network_cfg.get("Aliases"),
                    links=links,
                    ipv4_address=ipam_cfg.get("IPv4Address"),
                    ipv6_address=ipam_cfg.get("IPv6Address"),
                    link_local_ips=ipam_cfg.get("LinkLocalIPs"),
                    driver_opt=network_cfg.get("DriverOpts"),
                    mac_address=mac_address,
                )
                attached_networks.append(network_name)
                LOG.debug("Attached %s to network %s", original_name, network_name)

            except APIError as e:
                LOG.error(
                    "Failed to attach %s to network %s: %s",
                    original_name,
                    network_name,
                    e,
                )
                # Rollback already attached networks
                for net in attached_networks:
                    try:
                        client.api.disconnect_container_from_network(new_id, net)
                    except DockerException:
                        pass
                return False

        return True

    except Exception as e:
        LOG.error("Unexpected error during network attachment: %s", e)
        # Cleanup any partial attachments
        for net in attached_networks:
            try:
                client.api.disconnect_container_from_network(new_id, net)
            except DockerException:
                pass
        return False


def _build_create_kwargs(
    container: Container, image_ref: str, temp_name: str, client: DockerClient
) -> dict:
    """Build container creation kwargs from existing container."""
    attrs = getattr(container, "attrs", {}) or {}
    if not isinstance(attrs, dict):
        attrs = {}
    config = attrs.get("Config", {}) if isinstance(attrs, dict) else {}
    host_config = attrs.get("HostConfig") if isinstance(attrs, dict) else None
    networking = None
    if isinstance(attrs, dict):
        networking = attrs.get("NetworkSettings", {})
        networking = networking.get("Networks") if isinstance(networking, dict) else None

    # Build network endpoint config
    endpoint_map = {}
    if networking:
        for network_name, network_cfg in networking.items():
            ipam_cfg = network_cfg.get("IPAMConfig") or {}
            links = _normalize_links_value(network_cfg.get("Links"))
            endpoint_kwargs = {
                "aliases": network_cfg.get("Aliases"),
                "links": links,
                "ipv4_address": ipam_cfg.get("IPv4Address"),
                "ipv6_address": ipam_cfg.get("IPv6Address"),
                "link_local_ips": ipam_cfg.get("LinkLocalIPs"),
                "driver_opt": network_cfg.get("DriverOpts"),
                "mac_address": network_cfg.get("MacAddress"),
            }
            endpoint_kwargs = {
                key: value
                for key, value in endpoint_kwargs.items()
                if value is not None
            }
            endpoint_map[network_name] = client.api.create_endpoint_config(
                **endpoint_kwargs
            )

    # Build create kwargs
    create_kwargs = {
        "command": config.get("Cmd"),
        "domainname": config.get("Domainname"),
        "entrypoint": config.get("Entrypoint"),
        "environment": config.get("Env"),
        "healthcheck": config.get("Healthcheck"),
        "host_config": host_config,
        "hostname": config.get("Hostname"),
        "image": image_ref,
        "labels": config.get("Labels"),
        "mac_address": config.get("MacAddress"),
        "name": temp_name,
        "network_disabled": config.get("NetworkDisabled"),
        "ports": _extract_ports(config),
        "runtime": host_config.get("Runtime")
        if isinstance(host_config, dict)
        else None,
        "shell": config.get("Shell"),
        "stdin_open": config.get("OpenStdin"),
        "stop_signal": config.get("StopSignal"),
        "stop_timeout": config.get("StopTimeout"),
        "tty": config.get("Tty"),
        "user": config.get("User"),
        "volumes": config.get("Volumes"),
        "working_dir": config.get("WorkingDir"),
    }

    if endpoint_map:
        create_kwargs["networking_config"] = client.api.create_networking_config(
            endpoint_map
        )

    return {key: value for key, value in create_kwargs.items() if value is not None}


def _extract_ports(config: dict) -> Optional[list]:
    """Extract port list from container config."""
    exposed_ports = config.get("ExposedPorts")
    return list(exposed_ports.keys()) if isinstance(exposed_ports, dict) else None


def restart_container(
    client: DockerClient,
    container: Container,
    image_ref: str,
    new_image_id: Optional[str],
    settings: Settings,
    event_log: list[str],
    notify: bool,
    is_upgrade: bool = False,
    pre_update_hook: Optional[str] = None,
    post_update_hook: Optional[str] = None,
    pre_update_timeout: Optional[int] = None,
    post_update_timeout: Optional[int] = None,
) -> bool:
    """Enhanced container recreation with comprehensive fallback."""
    state = ContainerRecreateState()

    # Extract container information safely
    name = container.name
    if name is None:
        LOG.error("Container name is None, cannot proceed with recreation")
        return False

    if not container.id:
        LOG.error("Container ID is missing, cannot proceed with recreation")
        return False

    base_name = _strip_guerite_suffix(name)
    state.original_name = base_name
    short_suffix = container.id[:8]  # Already validated container.id exists
    state.temp_old_name = f"{base_name}-guerite-old-{short_suffix}"
    state.temp_new_name = f"{base_name}-guerite-new-{short_suffix}"

    # Setup upgrade state tracking if this is an upgrade
    upgrade_state = None
    if is_upgrade and new_image_id:
        old_image_id = current_image_id(container)
        upgrade_state = UpgradeState(
            original_image_id=old_image_id,
            target_image_id=new_image_id,
            base_name=base_name,
            started_at=now_utc(),
            status="in-progress",
        )
        _track_upgrade_state(container.id, upgrade_state, state_file=settings.state_file)
        LOG.info("Started upgrade tracking for %s", base_name)

    # Preflight checks
    attrs = getattr(container, "attrs", {}) or {}
    if not isinstance(attrs, dict):
        attrs = {}
    config = attrs.get("Config", {}) if isinstance(attrs, dict) else {}
    mounts = attrs.get("Mounts") or []
    if not isinstance(mounts, list):
        mounts = []
    networking = None
    if isinstance(attrs, dict):
        net = attrs.get("NetworkSettings", {})
        networking = net.get("Networks") if isinstance(net, dict) else None

    _preflight_mounts(name, mounts, notify, event_log)

    try:
        if pre_update_hook:
            _run_lifecycle_hook(
                client,
                container,
                pre_update_hook,
                pre_update_timeout or settings.hook_timeout_seconds,
                event_log,
                "pre-update",
            )

        # Step 1: Rename old container
        client.api.rename(container.id, state.temp_old_name)
        state.old_renamed = True
        LOG.debug("Renamed old container to %s", state.temp_old_name)

        # Step 2: Create new container
        create_kwargs = _build_create_kwargs(
            container, image_ref, state.temp_new_name, client
        )
        created = client.api.create_container(**create_kwargs)
        state.new_id = created.get("Id")

        if state.new_id is None:
            raise DockerException("create_container returned no Id")

        LOG.debug("Created new container %s", state.new_id)

        # Step 3: Stop old container
        LOG.info("Stopping %s", state.original_name)
        if notify:
            # Use old container's image hash for stopping report
            old_image_hash = None
            try:
                old_image_hash = container.image.id
            except (DockerException, AttributeError):
                pass
            event_log.append(
                f"Stopping container {state.original_name} ({_image_display_name(image_id=old_image_hash)})"
            )

        try:
            if settings.stop_timeout_seconds is not None:
                container.stop(timeout=settings.stop_timeout_seconds)
            else:
                container.stop()
            state.old_stopped = True
        except DockerException as e:
            LOG.warning(
                "Failed to stop old container %s: %s (continuing anyway)",
                state.original_name,
                e,
            )
            # Continue even if stop fails - Docker may still be able to work with it

        # Step 4: Attach to networks with MAC addresses
        # Containers are created with networking_config, but networks with MAC addresses
        # need to be re-attached to apply the MAC address correctly
        if networking:
            networks_with_mac = {
                name: cfg for name, cfg in networking.items() if cfg.get("MacAddress")
            }
            if networks_with_mac:
                success = _attach_to_networks_safely(
                    client, state.new_id, networks_with_mac, state.original_name
                )
                if not success:
                    raise DockerException(
                        "Failed to attach to networks with MAC addresses"
                    )

        # Step 5: Start new container, then verify health before giving it production name
        # Start FIRST so if it fails, we can safely remove container with temp name
        client.api.start(state.new_id)
        LOG.debug("Started new container %s", state.temp_new_name)

        # Step 6: Health check verification BEFORE final rename
        # This way if health check fails, container still has temp name and rollback is simpler
        if config.get("Healthcheck"):
            # Use a dedicated timeout for health check (separate from backoff delay)
            health_check_timeout = settings.health_check_timeout_seconds
            healthy, status = _wait_for_healthy(
                client, state.new_id, health_check_timeout
            )
            if not healthy:
                LOG.warning(
                    "New container %s did not become healthy (status=%s) after %ds; rolling back",
                    state.temp_new_name,
                    status,
                    health_check_timeout,
                )
                raise RuntimeError(
                    f"new container unhealthy after recreate (status={status})"
                )

        # Step 7: Now that it's running and healthy, give it the production name
        client.api.rename(state.new_id, state.original_name)
        state.new_renamed_to_production = True
        with _STATE_LOCK:
            _GUERITE_CREATED.add(state.new_id)

        LOG.info("Started new container %s", state.original_name)
        if notify:
            event_log.append(
                f"Created container {state.original_name} ({_image_display_name(image_ref=image_ref)})"
            )

        # Step 8: Cleanup old container
        try:
            container.remove()
            LOG.debug("Removed old container %s", state.temp_old_name)
        except DockerException as e:
            LOG.warning(
                "Could not remove old container %s: %s (will be cleaned up by stale rollback cleanup)",
                state.temp_old_name,
                e,
            )

        # Step 9: Reset failure counters on success
        if container.id:
            with _STATE_LOCK:
                _RESTART_FAIL_COUNT.pop(container.id, None)
                _RESTART_BACKOFF.pop(container.id, None)

        if post_update_hook:
            _run_lifecycle_hook(
                client,
                container,
                post_update_hook,
                post_update_timeout or settings.hook_timeout_seconds,
                event_log,
                "post-update",
            )

        # Step 10: Complete upgrade tracking if this was an upgrade
        if is_upgrade and upgrade_state and container.id:
            upgrade_state.status = "completed"
            _track_upgrade_state(
                container.id, upgrade_state, state_file=settings.state_file
            )
            LOG.info("Upgrade completed successfully for %s", state.original_name)

        return True

    except (APIError, DockerException, RuntimeError, TypeError, Exception) as error:
        LOG.error(
            "Failed to restart %s during recreate: %s", state.original_name, error
        )

        # Comprehensive rollback
        rollback_success = _rollback_container_recreation(client, state, container)

        if rollback_success:
            if notify:
                rollback_detail = (
                    "new container never became healthy"
                    if isinstance(error, RuntimeError)
                    and "unhealthy after recreate" in str(error)
                    else f"recreate failed: {error}"
                )
                event_log.append(
                    f"Rolled back {state.original_name}; {rollback_detail}"
                )
        else:
            # Provide specific details about what state the containers are in
            state_detail = f"old={state.temp_old_name if state.old_renamed else 'original'}, new={_short_id(state.new_id) if state.new_id else 'none'}"
            LOG.critical(
                "Rollback failed for %s (%s) - manual intervention may be required",
                state.original_name,
                state_detail,
            )
            if notify:
                event_log.append(
                    f"CRITICAL: Rollback failed for {state.original_name} ({state_detail}) - check container state manually"
                )

        # Register failure for backoff
        if container.id:
            _register_restart_failure(
                container.id, state.original_name, notify, event_log, settings, error
            )

        # Mark upgrade as failed if this was an upgrade
        if is_upgrade and upgrade_state and container.id:
            upgrade_state.status = "failed"
            _track_upgrade_state(
                container.id, upgrade_state, state_file=settings.state_file
            )
            LOG.error("Upgrade failed for %s: %s", state.original_name, error)

        return False


def remove_old_image(
    client: DockerClient,
    old_image_id: Optional[str],
    new_image_id: str,
    event_log: list[str],
    notify: bool,
) -> None:
    if old_image_id is None or old_image_id == new_image_id:
        return
    try:
        client.images.remove(image=old_image_id)
        LOG.info("Removed old image %s", _short_id(old_image_id))
        if notify:
            event_log.append(f"Removing image ({_short_id(old_image_id)})")
    except (APIError, DockerException) as error:
        LOG.warning("Could not remove old image %s: %s", _short_id(old_image_id), error)
        if notify:
            event_log.append(
                f"Failed to remove image ({_short_id(old_image_id)}): {error}"
            )


def prune_images(
    client: DockerClient,
    settings: Settings,
    event_log: list[str],
    notify: bool,
) -> None:
    try:
        all_containers = client.containers.list(all=True)
    except DockerException as error:
        LOG.warning("Skipping prune; could not list containers: %s", error)
        if notify:
            event_log.append(f"Skipping prune; could not list containers: {error}")
        return

    rollback_containers = _filter_rollback_containers(all_containers)
    rollback_containers = _cleanup_stale_rollbacks(
        client,
        all_containers,
        rollback_containers,
        settings,
        event_log,
        notify,
    )

    protected = _rollback_protected_images(rollback_containers)
    if protected:
        summary = ", ".join(sorted(_short_id(img) for img in protected))
        LOG.info("Skipping prune; rollback images protected: %s", summary)
        if notify:
            event_log.append(
                "Skipping prune while rollback containers exist; protected images: "
                + summary
            )
        return
    try:
        with _PRUNE_LOCK:
            had_timeout_attr = hasattr(client.api, "timeout")
            previous_timeout: Any = getattr(client.api, "timeout", None)

            if (
                settings.prune_timeout_seconds is not None
                and settings.prune_timeout_seconds > 0
            ):
                client.api.timeout = settings.prune_timeout_seconds
            elif isinstance(previous_timeout, (int, float)) and previous_timeout > 0:
                client.api.timeout = previous_timeout * 3
            else:
                client.api.timeout = 180

            try:
                result = client.api.prune_images(filters={"dangling": False})
            finally:
                if had_timeout_attr:
                    client.api.timeout = previous_timeout
                else:
                    if hasattr(client.api, "timeout"):
                        delattr(client.api, "timeout")
        reclaimed = result.get("SpaceReclaimed") if isinstance(result, dict) else None
        images_deleted = (
            result.get("ImagesDeleted") if isinstance(result, dict) else None
        )
        LOG.info(
            "Pruned images; reclaimed %s bytes; deleted %s entries",
            reclaimed,
            len(images_deleted or []),
        )
        if notify:
            summary = "Pruned images" + (
                f"; reclaimed {reclaimed} bytes" if reclaimed is not None else ""
            )
            event_log.append(summary)
            if images_deleted:
                # Docker returns items like {"Deleted": "sha256:..."} or {"Untagged": "repo:tag"}
                pruned_labels: list[str] = []
                for entry in images_deleted:
                    if isinstance(entry, dict):
                        label = entry.get("Untagged") or entry.get("Deleted")
                        if label:
                            pruned_labels.append(label)
                    elif isinstance(entry, str):
                        pruned_labels.append(entry)
                if pruned_labels:
                    max_list = 5
                    shown = pruned_labels[:max_list]
                    more = len(pruned_labels) - len(shown)
                    body = "Pruned entries:\n" + "\n".join(shown)
                    if more > 0:
                        body += f"\n(+{more} more)"
                    event_log.append(body)
    except (APIError, DockerException) as error:
        LOG.warning("Image prune failed: %s", error)
        if notify:
            event_log.append(f"Image prune failed: {error}")
    except (ReadTimeout, RequestException) as error:
        LOG.warning("Image prune timed out or connection failed: %s", error)
        if notify:
            event_log.append(f"Image prune timed out: {error}")
    except Exception as error:
        if ReadTimeoutError is not None and isinstance(error, ReadTimeoutError):
            LOG.warning("Image prune timed out: %s", error)
            if notify:
                event_log.append(f"Image prune timed out: {error}")
        else:
            raise


def _flush_detect_notifications(
    settings: Settings, hostname: str, current_time: datetime
) -> None:
    global _PENDING_DETECTS, _LAST_DETECT_NOTIFY
    if not _PENDING_DETECTS:
        return
    if not _should_notify(settings, "detect"):
        return
    if (
        _LAST_DETECT_NOTIFY is not None
        and (current_time - _LAST_DETECT_NOTIFY).total_seconds() < 60
    ):
        return
    unique = sorted({name or "unknown" for name in _PENDING_DETECTS})
    message = "New monitored containers: " + ", ".join(unique)
    notify_pushover(settings, f"Guerite on {hostname}", message)
    notify_webhook(settings, f"Guerite on {hostname}", message)
    LOG.info(message)
    _PENDING_DETECTS = []
    _LAST_DETECT_NOTIFY = current_time


def run_once(
    client: DockerClient,
    settings: Settings,
    timestamp: Optional[datetime] = None,
    containers: Optional[list[Container]] = None,
) -> None:
    _ensure_health_backoff_loaded(settings.state_file)
    _ensure_upgrade_state_loaded(settings.state_file)
    _ensure_known_containers_loaded(settings.state_file)
    current_time = timestamp or now_utc()
    _metric_increment("scans_total")

    # Check for stalled upgrades first
    try:
        _recover_stalled_upgrades(client, settings, [], _should_notify(settings, "restart"))
    except Exception as error:
        LOG.warning("Upgrade recovery failed: %s", error)

    # Check for upgrades that may need manual intervention
    try:
        _check_for_manual_intervention(
            client, settings, [], _should_notify(settings, "restart")
        )
    except Exception as error:
        LOG.warning("Manual intervention check failed: %s", error)

    prune_due = _prune_due(settings, current_time)
    monitored = (
        containers
        if containers is not None
        else select_monitored_containers(client, settings)
    )
    monitored = _order_by_compose(monitored, settings)
    _track_new_containers(monitored)
    event_log: list[str] = []
    hostname = gethostname()
    _metric_increment("containers_scanned", len(monitored))
    rolling_seen: set[Optional[str]] = set()
    if not monitored:
        _metric_increment("scans_skipped")
    base_map = {_base_name(container): container for container in monitored}
    try:
        _reconcile_failed_upgrades(
            client,
            base_map,
            event_log,
            _should_notify(settings, "restart"),
            state_file=settings.state_file,
        )
    except Exception as error:
        LOG.warning("Failed to reconcile failed upgrades: %s", error)
    for container in monitored:
        marked_in_flight = False
        if settings.rolling_restart:
            project = _compose_project(container)
            if project in rolling_seen:
                LOG.debug(
                    "Skipping %s; rolling restart already performed for %s",
                    container.name,
                    project,
                )
                continue
        try:
            deps = _label_dependencies(container, settings) | _link_targets(container)
            deps = {dep for dep in deps if dep in base_map}
            skip_container = False
            for dep in deps:
                dep_container = base_map.get(dep)
                if dep_container is None:
                    continue
                try:
                    dep_state = dep_container.attrs.get("State", {})
                    dep_running = bool(dep_state.get("Running"))
                except DockerException:
                    dep_running = True
                if not dep_running:
                    LOG.info("Skipping %s; dependency %s not running", container.name, dep)
                    skip_container = True
                    break
                if _is_unhealthy(dep_container):
                    LOG.info("Skipping %s; dependency %s unhealthy", container.name, dep)
                    skip_container = True
                    break
            if skip_container:
                continue

            base_name = _base_name(container)
            if not _action_allowed(base_name, current_time, settings):
                continue

            update_due = _cron_matches(container, settings.update_label, current_time)
            restart_due = _cron_matches(container, settings.restart_label, current_time)
            recreate_due = _cron_matches(container, settings.recreate_label, current_time)
            health_due = _cron_matches(container, settings.health_label, current_time)
            if (update_due or recreate_due or health_due) and _is_swarm_managed(container):
                LOG.warning(
                    "Skipping %s; swarm-managed containers may lose secrets/configs if recreated",
                    container.name,
                )
                if (
                    _should_notify(settings, "restart")
                    or _should_notify(settings, "recreate")
                    or _should_notify(settings, "update")
                    or _should_notify(settings, "health")
                ):
                    event_log.append(
                        f"Skipping swarm-managed container {container.name}; secrets/configs not safely restorable"
                    )
                continue
            if health_due and not _has_healthcheck(container):
                if container.id not in _NO_HEALTH_WARNED:
                    LOG.warning(
                        "Container %s has %s label but no healthcheck; skipping health restarts",
                        container.name,
                        settings.health_label,
                    )
                    _NO_HEALTH_WARNED.add(container.id)
                health_due = False
            recently_started = False
            if health_due and _has_healthcheck(container):
                recently_started = _started_recently(
                    container, current_time, settings.health_backoff_seconds
                )
            unhealthy_now = health_due and not recently_started and _is_unhealthy(container)

            if not any([update_due, restart_due, recreate_due, unhealthy_now]):
                LOG.debug("Skipping %s; no actions scheduled now", container.name)
                continue

            image_ref = get_image_reference(container)
            if image_ref is None:
                LOG.warning("Skipping %s; missing image reference", container.name)
                continue
            mode = _resolve_container_modes(container, settings)
            labels = container.labels or {}
            pre_check = labels.get(settings.pre_check_label)
            post_check = labels.get(settings.post_check_label)
            pre_update = labels.get(settings.pre_update_label)
            post_update = labels.get(settings.post_update_label)
            pre_update_timeout = _resolve_hook_timeout(
                container, settings.pre_update_timeout_label, settings.hook_timeout_seconds
            )
            post_update_timeout = _resolve_hook_timeout(
                container, settings.post_update_timeout_label, settings.hook_timeout_seconds
            )
            if settings.lifecycle_hooks_enabled and pre_check:
                _run_lifecycle_hook(
                    client,
                    container,
                    pre_check,
                    settings.hook_timeout_seconds,
                    event_log,
                    "pre-check",
                )

            update_executed = False
            if update_due:
                notify_update = _should_notify(settings, "update")
                old_image_id = current_image_id(container)
                pulled_image = (
                    None if mode["no_pull"] else pull_image(client, image_ref)
                )
                if pulled_image is not None and needs_update(container, pulled_image):
                    LOG.info("Updating %s with image %s", container.name, image_ref)
                    _mark_action(base_name, current_time)
                    marked_in_flight = True
                    if notify_update:
                        event_log.append(f"Found new {image_ref} image")
                    if settings.dry_run:
                        LOG.info("Dry-run enabled; not restarting %s", container.name)
                    elif mode["no_restart"]:
                        LOG.info("No-restart enabled; not recreating %s", container.name)
                        if notify_update:
                            event_log.append(
                                f"Update available for {container.name} but no-restart enabled"
                            )
                    else:
                        if _supports_is_upgrade(restart_container):
                            ok = restart_container(
                                client,
                                container,
                                image_ref,
                                pulled_image.id,
                                settings,
                                event_log,
                                notify_update,
                                is_upgrade=True,
                                pre_update_hook=pre_update if settings.lifecycle_hooks_enabled else None,
                                post_update_hook=post_update if settings.lifecycle_hooks_enabled else None,
                                pre_update_timeout=pre_update_timeout,
                                post_update_timeout=post_update_timeout,
                            )
                        else:
                            ok = restart_container(
                                client,
                                container,
                                image_ref,
                                pulled_image.id,
                                settings,
                                event_log,
                                notify_update,
                                pre_update_hook=pre_update if settings.lifecycle_hooks_enabled else None,
                                post_update_hook=post_update if settings.lifecycle_hooks_enabled else None,
                                pre_update_timeout=pre_update_timeout,
                                post_update_timeout=post_update_timeout,
                            )
                        if ok:
                            remove_old_image(
                                client,
                                old_image_id,
                                pulled_image.id,
                                event_log,
                                notify_update,
                            )
                            update_executed = True
                            _metric_increment("containers_updated")
                            if settings.rolling_restart:
                                rolling_seen.add(_compose_project(container))
                        else:
                            _metric_increment("containers_failed")
                elif pulled_image is not None:
                    LOG.debug("%s is up-to-date", container.name)
                elif mode["no_pull"]:
                    LOG.info("No-pull enabled; skipping update check for %s", container.name)
                elif notify_update:
                    event_log.append(f"Failed to pull {image_ref} for {container.name}")
                    _metric_increment("containers_failed")
                if update_executed:
                    continue

            if recreate_due and not unhealthy_now and not update_executed:
                if not _restart_allowed(
                    container.id, base_name, current_time, settings
                ):
                    if (
                        _should_notify(settings, "restart")
                        or _should_notify(settings, "recreate")
                        or _should_notify(settings, "update")
                        or _should_notify(settings, "health")
                    ):
                        with _STATE_LOCK:
                            backoff_until = _RESTART_BACKOFF.get(container.id)
                        if backoff_until is not None:
                            _notify_restart_backoff(
                                container.name,
                                container.id,
                                backoff_until,
                                event_log,
                                settings,
                            )
                    continue

                LOG.info("Recreating %s (scheduled recreate)", container.name)
                _mark_action(base_name, current_time)
                marked_in_flight = True
                if settings.dry_run:
                    LOG.info("Dry-run enabled; not recreating %s", container.name)
                    continue
                if mode["no_restart"]:
                    LOG.info("No-restart enabled; not recreating %s", container.name)
                    continue
                notify_recreate = _should_notify(settings, "recreate")
                image_id = current_image_id(container)
                if notify_recreate:
                    event_log.append(
                        f"Recreating {container.name} (scheduled recreate) ({_image_display_name(image_ref=image_ref)})"
                    )
                if restart_container(
                    client,
                    container,
                    image_ref,
                    image_id,
                    settings,
                    event_log,
                    notify_recreate,
                    pre_update_hook=pre_update if settings.lifecycle_hooks_enabled else None,
                    post_update_hook=post_update if settings.lifecycle_hooks_enabled else None,
                    pre_update_timeout=pre_update_timeout,
                    post_update_timeout=post_update_timeout,
                ):
                    _metric_increment("containers_updated")
                    if settings.rolling_restart:
                        rolling_seen.add(_compose_project(container))
                else:
                    _metric_increment("containers_failed")
                continue

            if unhealthy_now and not _health_allowed(
                container.id, base_name, current_time, settings
            ):
                continue
            if health_due and recently_started:
                LOG.debug("Skipping %s; healthcheck still in grace window", container.name)
                continue
            if restart_due and not unhealthy_now:
                LOG.info("Restarting %s (scheduled restart)", container.name)
                _mark_action(base_name, current_time)
                marked_in_flight = True
                if settings.dry_run:
                    LOG.info("Dry-run enabled; not restarting %s", container.name)
                    continue
                if mode["no_restart"]:
                    LOG.info("No-restart enabled; not restarting %s", container.name)
                    continue
                notify_restart = _should_notify(settings, "restart")
                image_id = current_image_id(container)
                try:
                    container.restart()
                    if notify_restart:
                        event_log.append(
                            f"Restarted {container.name} (scheduled restart) ({_image_display_name(image_ref=image_ref)})"
                        )
                    _metric_increment("containers_updated")
                    if settings.rolling_restart:
                        rolling_seen.add(_compose_project(container))
                except DockerException as error:
                    LOG.error("Failed to restart %s: %s", container.name, error)
                    if notify_restart:
                        event_log.append(f"Failed to restart {container.name}: {error}")
                    _metric_increment("containers_failed")
                    _register_restart_failure(
                        container.id,
                        container.name,
                        notify_restart,
                        event_log,
                        settings,
                        error,
                    )
                continue

            if unhealthy_now:
                if not _restart_allowed(container.id, base_name, current_time, settings):
                    if (
                        _should_notify(settings, "restart")
                        or _should_notify(settings, "recreate")
                        or _should_notify(settings, "update")
                        or _should_notify(settings, "health")
                    ):
                        with _STATE_LOCK:
                            backoff_until = _RESTART_BACKOFF.get(container.id)
                        if backoff_until is not None:
                            _notify_restart_backoff(
                                container.name,
                                container.id,
                                backoff_until,
                                event_log,
                                settings,
                            )
                    continue

                LOG.info("Restarting %s due to unhealthy", container.name)
                _mark_action(base_name, current_time)
                marked_in_flight = True
                if settings.dry_run:
                    LOG.info("Dry-run enabled; not restarting %s", container.name)
                    continue
                if mode["no_restart"]:
                    LOG.info("No-restart enabled; not restarting %s", container.name)
                    continue
                notify_event = _should_notify(settings, "health") or _should_notify(
                    settings, "health_check"
                )
                new_image_id = current_image_id(container)
                if restart_container(
                    client,
                    container,
                    image_ref,
                    new_image_id,
                    settings,
                    event_log,
                    notify_event,
                    pre_update_hook=pre_update if settings.lifecycle_hooks_enabled else None,
                    post_update_hook=post_update if settings.lifecycle_hooks_enabled else None,
                    pre_update_timeout=pre_update_timeout,
                    post_update_timeout=post_update_timeout,
                ):
                    with _STATE_LOCK:
                        _HEALTH_BACKOFF[container.id] = current_time + timedelta(
                            seconds=settings.health_backoff_seconds
                        )
                    _save_health_backoff(settings.state_file)
                    if _should_notify(settings, "health") or _should_notify(
                        settings, "health_check"
                    ):
                        event_log.append(
                            f"Restarted {container.name} after failed health check ({_image_display_name(image_ref=image_ref)})"
                        )
                    _metric_increment("containers_updated")
                    if settings.rolling_restart:
                        rolling_seen.add(_compose_project(container))
                elif notify_event:
                    event_log.append(f"Failed to restart {container.name}")
                    _metric_increment("containers_failed")

            if settings.lifecycle_hooks_enabled and post_check:
                _run_lifecycle_hook(
                    client,
                    container,
                    post_check,
                    settings.hook_timeout_seconds,
                    event_log,
                    "post-check",
                )
        finally:
            if marked_in_flight:
                _clear_in_flight(base_name)

    if prune_due:
        notify_prune = _should_notify(settings, "prune")
        prune_images(client, settings, event_log, notify_prune)

    if event_log:
        title = f"Guerite on {hostname}"
        body = "\n".join(event_log)
        notify_pushover(settings, title, body)
        notify_webhook(settings, title, body)
    _flush_detect_notifications(settings, hostname, current_time)
    with _STATE_LOCK:
        _IN_FLIGHT.clear()

    # Save state at the end of each run for crash recovery
    _save_upgrade_state(settings.state_file)
    _save_known_containers(settings.state_file)


def next_wakeup(
    containers: list[Container],
    settings: Settings,
    reference: datetime,
) -> tuple[datetime, Optional[str], Optional[str]]:
    candidates: list[tuple[datetime, Optional[str], Optional[str]]] = []
    for container in containers:
        for label_key in (
            settings.update_label,
            settings.restart_label,
            settings.recreate_label,
            settings.health_label,
        ):
            cron_expression = container.labels.get(label_key)
            if cron_expression is None:
                LOG.debug(
                    "%s has no %s; ignoring for scheduling", container.name, label_key
                )
                continue
            try:
                iterator = croniter(cron_expression, reference, ret_type=datetime)
                next_time = iterator.get_next(datetime)
                candidates.append((next_time, container.name, label_key))
                upcoming = _upcoming_runs(iterator, count=2)
                LOG.debug(
                    "%s %s (%s) next %s then %s",
                    container.name,
                    label_key,
                    cron_expression,
                    next_time.isoformat(),
                    [ts.isoformat() for ts in upcoming],
                )
            except (ValueError, KeyError) as error:
                LOG.warning(
                    "Invalid cron expression on %s (%s): %s",
                    container.name,
                    label_key,
                    error,
                )

    if not candidates:
        return reference + timedelta(seconds=300), None, None

    return min(candidates, key=lambda item: item[0])


def _upcoming_runs(iterator: croniter, count: int) -> list[datetime]:
    runs: list[datetime] = []
    for _ in range(count):
        try:
            runs.append(iterator.get_next(datetime))
        except (StopIteration, ValueError):
            break
    return runs


def _format_human(dt: datetime, reference: datetime) -> str:
    reference_date = reference.date()
    if dt.tzinfo is not None and reference.tzinfo is not None:
        dt = dt.astimezone(reference.tzinfo)
    date_part = dt.date()
    if date_part == reference_date:
        prefix = "today"
    elif date_part == reference_date + timedelta(days=1):
        prefix = "tomorrow"
    else:
        prefix = date_part.isoformat()
    return f"{prefix} {dt.strftime('%H:%M')}"


def _short_label(label: str) -> str:
    if label.startswith("guerite."):
        return label.split(".", 1)[1]
    return label


def schedule_summary(
    containers: list[Container], settings: Settings, reference: datetime
) -> list[str]:
    events: list[tuple[datetime, str, str]] = []
    for container in containers:
        for label_key in (
            settings.update_label,
            settings.restart_label,
            settings.recreate_label,
            settings.health_label,
        ):
            cron_expression = container.labels.get(label_key)
            if cron_expression is None:
                continue
            try:
                iterator = croniter(cron_expression, reference, ret_type=datetime)
                next_time = iterator.get_next(datetime)
                events.append((next_time, container.name, label_key))
            except (ValueError, KeyError):
                continue

    events.sort(key=lambda item: item[0])
    summary: list[str] = []
    for next_time, name, label in events[:10]:
        summary.append(
            f"{_format_human(next_time, reference)} {name} ({_short_label(label)})"
        )
    return summary
