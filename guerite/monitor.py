from re import compile as re_compile
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from json import JSONDecodeError
from json import dump
from json import load
from logging import getLogger
from os.path import exists

from typing import Any, Optional
from time import sleep
from dataclasses import dataclass

from croniter import croniter
from docker import DockerClient
from docker.errors import APIError, DockerException
from docker.models.containers import Container
from docker.models.images import Image
from requests.exceptions import ReadTimeout, RequestException
from urllib3.exceptions import ReadTimeoutError

from .config import Settings
from .notifier import notify_pushover
from .notifier import notify_webhook
from .utils import now_utc

LOG = getLogger(__name__)
_HEALTH_BACKOFF: dict[str, datetime] = {}
_HEALTH_BACKOFF_LOADED = False
_NO_HEALTH_WARNED: set[str] = set()
_PRUNE_CRON_INVALID = False
_KNOWN_CONTAINERS: set[str] = set()
_KNOWN_CONTAINER_NAMES: set[str] = set()
_KNOWN_INITIALIZED = False
_PENDING_DETECTS: list[str] = []
_LAST_DETECT_NOTIFY: Optional[datetime] = None
_GUERITE_CREATED: set[str] = set()
_RESTART_BACKOFF: dict[str, datetime] = {}
_RESTART_FAIL_COUNT: dict[str, int] = {}
_LAST_ACTION: dict[str, datetime] = {}
_IN_FLIGHT: set[str] = set()


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
    try:
        with open(state_file, "w", encoding="utf-8") as handle:
            dump(serializable, handle)
    except OSError as error:
        LOG.debug("Failed to persist health backoff state to %s: %s", state_file, error)


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
                seen[container.id] = container
        except DockerException as error:
            LOG.error("Failed to list containers with label %s: %s", label, error)
    return list(seen.values())


def pull_image(client: DockerClient, image_ref: str) -> Optional[Image]:
    try:
        return client.images.pull(image_ref)
    except DockerException as error:
        LOG.error("Failed to pull image %s: %s", image_ref, error)
        return None


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
    return event in settings.notifications


def _clean_cron_expression(value: Optional[str]) -> Optional[str]:
    if value is None:
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
    global _KNOWN_INITIALIZED, _GUERITE_CREATED
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


def _action_allowed(base_name: str, now: datetime, settings: Settings) -> bool:
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
    _LAST_ACTION[base_name] = when
    _IN_FLIGHT.add(base_name)


def _strip_guerite_suffix(name: str) -> str:
    pattern = re_compile(r"^(.*)-guerite-(?:old|new)-[0-9a-f]{8}$")
    current = name
    while True:
        match = pattern.match(current)
        if match is None:
            return current
        current = match.group(1)


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
    config = container.attrs.get("Config", {})
    host_config = container.attrs.get("HostConfig")
    networking = container.attrs.get("NetworkSettings", {}).get("Networks")

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

    # Preflight checks
    config = container.attrs.get("Config", {})
    mounts = container.attrs.get("Mounts") or []
    networking = container.attrs.get("NetworkSettings", {}).get("Networks")

    _preflight_mounts(name, mounts, notify, event_log)

    try:
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
            _RESTART_FAIL_COUNT.pop(container.id, None)
            _RESTART_BACKOFF.pop(container.id, None)

        return True

    except (APIError, DockerException, RuntimeError, TypeError) as error:
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

        LOG.debug("Pruning images with Docker API timeout=%s", client.api.timeout)

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
    except (ReadTimeout, ReadTimeoutError, RequestException) as error:
        LOG.warning("Image prune timed out or connection failed: %s", error)
        if notify:
            event_log.append(f"Image prune timed out: {error}")


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
    current_time = timestamp or now_utc()
    prune_due = _prune_due(settings, current_time)
    monitored = (
        containers
        if containers is not None
        else select_monitored_containers(client, settings)
    )
    monitored = _order_by_compose(monitored, settings)
    _track_new_containers(monitored)
    event_log: list[str] = []
    hostname = settings.hostname
    base_map = {_base_name(container): container for container in monitored}
    for container in monitored:
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

        update_executed = False
        if update_due:
            notify_update = _should_notify(settings, "update")
            old_image_id = current_image_id(container)
            pulled_image = pull_image(client, image_ref)
            if pulled_image is not None and needs_update(container, pulled_image):
                LOG.info("Updating %s with image %s", container.name, image_ref)
                _mark_action(base_name, current_time)
                if notify_update:
                    event_log.append(f"Found new {image_ref} image")
                if settings.dry_run:
                    LOG.info("Dry-run enabled; not restarting %s", container.name)
                elif restart_container(
                    client,
                    container,
                    image_ref,
                    pulled_image.id,
                    settings,
                    event_log,
                    notify_update,
                ):
                    remove_old_image(
                        client,
                        old_image_id,
                        pulled_image.id,
                        event_log,
                        notify_update,
                    )
                    update_executed = True
            elif pulled_image is not None:
                LOG.debug("%s is up-to-date", container.name)
            elif notify_update:
                event_log.append(f"Failed to pull {image_ref} for {container.name}")
            if update_executed:
                continue

            if recreate_due and not unhealthy_now:
                if not _restart_allowed(
                    container.id, base_name, current_time, settings
                ):
                    if (
                        _should_notify(settings, "restart")
                        or _should_notify(settings, "recreate")
                        or _should_notify(settings, "update")
                        or _should_notify(settings, "health")
                    ):
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
                if settings.dry_run:
                    LOG.info("Dry-run enabled; not recreating %s", container.name)
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
                ):
                    pass
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
            if settings.dry_run:
                LOG.info("Dry-run enabled; not restarting %s", container.name)
                continue
            notify_restart = _should_notify(settings, "restart")
            image_id = current_image_id(container)
            try:
                container.restart()
                if notify_restart:
                    event_log.append(
                        f"Restarted {container.name} (scheduled restart) ({_image_display_name(image_ref=image_ref)})"
                    )
            except DockerException as error:
                LOG.error("Failed to restart %s: %s", container.name, error)
                if notify_restart:
                    event_log.append(f"Failed to restart {container.name}: {error}")
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
            if settings.dry_run:
                LOG.info("Dry-run enabled; not restarting %s", container.name)
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
            ):
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
            elif notify_event:
                event_log.append(f"Failed to restart {container.name}")

    if prune_due:
        notify_prune = _should_notify(settings, "prune")
        prune_images(client, settings, event_log, notify_prune)

    if event_log:
        title = f"Guerite on {hostname}"
        body = "\n".join(event_log)
        notify_pushover(settings, title, body)
        notify_webhook(settings, title, body)
    _flush_detect_notifications(settings, hostname, current_time)
    _IN_FLIGHT.clear()


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
