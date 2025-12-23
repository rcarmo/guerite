import re
from datetime import datetime
from datetime import timedelta
from json import JSONDecodeError
from json import dump
from json import load
from logging import getLogger
from os.path import exists
from socket import gethostname
from typing import Optional

from croniter import croniter
from docker import DockerClient
from docker.errors import APIError, DockerException
from docker.models.containers import Container
from docker.models.images import Image

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
_KNOWN_INITIALIZED = False
_PENDING_DETECTS: list[str] = []
_LAST_DETECT_NOTIFY: Optional[datetime] = None
_RESTART_BACKOFF: dict[str, datetime] = {}
_RESTART_FAIL_COUNT: dict[str, int] = {}



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
    serializable = {container_id: value.isoformat() for container_id, value in _HEALTH_BACKOFF.items()}
    try:
        with open(state_file, "w", encoding="utf-8") as handle:
            dump(serializable, handle)
    except OSError as error:
        LOG.debug("Failed to persist health backoff state to %s: %s", state_file, error)


def select_monitored_containers(client: DockerClient, settings: Settings) -> list[Container]:
    labels = [settings.update_label, settings.restart_label, settings.health_label]
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


def _cron_matches(container: Container, label_key: str, timestamp: datetime) -> bool:
    cron_expression = container.labels.get(label_key)
    if cron_expression is None:
        LOG.debug("%s has no %s; skipping", container.name, label_key)
        return False
    try:
        allowed = croniter.match(cron_expression, timestamp)
        LOG.debug("%s %s %s at %s -> %s", container.name, label_key, cron_expression, timestamp.isoformat(), allowed)
        return allowed
    except (ValueError, KeyError) as error:
        LOG.warning("Invalid cron expression on %s (%s): %s", container.name, label_key, error)
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
    if lowered == "healthy":
        return False
    LOG.debug("%s health status %s", container.name, lowered)
    return True


def _has_healthcheck(container: Container) -> bool:
    try:
        health_cfg = container.attrs.get("Config", {}).get("Healthcheck")
    except DockerException as error:
        LOG.warning("Could not read health configuration for %s: %s", container.name, error)
        return False
    return bool(health_cfg)


def _is_swarm_managed(container: Container) -> bool:
    return "com.docker.swarm.service.id" in container.labels


def _preflight_mounts(name: str, mounts: list[dict], notify: bool, event_log: list[str]) -> None:
    for mount in mounts:
        mount_type = mount.get("Type")
        if mount_type == "bind":
            source = mount.get("Source")
            if source and not exists(source):
                LOG.warning("Bind source %s missing for %s; recreate may fail", source, name)
                if notify:
                    event_log.append(f"Bind source missing for {name}: {source}")
        elif mount_type == "volume":
            driver = mount.get("Driver")
            if driver and driver != "local":
                LOG.warning("Volume %s uses driver %s for %s; ensure driver is available", mount.get("Name"), driver, name)
                if notify:
                    event_log.append(
                        f"Volume driver {driver} for {name} at {mount.get('Destination')}"
                    )


def _health_allowed(container_id: str, now: datetime, settings: Settings) -> bool:
    next_time = _HEALTH_BACKOFF.get(container_id)
    if next_time is None:
        return True
    if now >= next_time:
        return True
    remaining = (next_time - now).total_seconds()
    LOG.debug("Skipping unhealthy restart for %s; backoff %.0fs remaining", container_id, remaining)
    return False


def _restart_allowed(container_id: str, now: datetime, settings: Settings) -> bool:
    next_time = _RESTART_BACKOFF.get(container_id)
    if next_time is None:
        return True
    if now >= next_time:
        return True
    remaining = (next_time - now).total_seconds()
    LOG.debug("Skipping restart for %s; recreate backoff %.0fs remaining", container_id, remaining)
    return False


def _notify_restart_backoff(container_name: str, container_id: str, backoff_until: datetime, event_log: list[str], settings: Settings) -> None:
    key = f"{container_id}-backoff-notified"
    if key in _HEALTH_BACKOFF:
        return
    event_log.append(
        f"Recreate for {container_name} deferred until {backoff_until.isoformat()} after repeated failures"
    )
    _HEALTH_BACKOFF[key] = backoff_until


def _should_notify(settings: Settings, event: str) -> bool:
    return event in settings.notifications


def _clean_cron_expression(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].strip()
    if (cleaned.startswith("\"") and cleaned.endswith("\"")) or (cleaned.startswith("'") and cleaned.endswith("'")):
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
    if not _KNOWN_INITIALIZED:
        for container in containers:
            _KNOWN_CONTAINERS.add(container.id)
        _KNOWN_INITIALIZED = True
        return
    for container in containers:
        if container.id not in _KNOWN_CONTAINERS:
            _KNOWN_CONTAINERS.add(container.id)
            _PENDING_DETECTS.append(container.name)


def _short_id(identifier: Optional[str]) -> str:
    if identifier is None:
        return "unknown"
    return identifier.split(":")[-1][:12]


def _strip_guerite_suffix(name: str) -> str:
    pattern = re.compile(r"^(.*)-guerite-(?:old|new)-[0-9a-f]{8}$")
    current = name
    while True:
        match = pattern.match(current)
        if match is None:
            return current
        current = match.group(1)


def restart_container(
    client: DockerClient,
    container: Container,
    image_ref: str,
    new_image_id: Optional[str],
    event_log: list[str],
    notify: bool,
) -> bool:
    config = container.attrs.get("Config", {})
    host_config = container.attrs.get("HostConfig")
    networking = container.attrs.get("NetworkSettings", {}).get("Networks")
    name = container.name

    base_name = _strip_guerite_suffix(name)

    exposed_ports = config.get("ExposedPorts")
    ports = list(exposed_ports.keys()) if isinstance(exposed_ports, dict) else None

    original_name = base_name
    short_suffix = container.id[:8]
    temp_old_name = f"{base_name}-guerite-old-{short_suffix}"
    temp_new_name = f"{base_name}-guerite-new-{short_suffix}"

    mounts = container.attrs.get("Mounts") or []
    networking = container.attrs.get("NetworkSettings", {}).get("Networks")

    endpoint_map: dict[str, dict] = {}
    if networking is not None:
        for network_name, network_cfg in networking.items():
            ipam_cfg = network_cfg.get("IPAMConfig") or {}
            endpoint_kwargs = {
                "aliases": network_cfg.get("Aliases"),
                "links": network_cfg.get("Links"),
                "ipv4_address": ipam_cfg.get("IPv4Address"),
                "ipv6_address": ipam_cfg.get("IPv6Address"),
                "link_local_ips": ipam_cfg.get("LinkLocalIPs"),
                "driver_opt": network_cfg.get("DriverOpts"),
                "mac_address": network_cfg.get("MacAddress"),
                "priority": network_cfg.get("GatewayPriority") or network_cfg.get("GwPriority"),
            }
            endpoint_kwargs = {key: value for key, value in endpoint_kwargs.items() if value is not None}
            try:
                endpoint_map[network_name] = client.api.create_endpoint_config(**endpoint_kwargs)
            except TypeError:
                if "priority" in endpoint_kwargs:
                    fallback = {key: value for key, value in endpoint_kwargs.items() if key != "priority"}
                    LOG.debug("create_endpoint_config without priority for %s", network_name)
                    endpoint_map[network_name] = client.api.create_endpoint_config(**fallback)
                else:
                    raise

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
        "name": temp_new_name,
        "network_disabled": config.get("NetworkDisabled"),
        "ports": ports,
        "runtime": host_config.get("Runtime") if isinstance(host_config, dict) else None,
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
        create_kwargs["networking_config"] = client.api.create_networking_config(endpoint_map)

    create_kwargs = {key: value for key, value in create_kwargs.items() if value is not None}

    _preflight_mounts(name, mounts, notify, event_log)

    new_id: Optional[str] = None
    old_renamed = False
    try:
        client.api.rename(container.id, temp_old_name)
        old_renamed = True
        created = client.api.create_container(**create_kwargs)
        new_id = created.get("Id")
        LOG.info("Stopping %s", original_name)
        if notify:
            event_log.append(f"Stopping container {original_name} ({_short_id(container.image.id)})")
        container.stop()
        if new_id is None:
            raise DockerException("create_container returned no Id")
        if networking is not None:
            for network_name, network_cfg in networking.items():
                mac_address = network_cfg.get("MacAddress")
                if mac_address:
                    ipam_cfg = network_cfg.get("IPAMConfig") or {}
                    try:
                        client.api.connect_container_to_network(
                            new_id,
                            network_name,
                            aliases=network_cfg.get("Aliases"),
                            links=network_cfg.get("Links"),
                            ipv4_address=ipam_cfg.get("IPv4Address"),
                            ipv6_address=ipam_cfg.get("IPv6Address"),
                            link_local_ips=ipam_cfg.get("LinkLocalIPs"),
                            driver_opt=network_cfg.get("DriverOpts"),
                            mac_address=mac_address,
                        )
                    except APIError as error:
                        LOG.error("Failed to attach %s to %s with MAC: %s", original_name, network_name, error)
                        raise
        client.api.rename(new_id, original_name)
        client.api.start(new_id)
        LOG.info("Restarted %s", original_name)
        if notify:
            event_log.append(f"Creating container {original_name} ({_short_id(new_image_id)})")
        try:
            container.remove()
        except DockerException:
            LOG.debug("Could not remove old container %s", temp_old_name)
        # Reset failure counters on success
        _RESTART_FAIL_COUNT.pop(container.id, None)
        _RESTART_BACKOFF.pop(container.id, None)
        return True
    except (APIError, DockerException, TypeError) as error:
        LOG.error("Failed to restart %s during recreate: %s", original_name, error)
        try:
            if old_renamed:
                client.api.rename(container.id, original_name)
            container.start()
        except DockerException as rollback_error:
            LOG.warning("Rollback failed for %s: %s", original_name, rollback_error)
        if new_id is not None:
            try:
                client.api.remove_container(new_id, force=True)
            except DockerException:
                LOG.debug("Cleanup failed for new container %s", new_id)
        if notify:
            event_log.append(f"Failed to restart {original_name}: {error}")
        # Bump failure count and set backoff to avoid tight loops
        fail_count = _RESTART_FAIL_COUNT.get(container.id, 0) + 1
        _RESTART_FAIL_COUNT[container.id] = fail_count
        backoff_seconds = min(settings.health_backoff_seconds * max(1, fail_count), 3600)
        backoff_until = now_utc() + timedelta(seconds=backoff_seconds)
        _RESTART_BACKOFF[container.id] = backoff_until
        if notify:
            _notify_restart_backoff(original_name, container.id, backoff_until, event_log, settings)
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
        LOG.info("Removed old image %s", old_image_id)
        if notify:
            event_log.append(f"Removing image ({_short_id(old_image_id)})")
    except (APIError, DockerException) as error:
        LOG.warning("Could not remove old image %s: %s", old_image_id, error)
        if notify:
            event_log.append(f"Failed to remove image ({_short_id(old_image_id)}): {error}")


def prune_images(
    client: DockerClient,
    settings: Settings,
    event_log: list[str],
    notify: bool,
) -> None:
    try:
        result = client.api.prune_images(filters={"dangling": False})
        reclaimed = result.get("SpaceReclaimed") if isinstance(result, dict) else None
        images_deleted = result.get("ImagesDeleted") if isinstance(result, dict) else None
        LOG.info("Pruned images; reclaimed %s bytes; deleted %s entries", reclaimed, len(images_deleted or []))
        if notify:
            summary = "Pruned images" + (f"; reclaimed {reclaimed} bytes" if reclaimed is not None else "")
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


def _flush_detect_notifications(settings: Settings, hostname: str, current_time: datetime) -> None:
    global _PENDING_DETECTS, _LAST_DETECT_NOTIFY
    if not _PENDING_DETECTS:
        return
    if not _should_notify(settings, "detect"):
        return
    if _LAST_DETECT_NOTIFY is not None and (current_time - _LAST_DETECT_NOTIFY).total_seconds() < 60:
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
    monitored = containers if containers is not None else select_monitored_containers(client, settings)
    _track_new_containers(monitored)
    event_log: list[str] = []
    hostname = gethostname()
    for container in monitored:
        update_due = _cron_matches(container, settings.update_label, current_time)
        restart_due = _cron_matches(container, settings.restart_label, current_time)
        health_due = _cron_matches(container, settings.health_label, current_time)
        if (update_due or restart_due or health_due) and _is_swarm_managed(container):
            LOG.warning("Skipping %s; swarm-managed containers may lose secrets/configs if recreated", container.name)
            if _should_notify(settings, "restart") or _should_notify(settings, "update") or _should_notify(settings, "health"):
                event_log.append(f"Skipping swarm-managed container {container.name}; secrets/configs not safely restorable")
            continue
        if health_due and not _has_healthcheck(container):
            if container.id not in _NO_HEALTH_WARNED:
                LOG.warning("Container %s has %s label but no healthcheck; skipping health restarts", container.name, settings.health_label)
                _NO_HEALTH_WARNED.add(container.id)
            health_due = False
        unhealthy_now = health_due and _is_unhealthy(container)

        if not any([update_due, restart_due, unhealthy_now]):
            LOG.debug("Skipping %s; no actions scheduled now", container.name)
            continue

        image_ref = container.attrs.get("Config", {}).get("Image")
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
                if notify_update:
                    event_log.append(
                        f"Found new {image_ref} image ({_short_id(pulled_image.id)})"
                    )
                if settings.dry_run:
                    LOG.info("Dry-run enabled; not restarting %s", container.name)
                elif restart_container(
                    client,
                    container,
                    image_ref,
                    pulled_image.id,
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

        if unhealthy_now and not _health_allowed(container.id, current_time, settings):
            continue
        if not _restart_allowed(container.id, current_time, settings):
            if _should_notify(settings, "restart") or _should_notify(settings, "update") or _should_notify(settings, "health"):
                backoff_until = _RESTART_BACKOFF.get(container.id)
                if backoff_until is not None:
                    _notify_restart_backoff(container.name, container.id, backoff_until, event_log, settings)
            continue

        reason = "scheduled restart" if restart_due else "unhealthy" if unhealthy_now else "restart"
        LOG.info("Restarting %s due to %s", container.name, reason)
        if settings.dry_run:
            LOG.info("Dry-run enabled; not restarting %s", container.name)
            continue
        if unhealthy_now:
            notify_event = _should_notify(settings, "health") or _should_notify(settings, "health_check")
        else:
            notify_event = _should_notify(settings, "restart")
        new_image_id = current_image_id(container)
        if restart_container(
            client,
            container,
            image_ref,
            new_image_id,
            event_log,
            notify_event,
        ):
            if unhealthy_now:
                _HEALTH_BACKOFF[container.id] = current_time + timedelta(seconds=settings.health_backoff_seconds)
                _save_health_backoff(settings.state_file)
                if _should_notify(settings, "health") or _should_notify(settings, "health_check"):
                    event_log.append(
                        f"Restarted {container.name} after failed health check ({_short_id(new_image_id)})"
                    )
            elif restart_due and _should_notify(settings, "restart"):
                event_log.append(
                    f"Restarted {container.name} (scheduled restart) ({_short_id(new_image_id)})"
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


def next_wakeup(containers: list[Container], settings: Settings, reference: datetime) -> datetime:
    candidates: list[datetime] = []
    for container in containers:
        for label_key in (settings.update_label, settings.restart_label, settings.health_label):
            cron_expression = container.labels.get(label_key)
            if cron_expression is None:
                LOG.debug("%s has no %s; ignoring for scheduling", container.name, label_key)
                continue
            try:
                iterator = croniter(cron_expression, reference, ret_type=datetime)
                next_time = iterator.get_next(datetime)
                candidates.append(next_time)
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
                LOG.warning("Invalid cron expression on %s (%s): %s", container.name, label_key, error)

    if not candidates:
        return reference + timedelta(seconds=300)

    return min(candidates)


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


def schedule_summary(containers: list[Container], settings: Settings, reference: datetime) -> list[str]:
    events: list[tuple[datetime, str, str]] = []
    for container in containers:
        for label_key in (settings.update_label, settings.restart_label, settings.health_label):
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
        summary.append(f"{_format_human(next_time, reference)} {name} ({_short_label(label)})")
    return summary
