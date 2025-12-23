from datetime import datetime
from datetime import timedelta
from logging import getLogger
from socket import gethostname
from typing import Optional

from croniter import croniter
from docker import DockerClient
from docker.errors import APIError, DockerException
from docker.models.containers import Container
from docker.models.images import Image

from .config import Settings
from .notifier import notify_pushover
from .utils import now_utc

LOG = getLogger(__name__)
_HEALTH_BACKOFF: dict[str, datetime] = {}


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


def _health_allowed(container_id: str, now: datetime, settings: Settings) -> bool:
    next_time = _HEALTH_BACKOFF.get(container_id)
    if next_time is None:
        return True
    if now >= next_time:
        return True
    remaining = (next_time - now).total_seconds()
    LOG.debug("Skipping unhealthy restart for %s; backoff %.0fs remaining", container_id, remaining)
    return False


def _should_notify(settings: Settings, event: str) -> bool:
    return event in settings.notifications


def _short_id(identifier: Optional[str]) -> str:
    if identifier is None:
        return "unknown"
    return identifier.split(":")[-1][:12]


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

    exposed_ports = config.get("ExposedPorts")
    ports = list(exposed_ports.keys()) if isinstance(exposed_ports, dict) else None

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
        "name": name,
        "network_disabled": config.get("NetworkDisabled"),
        "ports": ports,
        "runtime": host_config.get("Runtime") if isinstance(host_config, dict) else None,
        "stdin_open": config.get("OpenStdin"),
        "stop_signal": config.get("StopSignal"),
        "stop_timeout": config.get("StopTimeout"),
        "tty": config.get("Tty"),
        "user": config.get("User"),
        "volumes": config.get("Volumes"),
        "working_dir": config.get("WorkingDir"),
    }

    create_kwargs = {key: value for key, value in create_kwargs.items() if value is not None}

    try:
        LOG.info("Stopping %s", name)
        if notify:
            event_log.append(f"Stopping container {name} ({_short_id(container.image.id)})")
        container.stop()
        container.remove()
        created = client.api.create_container(**create_kwargs)
        new_id = created.get("Id")
        if networking is not None and new_id is not None:
            for network_name, network_cfg in networking.items():
                ipam_cfg = network_cfg.get("IPAMConfig") or {}
                client.api.connect_container_to_network(
                    new_id,
                    network_name,
                    aliases=network_cfg.get("Aliases"),
                    links=network_cfg.get("Links"),
                    ipv4_address=ipam_cfg.get("IPv4Address"),
                    ipv6_address=ipam_cfg.get("IPv6Address"),
                    link_local_ips=ipam_cfg.get("LinkLocalIPs"),
                )
        if new_id is not None:
            client.api.start(new_id)
            if notify:
                event_log.append(f"Creating container {name} ({_short_id(new_image_id)})")
        LOG.info("Restarted %s", name)
        return True
    except (APIError, DockerException) as error:
        LOG.error("Failed to restart %s: %s", name, error)
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
    except APIError as error:
        LOG.debug("Could not remove old image %s: %s", old_image_id, error)
    except DockerException as error:
        LOG.debug("Could not remove old image %s: %s", old_image_id, error)


def run_once(
    client: DockerClient,
    settings: Settings,
    timestamp: Optional[datetime] = None,
    containers: Optional[list[Container]] = None,
) -> None:
    current_time = timestamp or now_utc()
    monitored = containers if containers is not None else select_monitored_containers(client, settings)
    event_log: list[str] = []
    hostname = gethostname()
    for container in monitored:
        update_due = _cron_matches(container, settings.update_label, current_time)
        restart_due = _cron_matches(container, settings.restart_label, current_time)
        health_due = _cron_matches(container, settings.health_label, current_time)
        unhealthy_now = health_due and _is_unhealthy(container)

        if not any([update_due, restart_due, unhealthy_now]):
            LOG.debug("Skipping %s; no actions scheduled now", container.name)
            continue

        image_ref = container.attrs.get("Config", {}).get("Image")
        if image_ref is None:
            LOG.warning("Skipping %s; missing image reference", container.name)
            continue

        if update_due:
            notify_update = _should_notify(settings, "update")
            old_image_id = current_image_id(container)
            pulled_image = pull_image(client, image_ref)
            if pulled_image is None:
                continue
            if not needs_update(container, pulled_image):
                LOG.debug("%s is up-to-date", container.name)
                continue
            LOG.info("Updating %s with image %s", container.name, image_ref)
            if notify_update:
                event_log.append(
                    f"Found new {image_ref} image ({_short_id(pulled_image.id)})"
                )
            if settings.dry_run:
                LOG.info("Dry-run enabled; not restarting %s", container.name)
                continue
            if restart_container(
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
            continue

        if unhealthy_now and not _health_allowed(container.id, current_time, settings):
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
                if _should_notify(settings, "health") or _should_notify(settings, "health_check"):
                    event_log.append(
                        f"Restarted {container.name} after failed health check ({_short_id(new_image_id)})"
                    )
            elif restart_due and _should_notify(settings, "restart"):
                event_log.append(
                    f"Restarted {container.name} (scheduled restart) ({_short_id(new_image_id)})"
                )

    if event_log:
        notify_pushover(settings, f"Guerite on {hostname}", "\n".join(event_log))


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


def _format_human(dt: datetime) -> str:
    today = reference = now_utc().date()
    date_part = dt.date()
    if date_part == reference:
        prefix = "today"
    elif date_part == reference + timedelta(days=1):
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
        summary.append(f"{_format_human(next_time)} {name} ({_short_label(label)})")
    return summary
