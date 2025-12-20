from datetime import datetime
from logging import getLogger
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


def select_monitored_containers(client: DockerClient, settings: Settings) -> list[Container]:
    label_filter = f"{settings.monitor_label}={settings.monitor_value}"
    try:
        return client.containers.list(filters={"label": label_filter})
    except DockerException as error:
        LOG.error("Failed to list containers: %s", error)
        return []


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


def schedule_allows_run(container: Container, settings: Settings, timestamp: datetime) -> bool:
    cron_expression = container.labels.get(settings.cron_label)
    if cron_expression is None:
        LOG.debug("%s has no cron label; running now", container.name)
        return True
    try:
        allowed = croniter.match(cron_expression, timestamp)
        LOG.debug("%s cron %s at %s -> %s", container.name, cron_expression, timestamp.isoformat(), allowed)
        return allowed
    except (ValueError, KeyError) as error:
        LOG.warning("Invalid cron expression on %s: %s", container.name, error)
        return False


def restart_container(client: DockerClient, container: Container, image_ref: str) -> bool:
    config = container.attrs.get("Config", {})
    host_config = container.attrs.get("HostConfig")
    networking = container.attrs.get("NetworkSettings", {}).get("Networks")
    name = container.name

    try:
        LOG.info("Stopping %s", name)
        container.stop()
        container.remove()
        created = client.api.create_container(
            image=image_ref,
            name=name,
            command=config.get("Cmd"),
            environment=config.get("Env"),
            host_config=host_config,
            labels=config.get("Labels"),
            volumes=config.get("Volumes"),
            working_dir=config.get("WorkingDir"),
            user=config.get("User"),
            entrypoint=config.get("Entrypoint"),
            tty=config.get("Tty"),
        )
        new_id = created.get("Id")
        if networking is not None and new_id is not None:
            for network_name, network_cfg in networking.items():
                client.api.connect_container_to_network(
                    new_id,
                    network_name,
                    aliases=network_cfg.get("Aliases"),
                    links=network_cfg.get("Links"),
                )
        if new_id is not None:
            client.api.start(new_id)
        LOG.info("Restarted %s", name)
        return True
    except (APIError, DockerException) as error:
        LOG.error("Failed to restart %s: %s", name, error)
        return False


def remove_old_image(client: DockerClient, old_image_id: Optional[str], new_image_id: str) -> None:
    if old_image_id is None or old_image_id == new_image_id:
        return
    try:
        client.images.remove(image=old_image_id)
        LOG.info("Removed old image %s", old_image_id)
    except APIError as error:
        LOG.debug("Could not remove old image %s: %s", old_image_id, error)
    except DockerException as error:
        LOG.debug("Could not remove old image %s: %s", old_image_id, error)


def run_once(client: DockerClient, settings: Settings) -> None:
    timestamp = now_utc()
    for container in select_monitored_containers(client, settings):
        LOG.debug(
            "%s labels monitor=%s cron=%s",
            container.name,
            container.labels.get(settings.monitor_label),
            container.labels.get(settings.cron_label),
        )
        if not schedule_allows_run(container, settings, timestamp):
            LOG.debug("Skipping %s; not scheduled at this time", container.name)
            continue

        image_ref = container.attrs.get("Config", {}).get("Image")
        if image_ref is None:
            LOG.warning("Skipping %s; missing image reference", container.name)
            continue

        old_image_id = current_image_id(container)

        pulled_image = pull_image(client, image_ref)
        if pulled_image is None:
            continue

        if not needs_update(container, pulled_image):
            LOG.debug("%s is up-to-date", container.name)
            continue

        LOG.info("Updating %s with image %s", container.name, image_ref)
        if settings.dry_run:
            LOG.info("Dry-run enabled; not restarting %s", container.name)
            continue

        if restart_container(client, container, image_ref):
            remove_old_image(client, old_image_id, pulled_image.id)
            notify_pushover(settings, "Guerite", f"Updated {container.name} with {image_ref}")
