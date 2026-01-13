from datetime import timedelta
from logging import getLogger
from math import ceil

from threading import Event, Thread
from time import sleep
from typing import Optional

from docker import DockerClient
from docker.errors import DockerException

from .config import Settings, load_settings
from .monitor import (
    _action_allowed,
    _strip_guerite_suffix,
    next_prune_time,
    next_wakeup,
    run_once,
    schedule_summary,
    select_monitored_containers,
)
from .notifier import notify_pushover
from .utils import configure_logging, now_tz

LOG = getLogger(__name__)


def _format_human_local(dt, reference):
    if dt.tzinfo is not None and reference.tzinfo is not None:
        dt = dt.astimezone(reference.tzinfo)
    reference_date = reference.date()
    date_part = dt.date()
    if date_part == reference_date:
        prefix = "today"
    elif date_part == reference_date + timedelta(days=1):
        prefix = "tomorrow"
    else:
        prefix = date_part.isoformat()
    return f"{prefix} {dt.strftime('%H:%M')}"


def _short_label(label: Optional[str]) -> str:
    if label is None:
        return "unspecified"
    if label.startswith("guerite."):
        return label.split(".", 1)[1]
    return label


def _format_reason(container_name: Optional[str], label_key: Optional[str]) -> str:
    name = container_name or "unspecified"
    label = _short_label(label_key)
    return f"{name} ({label})"


def build_client(settings: Settings) -> DockerClient:
    try:
        return DockerClient(base_url=settings.docker_host)
    except DockerException as error:
        raise SystemExit(f"Unable to connect to Docker: {error}") from error


def is_monitored_event(event: dict, settings: Settings) -> bool:
    if event.get("Type") != "container":
        return False
    action = event.get("Action")
    if action not in {
        "create",
        "destroy",
        "die",
        "kill",
        "pause",
        "rename",
        "restart",
        "start",
        "stop",
        "unpause",
        "update",
    }:
        return False
    attributes = event.get("Actor", {}).get("Attributes", {})
    for label in (settings.update_label, settings.restart_label, settings.recreate_label, settings.health_label):
        if label in attributes:
            return True
    return False


def start_event_listener(client: DockerClient, settings: Settings, wake_signal: Event) -> None:
    def _run() -> None:
        while True:
            try:
                for event in client.events(decode=True):
                    if not isinstance(event, dict):
                        continue
                    if not is_monitored_event(event, settings):
                        continue
                    action = event.get("Action")
                    container_id = event.get("id") or ""
                    short_id = container_id.split(":")[-1][:12] if container_id else "unknown"
                    attributes = event.get("Actor", {}).get("Attributes", {})
                    name = attributes.get("name") or attributes.get("container") or attributes.get("com.docker.compose.service")
                    display = name or short_id
                    raw_name = display.split("/")[-1] if display else short_id
                    base_name = _strip_guerite_suffix(raw_name)
                    current_time = now_tz(settings.timezone)
                    # Skip events we likely triggered ourselves within cooldown
                    if not _action_allowed(base_name, current_time, settings):
                        LOG.debug("Ignoring event %s for %s (%s); in cooldown", action, display, short_id)
                        continue
                    LOG.info("Docker event %s for %s (%s); waking up", action, display, short_id)
                    wake_signal.set()
            except DockerException as error:
                LOG.warning("Event stream error: %s", error)
                sleep(5)

    thread = Thread(target=_run, daemon=True)
    thread.start()


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    client = build_client(settings)
    LOG.info("Starting Guerite")
    wake_signal = Event()
    start_event_listener(client, settings, wake_signal)
    logged_schedule = False
    hostname = settings.hostname
    current_reason_name: Optional[str] = None
    current_reason_label: Optional[str] = None
    current_reason_source: Optional[str] = "startup"
    while True:
        timestamp = now_tz(settings.timezone)
        containers = select_monitored_containers(client, settings)
        if current_reason_source is not None:
            if current_reason_source == "docker_event":
                LOG.info("Running checks due to docker event")
            elif current_reason_name is not None or current_reason_label is not None:
                LOG.info(
                    "Running checks for %s",
                    _format_reason(current_reason_name, current_reason_label),
                )
            else:
                LOG.info("Running checks (unspecified trigger)")
            current_reason_source = None
        if not logged_schedule:
            summary = schedule_summary(containers, settings, reference=timestamp)
            prune_next = next_prune_time(settings, reference=timestamp)
            if prune_next is not None:
                summary.append(_format_human_local(prune_next, timestamp) + " (prune)")
            if summary:
                LOG.info("Upcoming checks: %s", "; ".join(summary))
                if "startup" in settings.notifications:
                    notify_pushover(settings, f"Guerite on {hostname}", "Starting Guerite, checks scheduled for:\n" + "\n".join(summary))
            else:
                LOG.info("No upcoming checks found")
            logged_schedule = True
        run_once(client, settings, timestamp=timestamp, containers=containers)
        next_run_at, next_name, next_label = next_wakeup(containers, settings, reference=timestamp)
        delta_seconds = (next_run_at - now_tz(settings.timezone)).total_seconds()
        sleep_seconds = max(1, int(ceil(delta_seconds)))
        LOG.info(
            "Next check at %s (in %ss) for %s",
            next_run_at.isoformat(),
            sleep_seconds,
            _format_reason(next_name, next_label),
        )
        woke = wake_signal.wait(timeout=sleep_seconds)
        if woke:
            wake_signal.clear()
            LOG.debug("Woken early by Docker event")
            current_reason_name = None
            current_reason_label = None
            current_reason_source = "docker_event"
        else:
            current_reason_name = next_name
            current_reason_label = next_label
            current_reason_source = "schedule"


if __name__ == "__main__":
    main()
