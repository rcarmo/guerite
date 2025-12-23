from datetime import timedelta
from logging import getLogger
from math import ceil
from socket import gethostname
from threading import Event, Thread
from time import sleep

from docker import DockerClient
from docker.errors import DockerException

from .config import Settings, load_settings
from .monitor import next_prune_time, next_wakeup, run_once, schedule_summary, select_monitored_containers
from .notifier import notify_pushover
from .utils import configure_logging, now_tz

LOG = getLogger(__name__)


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
    for label in (settings.update_label, settings.restart_label, settings.health_label):
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
                    LOG.info("Docker event %s on %s; waking up", event.get("Action"), event.get("id"))
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
    hostname = gethostname()
    while True:
        timestamp = now_tz(settings.timezone)
        containers = select_monitored_containers(client, settings)
        if not logged_schedule:
            summary = schedule_summary(containers, settings, reference=timestamp)
            prune_next = next_prune_time(settings, reference=timestamp)
            if prune_next is not None:
                summary.append(_format_human_local(prune_next, timestamp) + " (prune)")
            if summary:
                LOG.info("Upcoming checks: %s", "; ".join(summary))
                if "startup" in settings.notifications:
                    notify_pushover(settings, f"Guerite on {hostname}", "Next checks:\n" + "\n".join(summary))
            else:
                LOG.info("No upcoming checks found")
            logged_schedule = True
        run_once(client, settings, timestamp=timestamp, containers=containers)
        next_run_at = next_wakeup(containers, settings, reference=timestamp)
        delta_seconds = (next_run_at - now_tz(settings.timezone)).total_seconds()
        sleep_seconds = max(1, int(ceil(delta_seconds)))
        LOG.info("Next check at %s (in %ss)", next_run_at.isoformat(), sleep_seconds)
        woke = wake_signal.wait(timeout=sleep_seconds)
        if woke:
            wake_signal.clear()
            LOG.debug("Woken early by Docker event")


if __name__ == "__main__":
    main()


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
