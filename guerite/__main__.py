from logging import getLogger
from threading import Event, Thread
from time import sleep

from docker import DockerClient
from docker.errors import DockerException

from .config import Settings, load_settings
from .monitor import next_wakeup, run_once, select_monitored_containers
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
    attributes = event.get("Actor", {}).get("Attributes", {})
    return settings.cron_label in attributes


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
    while True:
        timestamp = now_tz(settings.timezone)
        containers = select_monitored_containers(client, settings)
        run_once(client, settings, timestamp=timestamp, containers=containers)
        next_run_at = next_wakeup(containers, settings, reference=timestamp)
        sleep_seconds = max(1, int((next_run_at - now_tz(settings.timezone)).total_seconds()))
        LOG.info("Next check at %s (in %ss)", next_run_at.isoformat(), sleep_seconds)
        woke = wake_signal.wait(timeout=sleep_seconds)
        if woke:
            wake_signal.clear()
            LOG.debug("Woken early by Docker event")


if __name__ == "__main__":
    main()
