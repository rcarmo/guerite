from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Callable, Iterable, Optional

import pytest

import guerite.monitor as monitor
from guerite.config import Settings


class DummyImage:
    def __init__(self, image_id: str = "old"):
        self.id = image_id


class DummyContainer:
    def __init__(
        self,
        name: str,
        labels: Optional[dict] = None,
        health: Optional[str] = None,
        running: bool = True,
        healthcheck: bool = False,
        networks: Optional[dict] = None,
        host_links: Optional[list[str]] = None,
        image_id: str = "old",
    ):
        self.name = name
        self.id = f"{name}-id"
        self.labels = labels or {}
        health_state = {"Status": health} if health is not None else {}
        self.attrs = {
            "Config": {
                "Image": "repo:tag",
                "Healthcheck": {"Test": ["CMD-SHELL", "true"]} if healthcheck else None,
                "Labels": self.labels,
            },
            "HostConfig": {"Links": host_links or []},
            "NetworkSettings": {"Networks": networks or {}},
            "State": {"Running": running, "Health": health_state, "StartedAt": datetime.now(timezone.utc).isoformat()},
            "Mounts": [],
        }
        self.image = DummyImage(image_id)
        self.stopped = False
        self.started = False
        self.removed = False

    def stop(self):
        self.stopped = True

    def start(self):
        self.started = True

    def remove(self):
        self.removed = True


class DummyAPI:
    def __init__(self):
        self.calls: list = []
        self.fail_connect = False
        self.raise_priority_type_error = False
        self.prune_images_result: Optional[dict] = None
        self.prune_images_error: Optional[Exception] = None

    def rename(self, cid, name):
        self.calls.append(("rename", cid, name))

    def create_container(self, **kwargs):
        self.calls.append(("create_container", kwargs))
        return {"Id": "new-id"}

    def create_endpoint_config(self, **kwargs):
        if self.raise_priority_type_error and "priority" in kwargs:
            raise TypeError("unexpected priority")
        return {}

    def create_networking_config(self, *args, **kwargs):  # pragma: no cover
        return {}

    def connect_container_to_network(self, *args, **kwargs):
        if self.fail_connect:
            raise monitor.APIError("fail", None, None)  # type: ignore[arg-type]
        self.calls.append(("connect", args, kwargs))

    def start(self, cid):
        self.calls.append(("start", cid))

    def stop(self, cid):
        self.calls.append(("stop", cid))

    def remove_container(self, cid, force=False):
        self.calls.append(("remove", cid, force))

    def prune_images(self, **kwargs):
        if self.prune_images_error:
            raise self.prune_images_error
        return self.prune_images_result or {}


class DummyImages:
    def __init__(self):
        self.calls = []
        self.fail_remove = False

    def remove(self, image):
        if self.fail_remove:
            raise monitor.DockerException("boom")
        self.calls.append(("remove", image))


class DummyClient:
    def __init__(self, containers_list: Optional[Iterable] = None):
        self.api = DummyAPI()
        self.images = DummyImages()
        self.containers = SimpleNamespace(list=lambda all=True: list(containers_list or []))


@pytest.fixture(autouse=True)
def reset_monitor_state():
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    monitor._KNOWN_CONTAINERS.clear()
    monitor._KNOWN_INITIALIZED = False
    monitor._PENDING_DETECTS.clear()
    monitor._LAST_DETECT_NOTIFY = None
    monitor._HEALTH_BACKOFF_LOADED = False
    yield
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    monitor._KNOWN_CONTAINERS.clear()
    monitor._KNOWN_INITIALIZED = False
    monitor._PENDING_DETECTS.clear()
    monitor._LAST_DETECT_NOTIFY = None
    monitor._HEALTH_BACKOFF_LOADED = False


@pytest.fixture
def settings() -> Settings:
    return Settings(
        docker_host="unix://test",
        update_label="guerite.update",
        restart_label="guerite.restart",
        recreate_label="guerite.recreate",
        health_label="guerite.health_check",
        health_backoff_seconds=30,
        notifications={"update"},
        timezone="UTC",
        pushover_token=None,
        pushover_user=None,
        pushover_api="https://example",
        webhook_url=None,
        dry_run=False,
        log_level="INFO",
        state_file="/tmp/state",
        prune_cron=None,
        rollback_grace_seconds=3600,
        restart_retry_limit=3,
        depends_label="guerite.depends_on",
        action_cooldown_seconds=60,
    )


@pytest.fixture
def notifier_settings(settings: Settings) -> Settings:
    data = settings.__dict__ | {"pushover_token": "token", "pushover_user": "user", "webhook_url": "https://hook"}
    return Settings(**data)


@pytest.fixture
def restart_settings(settings: Settings) -> Settings:
    return settings


@pytest.fixture
def dummy_container() -> Callable[..., DummyContainer]:
    def _make(**kwargs):
        return DummyContainer(**kwargs)
    return _make


@pytest.fixture
def dummy_client() -> Callable[..., DummyClient]:
    def _make(containers_list: Optional[Iterable] = None):
        return DummyClient(containers_list=containers_list)
    return _make


@pytest.fixture
def fake_image():
    return DummyImage("new")
