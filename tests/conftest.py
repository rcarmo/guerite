from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Callable, Iterable, Optional

import pytest

import guerite.monitor as monitor
from guerite.config import Settings


class DummyImage:
    def __init__(self, image_id: str = "old", tags: Optional[list[str]] = None):
        self.id = image_id
        self.tags = tags if tags is not None else ["repo:tag"]


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
        stop_raises: bool = False,
        remove_raises: Optional[Exception] = None,
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
        self.stop_raises = stop_raises
        self.remove_raises = remove_raises

    def stop(self):
        if self.stop_raises:
            raise monitor.DockerException("stop failed")
        self.stopped = True

    def start(self):
        self.started = True

    def remove(self):
        if self.remove_raises is not None:
            raise self.remove_raises
        self.removed = True


class DummyAPI:
    def __init__(self):
        self.calls: list = []
        self.fail_connect = False
        self.raise_priority_type_error = False
        self.prune_images_result: Optional[dict] = None
        self.prune_images_error: Optional[Exception] = None
        self.endpoint_kwargs: dict = {}
        self.remove_failures_remaining: int = 0
        self.exec_exit_code: int = 0

    def rename(self, cid, name):
        self.calls.append(("rename", cid, name))

    def create_container(self, **kwargs):
        self.calls.append(("create_container", kwargs))
        return {"Id": "new-id"}

    def create_endpoint_config(self, **kwargs):
        if self.raise_priority_type_error and "priority" in kwargs:
            raise TypeError("unexpected priority")
        self.endpoint_kwargs = kwargs
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
        if self.remove_failures_remaining > 0:
            self.remove_failures_remaining -= 1
            raise monitor.DockerException("remove failed")

    def prune_images(self, **kwargs):
        if self.prune_images_error:
            raise self.prune_images_error
        return self.prune_images_result or {}

    def exec_create(self, *args, **kwargs):
        self.calls.append(("exec_create", args, kwargs))
        return {"Id": "exec-id"}

    def exec_start(self, *args, **kwargs):
        self.calls.append(("exec_start", args, kwargs))
        return None

    def exec_inspect(self, *args, **kwargs):
        self.calls.append(("exec_inspect", args, kwargs))
        return {"ExitCode": self.exec_exit_code}


class DummyImages:
    def __init__(self):
        self.calls = []
        self.fail_remove = False

    def remove(self, image):
        if self.fail_remove:
            raise monitor.DockerException("boom")
        self.calls.append(("remove", image))


class FakeResponse:
    def __init__(self, status: int = 200, reason: str = "OK"):
        self.status = status
        self.reason = reason


class FakeConnection:
    def __init__(self, status: int = 200, reason: str = "OK"):
        self.calls: list = []
        self.status = status
        self.reason = reason

    def request(self, method, path, body=None, headers=None):
        self.calls.append((method, path, body, headers))

    def getresponse(self):
        return FakeResponse(self.status, self.reason)

    def close(self):
        self.calls.append("closed")


class DummyClient:
    def __init__(self, containers_list: Optional[Iterable] = None):
        self.api = DummyAPI()
        self.images = DummyImages()
        self.containers = SimpleNamespace(list=lambda all=True: list(containers_list or []))
        self.events_iter: Iterable = iter([])

    def events(self, decode: bool = True):
        yield from self.events_iter


@pytest.fixture(autouse=True)
def reset_monitor_state():
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    monitor._KNOWN_CONTAINERS.clear()
    monitor._KNOWN_CONTAINER_NAMES.clear()
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
    monitor._KNOWN_CONTAINER_NAMES.clear()
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
        health_check_timeout_seconds=60,
        prune_timeout_seconds=None,
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
        monitor_only=False,
        no_pull=False,
        no_restart=False,
        monitor_only_label="guerite.monitor_only",
        no_pull_label="guerite.no_pull",
        no_restart_label="guerite.no_restart",
        scope_label="guerite.scope",
        scope=None,
        include_containers=set(),
        exclude_containers=set(),
        rolling_restart=False,
        stop_timeout_seconds=None,
        lifecycle_hooks_enabled=False,
        hook_timeout_seconds=60,
        pre_check_label="guerite.lifecycle.pre_check",
        pre_update_label="guerite.lifecycle.pre_update",
        post_update_label="guerite.lifecycle.post_update",
        post_check_label="guerite.lifecycle.post_check",
        pre_update_timeout_label="guerite.lifecycle.pre_update_timeout_seconds",
        post_update_timeout_label="guerite.lifecycle.post_update_timeout_seconds",
        http_api_enabled=False,
        http_api_host="0.0.0.0",
        http_api_port=8080,
        http_api_token=None,
        http_api_metrics=False,
        run_once=False,
    )


@pytest.fixture
def notifier_settings(settings: Settings) -> Settings:
    data = settings.__dict__ | {
        "pushover_token": "token",
        "pushover_user": "user",
        "pushover_api": "https://api.example/endpoint",
        "webhook_url": "https://hook.example/hit",
    }
    return Settings(**data)


@pytest.fixture
def restart_settings(tmp_path, settings: Settings) -> Settings:
    data = settings.__dict__ | {"state_file": str(tmp_path / "state.json")}
    return Settings(**data)


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


@pytest.fixture
def fake_connection() -> Callable[..., FakeConnection]:
    def _make(status: int = 200, reason: str = "OK"):
        return FakeConnection(status=status, reason=reason)
    return _make
