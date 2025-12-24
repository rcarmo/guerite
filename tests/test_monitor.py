from datetime import datetime, timedelta, timezone

import pytest

from guerite.config import Settings
from guerite import monitor


class DummyContainer:
    def __init__(self, name: str, labels: dict | None = None, host_links: list[str] | None = None):
        self.name = name
        self.labels = labels or {}
        self.attrs = {
            "HostConfig": {"Links": host_links or []},
            "NetworkSettings": {},
        }


@pytest.fixture(autouse=True)
def reset_state():
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()
    yield
    monitor._LAST_ACTION.clear()
    monitor._IN_FLIGHT.clear()
    monitor._RESTART_BACKOFF.clear()
    monitor._HEALTH_BACKOFF.clear()
    monitor._RESTART_FAIL_COUNT.clear()


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
        state_file="/tmp/guerite_state_test.json",
        prune_cron=None,
        rollback_grace_seconds=3600,
        restart_retry_limit=3,
        depends_label="guerite.depends_on",
        action_cooldown_seconds=60,
    )


def test_strip_guerite_suffix_handles_nested():
    name = "app-guerite-old-deadbeef-guerite-new-12345678"
    assert monitor._strip_guerite_suffix(name) == "app"


def test_toposort_orders_dependencies():
    names = {"db", "app", "cache"}
    deps = {"app": {"db"}, "cache": {"db"}, "db": set()}
    ordered = monitor._toposort(names, deps)
    assert ordered[0] == "db"
    assert set(ordered) == names


def test_toposort_with_cycle_falls_back_sorted():
    names = {"a", "b"}
    deps = {"a": {"b"}, "b": {"a"}}
    ordered = monitor._toposort(names, deps)
    assert ordered == sorted(names)


def test_action_allowed_respects_cooldown_and_inflight(settings: Settings):
    base_name = "app"
    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._LAST_ACTION[base_name] = now - timedelta(seconds=settings.action_cooldown_seconds - 1)
    assert monitor._action_allowed(base_name, now, settings) is False
    monitor._LAST_ACTION[base_name] = now - timedelta(seconds=settings.action_cooldown_seconds + 1)
    assert monitor._action_allowed(base_name, now, settings) is True
    monitor._IN_FLIGHT.add(base_name)
    assert monitor._action_allowed(base_name, now, settings) is False


def test_restart_and_health_backoff(settings: Settings):
    container_id = "sha256:abc123def456"
    base_name = "app"
    now = datetime(2025, 12, 24, 12, 0, tzinfo=timezone.utc)
    monitor._RESTART_BACKOFF[container_id] = now + timedelta(seconds=10)
    assert monitor._restart_allowed(container_id, base_name, now, settings) is False
    monitor._RESTART_BACKOFF[container_id] = now - timedelta(seconds=1)
    assert monitor._restart_allowed(container_id, base_name, now, settings) is True
    monitor._HEALTH_BACKOFF[container_id] = now + timedelta(seconds=10)
    assert monitor._health_allowed(container_id, base_name, now, settings) is False
    monitor._HEALTH_BACKOFF[container_id] = now - timedelta(seconds=1)
    assert monitor._health_allowed(container_id, base_name, now, settings) is True


def test_order_by_compose_sorts_by_dependencies(settings: Settings):
    db = DummyContainer("db", labels={"com.docker.compose.project": "stack"})
    app = DummyContainer(
        "app",
        labels={"com.docker.compose.project": "stack", settings.depends_label: "db"},
    )
    cache = DummyContainer(
        "cache",
        labels={"com.docker.compose.project": "stack", settings.depends_label: "db"},
    )
    unordered = [app, cache, db]
    ordered = monitor._order_by_compose(unordered, settings)
    assert ordered[0].name == "db"
    assert {item.name for item in ordered} == {"app", "cache", "db"}