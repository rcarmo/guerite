"""Tests for recent bug fixes and enhancements."""

from guerite import monitor
from tests.conftest import DummyContainer, DummyImage


class TestNormalizeLinks:
    """Test _normalize_links_value function."""

    def test_none_returns_none(self):
        assert monitor._normalize_links_value(None) is None

    def test_false_returns_none(self):
        assert monitor._normalize_links_value(False) is None

    def test_dict_returns_as_is(self):
        links = {"container": "alias"}
        assert monitor._normalize_links_value(links) == {"container": "alias"}

    def test_list_of_strings_with_colon_converts_to_dict(self):
        links = ["container:alias", "db:database"]
        result = monitor._normalize_links_value(links)
        assert result == {"container": "alias", "db": "database"}

    def test_list_of_strings_without_colon_uses_name_as_alias(self):
        links = ["container", "db"]
        result = monitor._normalize_links_value(links)
        assert result == {"container": "container", "db": "db"}

    def test_mixed_list_with_and_without_colons(self):
        links = ["container:alias", "db"]
        result = monitor._normalize_links_value(links)
        assert result == {"container": "alias", "db": "db"}

    def test_empty_list_returns_none(self):
        assert monitor._normalize_links_value([]) is None

    def test_tuple_converts_like_list(self):
        links = ("container:alias",)
        result = monitor._normalize_links_value(links)
        assert result == {"container": "alias"}

    def test_unknown_type_returns_none(self):
        assert monitor._normalize_links_value(123) is None


class TestImageDisplayName:
    """Test _image_display_name function."""

    def test_prefers_image_ref(self):
        container = DummyContainer("test")
        result = monitor._image_display_name(container=container, image_ref="nginx:latest")
        assert result == "nginx:latest"

    def test_uses_container_config_image(self):
        container = DummyContainer("test")
        container.attrs["Config"]["Image"] = "redis:alpine"
        result = monitor._image_display_name(container=container)
        assert result == "redis:alpine"

    def test_skips_sha256_config_image(self):
        container = DummyContainer("test")
        container.attrs["Config"]["Image"] = "sha256:abc123def456"
        container.image = DummyImage("abc123def456", tags=["postgres:15"])
        result = monitor._image_display_name(container=container)
        assert result == "postgres:15"

    def test_uses_image_tags_from_container(self):
        container = DummyContainer("test")
        container.attrs["Config"]["Image"] = "sha256:oldhash"  # Force fallback to tags
        container.image = DummyImage("img123", tags=["myapp:v1.0"])
        result = monitor._image_display_name(container=container)
        assert result == "myapp:v1.0"

    def test_falls_back_to_image_id(self):
        container = DummyContainer("test")
        container.image = DummyImage("sha256:abcdef123456", tags=[])
        container.attrs["Config"]["Image"] = "sha256:abcdef123456"
        result = monitor._image_display_name(container=container, image_id="sha256:abcdef123456")
        assert result == "abcdef123456"

    def test_returns_unknown_when_no_info(self):
        result = monitor._image_display_name()
        assert result == "unknown"

    def test_image_ref_overrides_all(self):
        container = DummyContainer("test")
        container.image = DummyImage("old", tags=["old:tag"])
        result = monitor._image_display_name(
            container=container,
            image_ref="new:tag",
            image_id="newid"
        )
        assert result == "new:tag"


class TestGetImageReference:
    """Test get_image_reference function."""

    def test_returns_first_tag_from_image(self):
        container = DummyContainer("test")
        container.image = DummyImage("img123", tags=["nginx:latest", "nginx:1.25"])
        result = monitor.get_image_reference(container)
        assert result == "nginx:latest"

    def test_falls_back_to_config_image(self):
        container = DummyContainer("test")
        container.image = DummyImage("img123", tags=[])
        container.attrs["Config"]["Image"] = "postgres:15"
        result = monitor.get_image_reference(container)
        assert result == "postgres:15"

    def test_skips_sha256_config_image(self):
        container = DummyContainer("test")
        container.image = DummyImage("img123", tags=[])
        container.attrs["Config"]["Image"] = "sha256:abc123"
        result = monitor.get_image_reference(container)
        assert result is None

    def test_handles_docker_exception(self, monkeypatch):
        container = DummyContainer("test")
        
        def raise_error(*args, **kwargs):
            raise monitor.DockerException("boom")
        
        monkeypatch.setattr(container, "image", property(lambda self: raise_error()))
        container.attrs["Config"]["Image"] = "fallback:tag"
        
        result = monitor.get_image_reference(container)
        assert result == "fallback:tag"

    def test_returns_none_when_no_valid_reference(self):
        container = DummyContainer("test")
        container.image = DummyImage("img123", tags=[])
        container.attrs["Config"]["Image"] = "sha256:nohash"
        result = monitor.get_image_reference(container)
        assert result is None


class TestGueriteCreatedTracking:
    """Test that containers created by Guerite don't trigger detection notifications."""

    def test_guerite_created_set_exists(self):
        assert hasattr(monitor, "_GUERITE_CREATED")
        assert isinstance(monitor._GUERITE_CREATED, set)

    def test_track_new_containers_skips_guerite_created(self):
        # Reset state
        monitor._KNOWN_CONTAINERS.clear()
        monitor._KNOWN_INITIALIZED = False
        monitor._PENDING_DETECTS.clear()
        monitor._GUERITE_CREATED.clear()

        # First call initializes
        container1 = DummyContainer("app1")
        monitor._track_new_containers([container1])
        assert monitor._KNOWN_INITIALIZED is True
        assert len(monitor._PENDING_DETECTS) == 0  # No detects on init

        # New container created by Guerite
        container2 = DummyContainer("app2")
        monitor._GUERITE_CREATED.add(container2.id)
        monitor._track_new_containers([container1, container2])
        
        # Should not be in pending detects
        assert len(monitor._PENDING_DETECTS) == 0
        assert container2.id not in monitor._GUERITE_CREATED  # Cleaned up

    def test_track_new_containers_detects_external_containers(self):
        # Reset state
        monitor._KNOWN_CONTAINERS.clear()
        monitor._KNOWN_CONTAINER_NAMES.clear()
        monitor._KNOWN_INITIALIZED = False
        monitor._PENDING_DETECTS.clear()
        monitor._GUERITE_CREATED.clear()

        # Initialize with one container
        container1 = DummyContainer("app1")
        monitor._track_new_containers([container1])

        # External container appears (not created by Guerite)
        container2 = DummyContainer("app2")
        monitor._track_new_containers([container1, container2])
        
        # Should be detected
        assert len(monitor._PENDING_DETECTS) == 1
        assert monitor._PENDING_DETECTS[0] == "app2"

    def test_track_new_containers_ignores_external_restarts(self):
        """Containers restarted externally (new ID, same name) should not trigger notifications."""
        # Reset state
        monitor._KNOWN_CONTAINERS.clear()
        monitor._KNOWN_CONTAINER_NAMES.clear()
        monitor._KNOWN_INITIALIZED = False
        monitor._PENDING_DETECTS.clear()
        monitor._GUERITE_CREATED.clear()

        # Initialize with one container
        container1 = DummyContainer("app1")
        original_id = container1.id
        monitor._track_new_containers([container1])
        assert "app1" in monitor._KNOWN_CONTAINER_NAMES
        assert original_id in monitor._KNOWN_CONTAINERS

        # Container restarted externally - same name, different ID
        container1_restarted = DummyContainer("app1")
        container1_restarted.id = "app1-id-new"
        monitor._track_new_containers([container1_restarted])
        
        # Should NOT be detected as new
        assert len(monitor._PENDING_DETECTS) == 0
        assert "app1" in monitor._KNOWN_CONTAINER_NAMES
        assert container1_restarted.id in monitor._KNOWN_CONTAINERS
