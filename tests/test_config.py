from pathlib import Path
from unittest.mock import patch

from bigquery_etl.config import ConfigLoader, _ConfigLoader

TEST_DIR = Path(__file__).parent


class TestConfig:
    def test_config_loader_set_project(self):
        ConfigLoader.set_project_dir(TEST_DIR / "data")
        assert ConfigLoader.project_dir == TEST_DIR / "data"

    def test_config_loader_set_config_file(self):
        config_loader = _ConfigLoader()
        config_loader.set_config_file("config.yaml")

        with patch.object(config_loader, "get", return_value=None):
            assert config_loader.config_file == "config.yaml"

    def test_config_loader_get(self):
        ConfigLoader.set_project_dir(TEST_DIR / "data")

        assert "function" in ConfigLoader.get("dry_run")
        assert "skip" in ConfigLoader.get("dry_run")

        assert (
            ConfigLoader.get("default", "test_project")
            == "bigquery-etl-integration-test"
        )

        assert len(ConfigLoader.get("dry_run", "skip")) > 0

        assert "dry_run" in ConfigLoader.get()

    def test_config_loader_get_non_existing(self):
        ConfigLoader.set_project_dir(TEST_DIR / "data")

        assert ConfigLoader.get("non_existing") is None
        assert ConfigLoader.get("dry_run", "non_existing") is None
        assert ConfigLoader.get("non_existing", fallback=[]) == []
        assert ConfigLoader.get("dry_run", "foo", fallback=123) == 123
