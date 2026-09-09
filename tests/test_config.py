from pathlib import Path
from textwrap import dedent

from bigquery_etl.config import BQETL_PROJECT_CONFIG, _ConfigLoader

TEST_DIR = Path(__file__).parent


class TestConfig:
    def test_config_loader_get(self):
        config_loader = _ConfigLoader(TEST_DIR / "data" / "bqetl_project.yaml")

        assert (
            config_loader.get("default", "test_project")
            == "moz-fx-data-integration-tests"
        )

        assert "dry_run" in config_loader.get()
        assert "function" in config_loader.get("dry_run")
        assert "skip" in config_loader.get("dry_run")
        assert len(config_loader.get("dry_run", "skip")) == 2

    def test_config_loader_get_non_existing(self):
        config_loader = _ConfigLoader(TEST_DIR / "data" / "bqetl_project.yaml")

        assert config_loader.get("non_existing") is None
        assert config_loader.get("dry_run", "non_existing") is None
        assert config_loader.get("non_existing", fallback=[]) == []
        assert config_loader.get("dry_run", "foo", fallback=123) == 123

    def test_config_loader_multiple_files(self, monkeypatch, tmp_path):
        monkeypatch.setattr("bigquery_etl.config.ROOT", TEST_DIR / "data")
        monkeypatch.chdir(tmp_path)
        local_config_file = tmp_path / BQETL_PROJECT_CONFIG
        local_config_file.write_text(dedent("""
            default:
              another_setting: true

            dry_run:
              function: override
              skip:
              - sql/moz-fx-data-shared-prod/test_derived/yet_another_query_v1/query.sql

            another_section:
              foo: bar
        """))

        config_loader = _ConfigLoader()

        assert config_loader.config_files == [
            TEST_DIR / "data" / BQETL_PROJECT_CONFIG,
            local_config_file,
        ]

        assert (
            config_loader.get("default", "test_project")
            == "moz-fx-data-integration-tests"
        )
        assert config_loader.get("default", "another_setting") is True

        assert "dry_run" in config_loader.get()
        assert config_loader.get("dry_run", "function") == "override"
        assert "skip" in config_loader.get("dry_run")
        assert len(config_loader.get("dry_run", "skip")) == 3

        assert "another_section" in config_loader.get()
        assert config_loader.get("another_section", "foo") == "bar"
