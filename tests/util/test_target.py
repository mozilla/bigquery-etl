import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from bigquery_etl.util.target import (
    MANIFEST_FILENAME,
    Target,
    get_deployed_tables_in_target,
    get_target,
    prepare_target_directory,
    prepare_target_files,
)


class TestTarget:
    def test_target_creation(self):
        """Test creating a Target instance."""
        target = Target(
            name="test", project_id="test-project", dataset_prefix="test_prefix_"
        )
        assert target.name == "test"
        assert target.project_id == "test-project"
        assert target.dataset_prefix == "test_prefix_"

    def test_target_optional_prefix(self):
        """Test Target with no dataset_prefix."""
        target = Target(name="test", project_id="test-project")
        assert target.name == "test"
        assert target.project_id == "test-project"
        assert target.dataset_prefix is None

    def test_target_with_dataset(self):
        """Test Target with full dataset name."""
        target = Target(name="test", project_id="test-project", dataset="anna_dev")
        assert target.dataset == "anna_dev"
        assert target.dataset_prefix is None
        assert target.artifact_prefix is None

    def test_target_with_artifact_prefix(self):
        """Test Target with artifact_prefix."""
        target = Target(
            name="test",
            project_id="test-project",
            dataset="anna_dev",
            artifact_prefix="feature_",
        )
        assert target.dataset == "anna_dev"
        assert target.artifact_prefix == "feature_"

    def test_target_dataset_and_dataset_prefix_mutually_exclusive(self):
        """Test that dataset and dataset_prefix cannot both be set."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            Target(
                name="test",
                project_id="test-project",
                dataset="anna_dev",
                dataset_prefix="prefix_",
            )

    @patch(
        "bigquery_etl.util.target._get_account_context",
        return_value={"username": "testuser"},
    )
    @patch(
        "bigquery_etl.util.target._get_git_context",
        return_value={"branch": "main", "commit": "abc123de"},
    )
    @patch("bigquery_etl.util.target.ConfigLoader")
    def test_get_target_success(self, mock_config_loader, _mock_git, _mock_account):
        """Test successfully getting a target with git info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            targets_file = tmpdir_path / "bqetl_targets.yaml"
            targets_file.write_text("""
dev:
  project_id: test-project-dev
  dataset_prefix: dev_{{ git.branch }}_{{ git.commit }}_
prod:
  project_id: test-project-prod
""")

            mock_config_loader.get.return_value = "bqetl_targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            target = get_target("dev")

            assert target.name == "dev"
            assert target.project_id == "test-project-dev"
            assert target.dataset_prefix == "dev_main_abc123de_"

    @patch(
        "bigquery_etl.util.target._get_account_context",
        return_value={"username": "unknown"},
    )
    @patch(
        "bigquery_etl.util.target._get_git_context",
        return_value={"branch": "unknown", "commit": "unknown"},
    )
    @patch("bigquery_etl.util.target.ConfigLoader")
    def test_get_target_no_git_repo(self, mock_config_loader, _mock_git, _mock_account):
        """Test getting a target when not in a git repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            targets_file = tmpdir_path / "bqetl_targets.yaml"
            targets_file.write_text("""
dev:
  project_id: test-project-dev
  dataset_prefix: dev_{{ git.branch }}_{{ git.commit }}_
""")

            mock_config_loader.get.return_value = "bqetl_targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            target = get_target("dev")

            assert target.name == "dev"
            assert target.project_id == "test-project-dev"
            assert target.dataset_prefix == "dev_unknown_unknown_"

    @patch("bigquery_etl.util.target.ConfigLoader")
    def test_get_target_not_found(self, mock_config_loader):
        """Test getting a target that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            targets_file = tmpdir_path / "bqetl_targets.yaml"
            targets_file.write_text("""
dev:
  project_id: test-project-dev
""")

            mock_config_loader.get.return_value = "bqetl_targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            with pytest.raises(Exception, match="Couldn't find target `nonexistent`"):
                get_target("nonexistent")

    @patch("bigquery_etl.util.target.ConfigLoader")
    def test_get_target_file_not_found(self, mock_config_loader):
        """Test getting a target when targets file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            mock_config_loader.get.return_value = "bqetl_targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            with pytest.raises(Exception, match="Targets file not found"):
                get_target("dev")


class TestPrepareTargetDirectory:
    def _make_query_file(
        self, sql_dir: Path, project: str, dataset: str, table: str
    ) -> Path:
        query_dir = sql_dir / project / dataset / table
        query_dir.mkdir(parents=True)
        query_file = query_dir / "query.sql"
        query_file.write_text("SELECT 1")
        return query_file

    def test_dataset_field_overrides_source_dataset(self):
        """dataset places artifact in the named dataset regardless of source dataset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_dir = Path(tmpdir) / "sql"
            query_file = self._make_query_file(
                sql_dir,
                "moz-fx-data-shared-prod",
                "telemetry_derived",
                "clients_daily_v6",
            )

            result = prepare_target_directory(
                query_file,
                str(sql_dir),
                Target(name="test", project_id="my-dev-project", dataset="anna_dev"),
                defer_to_target=False,
                isolated=False,
            )

            assert result.parent.parent.name == "anna_dev"
            assert result.parent.name == "clients_daily_v6"
            assert result.parent.parent.parent.name == "my-dev-project"

    def test_artifact_prefix_prepended_to_table_name(self):
        """artifact_prefix is prepended to the artifact name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_dir = Path(tmpdir) / "sql"
            query_file = self._make_query_file(
                sql_dir,
                "moz-fx-data-shared-prod",
                "telemetry_derived",
                "clients_daily_v6",
            )

            result = prepare_target_directory(
                query_file,
                str(sql_dir),
                Target(
                    name="test",
                    project_id="my-dev-project",
                    dataset="anna_dev",
                    artifact_prefix="feature_",
                ),
                defer_to_target=False,
                isolated=False,
            )

            assert result.parent.name == "feature_clients_daily_v6"
            assert result.parent.parent.name == "anna_dev"

    def test_manifest_written_with_source_info(self):
        """Manifest file is written with original source location."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_dir = Path(tmpdir) / "sql"
            query_file = self._make_query_file(
                sql_dir,
                "moz-fx-data-shared-prod",
                "telemetry_derived",
                "clients_daily_v6",
            )

            result = prepare_target_directory(
                query_file,
                str(sql_dir),
                Target(
                    name="test",
                    project_id="my-dev-project",
                    dataset="anna_dev",
                    artifact_prefix="feature_",
                ),
                defer_to_target=False,
                isolated=False,
            )

            manifest_file = result.parent / MANIFEST_FILENAME
            assert manifest_file.exists()
            manifest = yaml.safe_load(manifest_file.read_text())
            assert manifest["source_project"] == "moz-fx-data-shared-prod"
            assert manifest["source_dataset"] == "telemetry_derived"
            assert manifest["source_table"] == "clients_daily_v6"

    @patch("bigquery_etl.util.target.bigquery.Client")
    @patch("bigquery_etl.util.target._table_exists", return_value=True)
    def test_get_deployed_tables_reads_manifest(self, _mock_table_exists, _mock_client):
        """get_deployed_tables_in_target reads manifest for source info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_dir = Path(tmpdir) / "sql"
            query_file = self._make_query_file(
                sql_dir,
                "moz-fx-data-shared-prod",
                "telemetry_derived",
                "clients_daily_v6",
            )

            prepare_target_directory(
                query_file,
                str(sql_dir),
                Target(
                    name="test",
                    project_id="my-dev-project",
                    dataset="anna_dev",
                    artifact_prefix="feature_",
                ),
                defer_to_target=False,
                isolated=False,
            )

            deployed = get_deployed_tables_in_target(str(sql_dir), "my-dev-project")
            assert len(deployed) == 1
            info = next(iter(deployed))
            assert info.target_project == "my-dev-project"
            assert info.target_dataset == "anna_dev"
            assert info.target_table == "feature_clients_daily_v6"
            assert info.source_dataset == "telemetry_derived"
            assert info.source_table == "clients_daily_v6"


class TestPrepareTargetFiles:
    def _make_query_file(
        self, sql_dir: Path, project: str, dataset: str, table: str
    ) -> Path:
        query_dir = sql_dir / project / dataset / table
        query_dir.mkdir(parents=True)
        query_file = query_dir / "query.sql"
        query_file.write_text("SELECT 1")
        return query_file

    def test_artifact_prefix_artifact_project_id_rendered(self):
        """{{ artifact.project_id }} in artifact_prefix is rendered from source project."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_dir = Path(tmpdir) / "sql"
            query_file = self._make_query_file(
                sql_dir,
                "moz-fx-data-shared-prod",
                "telemetry_derived",
                "clients_daily_v6",
            )

            target = Target(
                name="test",
                project_id="my-dev-project",
                dataset="anna_dev",
                artifact_prefix="{{ artifact.project_id }}_",
            )
            result = prepare_target_files(
                query_files=[query_file],
                sql_dir=str(sql_dir),
                project_id="moz-fx-data-shared-prod",
                target=target,
                defer_to_target=False,
                isolated=False,
                auto_deploy=False,
            )

            assert len(result) == 1
            # artifact.project_id = sanitize("moz-fx-data-shared-prod") = "moz_fx_data_shared_prod"
            assert result[0].parent.name == "moz_fx_data_shared_prod_clients_daily_v6"
