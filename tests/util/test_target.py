import re
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
    render_artifact_prefix_pattern,
    render_dataset_pattern,
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
    @patch("bigquery_etl.util.target._artifact_exists", return_value=True)
    def test_get_deployed_tables_reads_manifest(
        self, _mock_artifact_exists, _mock_client
    ):
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


@pytest.fixture
def mock_account(monkeypatch):
    """Patch _get_account_context to avoid real GCP auth."""
    monkeypatch.setattr(
        "bigquery_etl.util.target._get_account_context",
        lambda: {"username": "testuser"},
    )


@pytest.mark.usefixtures("mock_account")
class TestRenderDatasetPattern:
    """Tests for render_dataset_pattern used by target clean."""

    def test_dataset_prefix_with_branch(self):
        """Pattern with --branch pins branch literally, wildcards the rest."""
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset_prefix="dev_{{ git.branch }}_{{ git.commit }}_{{ artifact.project_id }}_",
        )
        pattern = render_dataset_pattern(target, branch="feature/xyz")
        regex = re.compile(pattern)

        assert regex.match("dev_feature_xyz_abc123_moz_fx_data_shared_prod_telemetry")
        assert not regex.match(
            "dev_other_branch_abc123_moz_fx_data_shared_prod_telemetry"
        )

    def test_dataset_prefix_without_branch(self):
        """Pattern without --branch wildcards everything."""
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset_prefix="dev_{{ git.branch }}_{{ git.commit }}_",
        )
        pattern = render_dataset_pattern(target)
        regex = re.compile(pattern)

        assert regex.match("dev_feature_xyz_abc123_telemetry")
        assert regex.match("dev_main_def456_telemetry")

    def test_dataset_anchored_end(self):
        """dataset (not dataset_prefix) produces a $-anchored pattern."""
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset="dev_{{ git.branch }}_{{ git.commit }}",
        )
        pattern = render_dataset_pattern(target, branch="feature-xyz")
        regex = re.compile(pattern)

        # Commit wildcard matches any alphanumeric+underscore suffix
        assert regex.match("dev_feature_xyz_abc123")
        assert regex.match("dev_feature_xyz_abc123_extra_stuff")

    def test_username_rendered_literally(self):
        """account.username is rendered with real value, not wildcarded."""
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset_prefix="dev_{{ account.username }}_{{ git.branch }}_",
        )
        pattern = render_dataset_pattern(target, branch="main")

        assert "testuser" in pattern
        assert re.compile(pattern).match("dev_testuser_main_telemetry")
        assert not re.compile(pattern).match("dev_otheruser_main_telemetry")


@pytest.mark.usefixtures("mock_account")
class TestRenderArtifactPrefixPattern:
    """Tests for render_artifact_prefix_pattern used by target clean."""

    def test_with_branch(self):
        target = Target(
            name="dev",
            project_id="test-project",
            raw_artifact_prefix="{{ git.branch }}_{{ git.commit }}_",
        )
        pattern = render_artifact_prefix_pattern(target, branch="feature-xyz")
        regex = re.compile(pattern)

        assert regex.match("feature_xyz_abc123_clients_daily_v6")
        assert not regex.match("other_branch_abc123_clients_daily_v6")

    def test_returns_none_without_artifact_prefix(self):
        target = Target(name="dev", project_id="test-project")
        assert render_artifact_prefix_pattern(target, branch="main") is None

    def test_no_branch_in_template(self):
        """When artifact_prefix doesn't use git.branch, branch param is ignored."""
        target = Target(
            name="dev",
            project_id="test-project",
            raw_artifact_prefix="{{ artifact.project_id }}_{{ account.username }}_",
        )
        pattern = render_artifact_prefix_pattern(target, branch="feature-xyz")
        regex = re.compile(pattern)

        assert regex.match("moz_fx_data_shared_prod_testuser_clients_daily_v6")
        assert not regex.match("moz_fx_data_shared_prod_otheruser_clients_daily_v6")


@pytest.fixture
def mock_git(monkeypatch):
    """Patch _get_git_context to avoid real git calls."""
    monkeypatch.setattr(
        "bigquery_etl.util.target._get_git_context",
        lambda: {"branch": "main", "commit": "abc123de"},
    )


@pytest.fixture
def mock_config_loader(monkeypatch, tmp_path):
    """Patch ConfigLoader to use a temp directory for targets file."""
    monkeypatch.setattr(
        "bigquery_etl.util.target.ConfigLoader.get",
        lambda *a, **kw: "bqetl_targets.yaml",
    )
    monkeypatch.setattr("bigquery_etl.util.target.ConfigLoader.project_dir", tmp_path)
    return tmp_path


@pytest.mark.usefixtures("mock_account", "mock_git")
class TestGetTargetRawTemplates:
    """Test that get_target preserves raw (unrendered) template strings."""

    def test_raw_fields_preserved(self, mock_config_loader):
        targets_file = mock_config_loader / "bqetl_targets.yaml"
        targets_file.write_text(
            "dev:\n"
            "  project_id: test-project\n"
            "  dataset: dev_{{ git.branch }}_{{ git.commit }}\n"
            "  artifact_prefix: test_{{ artifact.project_id }}_{{ account.username }}_\n"
        )

        target = get_target("dev")

        # Rendered fields have git/account vars resolved
        assert target.dataset == "dev_main_abc123de"
        assert "artifact.project_id" in target.artifact_prefix

        # Raw fields preserve the original templates
        assert "git.branch" in target.raw_dataset
        assert "git.commit" in target.raw_dataset
        assert "artifact.project_id" in target.raw_artifact_prefix
        assert target.raw_dataset_prefix is None

    def test_raw_dataset_prefix(self, mock_config_loader):
        targets_file = mock_config_loader / "bqetl_targets.yaml"
        targets_file.write_text(
            "dev:\n"
            "  project_id: test-project\n"
            "  dataset_prefix: dev_{{ git.branch }}_{{ git.commit }}_\n"
        )

        target = get_target("dev")

        assert target.dataset_prefix == "dev_main_abc123de_"
        assert "git.branch" in target.raw_dataset_prefix
        assert target.raw_dataset is None
        assert target.raw_artifact_prefix is None
