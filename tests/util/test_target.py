import re
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from bigquery_etl.util.target import (
    MANIFEST_FILENAME,
    SCHEMA_FILE,
    Target,
    _normalize_table_ref,
    _substitute_3part_ref,
    _target_ref_for_source,
    collect_target_dependencies,
    extract_commit_from_dataset_name,
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
            target_files = prepare_target_files(
                query_files=[query_file],
                sql_dir=str(sql_dir),
                project_id="moz-fx-data-shared-prod",
                target=target,
                defer_to_target=False,
                isolated=False,
                auto_deploy=False,
            )

            assert len(target_files) == 1
            # artifact.project_id = sanitize("moz-fx-data-shared-prod") = "moz_fx_data_shared_prod"
            assert (
                target_files[0].parent.name
                == "moz_fx_data_shared_prod_clients_daily_v6"
            )


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

        assert regex.match("dev_feature_xyz_abc1234_moz_fx_data_shared_prod_telemetry")
        assert not regex.match(
            "dev_other_branch_abc1234_moz_fx_data_shared_prod_telemetry"
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

        assert regex.match("dev_feature_xyz_abc1234_telemetry")
        assert regex.match("dev_main_def4567_telemetry")

    def test_dataset_anchored_end(self):
        """dataset (not dataset_prefix) produces a $-anchored pattern."""
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset="dev_{{ git.branch }}_{{ git.commit }}",
        )
        pattern = render_dataset_pattern(target, branch="feature-xyz")
        regex = re.compile(pattern)

        # Commit starts with a short-SHA-length hex prefix; trailing alphanumeric
        # segments from legacy template variants are tolerated.
        assert regex.match("dev_feature_xyz_abc1234")
        assert regex.match("dev_feature_xyz_abc1234_extra_stuff")
        assert regex.match("dev_feature_xyz_f379269e_json")
        # Substring branch must not over-match a longer branch's dataset
        assert not regex.match("dev_feature_xyz_branch_rename_abc1234")

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

        assert regex.match("feature_xyz_abc1234_clients_daily_v6")
        assert not regex.match("other_branch_abc1234_clients_daily_v6")

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


@pytest.mark.usefixtures("mock_account")
class TestExtractCommitFromDatasetName:
    """Tests for extract_commit_from_dataset_name used by target migrate-branch."""

    def test_extracts_from_dataset_prefix(self):
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset_prefix="dev_{{ git.branch }}_{{ git.commit }}_{{ artifact.project_id }}_",
        )
        commit = extract_commit_from_dataset_name(
            target,
            "dev_feature_xyz_abc1234_moz_fx_data_shared_prod_telemetry",
            branch="feature-xyz",
        )
        assert commit == "abc1234"

    def test_extracts_full_sha(self):
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset="dev_{{ git.branch }}_{{ git.commit }}",
        )
        commit = extract_commit_from_dataset_name(
            target,
            "dev_feature_xyz_235bf9cf0ac1ef0fa3a127cdc5f0061a588dd819",
            branch="feature-xyz",
        )
        assert commit == "235bf9cf0ac1ef0fa3a127cdc5f0061a588dd819"

    def test_extracts_legacy_trailing_segment(self):
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset="dev_{{ git.branch }}_{{ git.commit }}",
        )
        commit = extract_commit_from_dataset_name(
            target, "dev_feature_xyz_f379269e_json", branch="feature-xyz"
        )
        assert commit == "f379269e_json"

    def test_returns_none_without_commit_in_template(self):
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset_prefix="dev_{{ git.branch }}_",
        )
        assert (
            extract_commit_from_dataset_name(
                target, "dev_feature_xyz_telemetry", branch="feature-xyz"
            )
            is None
        )

    def test_returns_none_when_name_does_not_match(self):
        target = Target(
            name="dev",
            project_id="test-project",
            raw_dataset="dev_{{ git.branch }}_{{ git.commit }}",
        )
        assert (
            extract_commit_from_dataset_name(
                target, "completely_unrelated", branch="feature-xyz"
            )
            is None
        )


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


class TestTargetRefForSource:
    def test_dataset_prefix_template(self):
        target = Target(
            name="dev",
            project_id="dev-proj",
            dataset_prefix="user_main_{{ artifact.project_id }}_",
        )
        proj, ds, tbl = _target_ref_for_source(
            target, "dev-proj", "moz-fx-data-shared-prod", "telemetry_derived", "tbl_v1"
        )
        assert proj == "dev-proj"
        assert ds == "user_main_moz_fx_data_shared_prod_telemetry_derived"
        assert tbl == "tbl_v1"

    def test_dataset_field(self):
        target = Target(name="dev", project_id="dev-proj", dataset="anna_dev")
        proj, ds, tbl = _target_ref_for_source(
            target, "dev-proj", "moz-fx-data-shared-prod", "telemetry_derived", "tbl_v1"
        )
        assert (proj, ds, tbl) == ("dev-proj", "anna_dev", "tbl_v1")

    def test_artifact_prefix_applied(self):
        target = Target(
            name="dev",
            project_id="dev-proj",
            dataset="anna_dev",
            artifact_prefix="branch_{{ artifact.dataset_id }}_",
        )
        _, _, tbl = _target_ref_for_source(
            target, "dev-proj", "moz-fx-data-shared-prod", "telemetry_derived", "tbl_v1"
        )
        assert tbl == "branch_telemetry_derived_tbl_v1"

    def test_no_dataset_or_prefix_passthrough(self):
        target = Target(name="dev", project_id="dev-proj")
        proj, ds, tbl = _target_ref_for_source(
            target, "dev-proj", "moz-fx-data-shared-prod", "telemetry_derived", "tbl_v1"
        )
        assert (proj, ds, tbl) == ("dev-proj", "telemetry_derived", "tbl_v1")


class TestSubstitute3PartRef:
    """Test _substitute_3part_ref handles common ref forms."""

    def test_fully_backticked(self):
        sql = "SELECT * FROM `proj`.`ds`.`tbl`"
        out = _substitute_3part_ref(sql, ("proj", "ds", "tbl"), ("p2", "d2", "t2"))
        assert "`p2`.`d2`.`t2`" in out

    def test_word_boundary_blocks_partial_match(self):
        """`foo` shouldn't match the start of `foobar`."""
        sql = "SELECT * FROM proj.ds.tbl_other JOIN proj.ds.tbl ON 1=1"
        out = _substitute_3part_ref(sql, ("proj", "ds", "tbl"), ("p2", "d2", "t2"))
        assert "proj.ds.tbl_other" in out
        assert "`p2`.`d2`.`t2`" in out

    def test_project_with_hyphen(self):
        sql = "SELECT * FROM `moz-fx-data-shared-prod`.telemetry.main"
        out = _substitute_3part_ref(
            sql,
            ("moz-fx-data-shared-prod", "telemetry", "main"),
            ("ascholtz-dev", "test_ds", "test_tbl"),
        )
        assert "`ascholtz-dev`.`test_ds`.`test_tbl`" in out


class TestNormalizeTableRef:
    def test_information_schema_dataset_prepends_project(self):
        # `dataset.INFORMATION_SCHEMA.TABLES` lacks a project → use default,
        # then collapse the trailing INFORMATION_SCHEMA pseudo-path.
        assert _normalize_table_ref(
            "telemetry_derived.INFORMATION_SCHEMA.TABLES", "default-proj"
        ) == ("default-proj", "telemetry_derived", "INFORMATION_SCHEMA.TABLES")

    def test_information_schema_4part_collapsed(self):
        # `proj.dataset.INFORMATION_SCHEMA.TABLES` → keep project & dataset,
        # collapse INFORMATION_SCHEMA.* into the table position.
        assert _normalize_table_ref(
            "proj.telemetry_derived.INFORMATION_SCHEMA.TABLES", "default"
        ) == ("proj", "telemetry_derived", "INFORMATION_SCHEMA.TABLES")

    def test_invalid_returns_none(self):
        assert _normalize_table_ref("just_one_part", "default") is None
        assert _normalize_table_ref("a.b", "default") is None


class TestCollectTargetDependencies:
    """Integration test: build a fixture sql/ tree and check stub placement."""

    def _make_sql_tree(self, sql_dir: Path) -> Path:
        """Set up a small tree where artifact references an unmanaged table."""
        artifact_dir = sql_dir / "src-proj" / "src_ds" / "view_a"
        artifact_dir.mkdir(parents=True)
        (artifact_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `src-proj.src_ds.view_a` AS\n"
            "SELECT * FROM `src-proj.unmanaged_ds.unmanaged_tbl`"
        )
        return artifact_dir / "view.sql"

    @patch("bigquery_etl.util.target._fetch_stub_schema")
    def test_stub_lands_at_target_path(self, mock_fetch_schema, tmp_path):
        artifact_view = self._make_sql_tree(tmp_path)
        target = Target(
            name="dev",
            project_id="dev-proj",
            dataset="dev_ds",
            artifact_prefix="prefix_",
        )

        # Don't actually hit BigQuery for the schema; just verify the
        # caller's path computation.
        deps = collect_target_dependencies({artifact_view}, str(tmp_path), target)

        # Expected stub path: <sql_dir>/dev-proj/dev_ds/prefix_unmanaged_tbl/query.py
        expected_stub = (
            tmp_path / "dev-proj" / "dev_ds" / "prefix_unmanaged_tbl" / "query.py"
        )
        assert expected_stub in deps
        assert expected_stub.exists()
        assert expected_stub.read_text().startswith("# Table stub generated")
        # _fetch_stub_schema should have been called with the right out_path
        mock_fetch_schema.assert_called_once()
        out_path_arg = mock_fetch_schema.call_args.args[3]
        assert out_path_arg.parent.name == "prefix_unmanaged_tbl"

    @patch("bigquery_etl.util.target._fetch_stub_schema")
    def test_managed_dep_is_followed_not_stubbed(self, mock_fetch_schema, tmp_path):
        # The view references a managed table that exists in sql/.
        view_dir = tmp_path / "src-proj" / "ds" / "view_a"
        view_dir.mkdir(parents=True)
        (view_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `src-proj.ds.view_a` AS\n"
            "SELECT * FROM `src-proj.ds.tbl_managed`"
        )
        managed_dir = tmp_path / "src-proj" / "ds" / "tbl_managed"
        managed_dir.mkdir(parents=True)
        (managed_dir / "query.sql").write_text("SELECT 1")
        (managed_dir / "schema.yaml").write_text("fields: []")

        target = Target(name="dev", project_id="dev-proj", dataset="dev_ds")
        deps = collect_target_dependencies(
            {view_dir / "view.sql"}, str(tmp_path), target
        )

        # Managed dep returned as its source path; no stub written.
        assert managed_dir / "query.sql" in deps
        mock_fetch_schema.assert_not_called()


class TestStripMaterializedView:
    def test_strips_create_clause(self, tmp_path):
        from bigquery_etl.cli.deploy import _strip_materialized_view

        mv_dir = tmp_path / "p" / "ds" / "tbl"
        mv_dir.mkdir(parents=True)
        mv_file = mv_dir / "materialized_view.sql"
        mv_file.write_text(
            "CREATE MATERIALIZED VIEW `p.ds.tbl`\n"
            "OPTIONS(refresh_interval_minutes=10) AS\n"
            "SELECT a, b FROM `p.other.tbl`"
        )
        new_path = _strip_materialized_view(mv_file)
        assert new_path.name == "query.sql"
        assert not mv_file.exists()
        assert "CREATE MATERIALIZED VIEW" not in new_path.read_text()
        assert "SELECT a, b FROM" in new_path.read_text()


class TestRewriteTestsForTarget:
    def test_renames_files_in_target_subtree(self, tmp_path):
        from bigquery_etl.cli.deploy import _rewrite_tests_for_target

        # Source artifact + its tests
        source_artifact = tmp_path / "sql" / "src-p" / "src_ds" / "tbl" / "query.sql"
        source_artifact.parent.mkdir(parents=True)
        source_artifact.write_text("SELECT 1")
        src_test_dir = tmp_path / "tests" / "sql" / "src-p" / "src_ds" / "tbl"
        src_test_dir.mkdir(parents=True)
        # Files that encode the source identity in their basename: full
        # `<proj>.<ds>.<tbl>` form and the `.schema` variant.
        (src_test_dir / "src-p.src_ds.tbl.yaml").write_text("rows: []")
        (src_test_dir / "src-p.src_ds.tbl.schema.yaml").write_text("fields: []")

        # Target path mapping
        target_artifact = tmp_path / "sql" / "tgt-p" / "tgt_ds" / "tbl" / "query.sql"
        target_artifact.parent.mkdir(parents=True)
        target_artifact.write_text("SELECT 1")

        _rewrite_tests_for_target(
            {source_artifact: target_artifact},
            tmp_path / "tests" / "sql",
        )

        tgt_test_dir = tmp_path / "tests" / "sql" / "tgt-p" / "tgt_ds" / "tbl"
        assert (tgt_test_dir / "tgt-p.tgt_ds.tbl.yaml").exists()
        assert (tgt_test_dir / "tgt-p.tgt_ds.tbl.schema.yaml").exists()
        # Source files preserved at their original location.
        assert (src_test_dir / "src-p.src_ds.tbl.yaml").exists()


class TestResolveIsolatedSchema:
    """Test the 3-step fallback chain."""

    def _setup(self, tmp_path):
        target_dir = tmp_path / "tgt-p" / "tgt_ds" / "tbl"
        target_dir.mkdir(parents=True)
        target_file = target_dir / "query.sql"
        target_file.write_text("SELECT 1")
        metadata_file = tmp_path / "src-p" / "src_ds" / "tbl" / "metadata.yaml"
        metadata_file.parent.mkdir(parents=True)
        return target_file, metadata_file

    def test_existing_schema_kept(self, tmp_path):
        from bigquery_etl.cli.deploy import _resolve_isolated_schema

        target_file, metadata_file = self._setup(tmp_path)
        target_schema = target_file.parent / SCHEMA_FILE
        target_schema.write_text("fields:\n  - name: existing_field\n    type: INT64\n")

        # Should keep the existing schema unchanged.
        _resolve_isolated_schema(
            target_file=target_file,
            artifact_metadata_path=metadata_file,
            source_project="src-p",
            source_dataset="src_ds",
            source_table="tbl",
            sql_dir=str(tmp_path),
        )
        assert "existing_field" in target_schema.read_text()

    @patch("bigquery_etl.cli.deploy.bigquery.Client")
    def test_falls_back_to_get_table(self, mock_client_cls, tmp_path):
        from bigquery_etl.cli.deploy import _resolve_isolated_schema

        target_file, metadata_file = self._setup(tmp_path)
        # No existing schema → falls through to step 2.

        # Mock the BQ client return.
        mock_table = mock_client_cls.return_value.get_table.return_value
        mock_table.schema = []

        _resolve_isolated_schema(
            target_file=target_file,
            artifact_metadata_path=metadata_file,
            source_project="src-p",
            source_dataset="src_ds",
            source_table="tbl",
            sql_dir=str(tmp_path),
        )

        target_schema = target_file.parent / SCHEMA_FILE
        assert target_schema.exists()
        mock_client_cls.return_value.get_table.assert_called_once_with(
            "src-p.src_ds.tbl"
        )
