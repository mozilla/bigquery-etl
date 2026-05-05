from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from click.utils import strip_ansi
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField

from bigquery_etl.cli.view import _collect_views
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.view import CREATE_VIEW_PATTERN, View

TEST_DIR = Path(__file__).parent.parent


class TestView:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def simple_view(self):
        return View.from_file(
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "simple_view"
            / "view.sql"
        )

    @pytest.fixture
    def metadata_view(self):
        return View.from_file(
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "view_with_metadata"
            / "view.sql"
        )

    def test_from_file(self, simple_view):
        assert simple_view.dataset == "test"
        assert simple_view.project == "moz-fx-data-test-project"
        assert simple_view.name == "simple_view"
        assert (
            simple_view.view_identifier == "moz-fx-data-test-project.test.simple_view"
        )
        assert simple_view.is_user_facing

    def test_view_create(self, runner):
        with runner.isolated_filesystem():
            view = View.create("moz-fx-data-test-project", "test", "simple_view", "sql")
            assert view.path == Path(
                "sql/moz-fx-data-test-project/test/simple_view/view.sql"
            )
            assert "CREATE OR REPLACE VIEW" in view.content
            assert "`moz-fx-data-test-project.test.simple_view`" in view.content
            assert Path(
                "sql/moz-fx-data-test-project/test/dataset_metadata.yaml"
            ).exists()
            assert Path(
                "sql/moz-fx-data-test-project/test/simple_view/view.sql"
            ).exists()

            view = View.create(
                "moz-fx-data-test-project",
                "test",
                "simple_view",
                "sql",
                base_table="moz-fx-data-test-project.test.some_table",
            )
            assert "`moz-fx-data-test-project.test.some_table`" in view.content

    def test_view_invalid_path(self):
        with pytest.raises(ValueError):
            View(path="test", name="test", dataset="test", project="test")

    def test_view_valid(self, runner):
        with runner.isolated_filesystem():
            view = View.create("moz-fx-data-test-project", "test", "view", "sql")
            assert view.is_valid()

    def test_view_invalid(self, runner):
        with runner.isolated_filesystem():
            view = View.create("moz-fx-data-test-project", "test", "view", "sql")
            assert view.is_valid()

            view.path.write_text("CREATE OR REPLACE VIEW test.view AS SELECT 1")
            assert view.is_valid() is False

            view.path.write_text("SELECT 1")
            assert view.is_valid() is False

            view.path.write_text(
                "CREATE OR REPLACE VIEW `moz-fx-data-test-project.foo.bar` AS SELECT 1"
            )
            assert view.is_valid() is False

    def test_view_do_not_publish_invalid(self, runner):
        with runner.isolated_filesystem():
            view = View.create("moz-fx-data-test-project", "test", "view", "sql")
            assert view.is_valid()
            view.path.write_text("SELECT 1")
            assert view.is_valid() is False
            assert view.publish() is False

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_publish_valid_view(
        self, mock_bigquery_table, mock_bigquery_client, metadata_view
    ):
        mock_bigquery_client().get_table.return_value = mock_bigquery_table()

        assert metadata_view.is_valid()
        assert metadata_view.publish()
        # update_table is called once for metadata/labels (view query is handled by CREATE OR REPLACE VIEW)
        assert mock_bigquery_client().update_table.call_count == 1
        # Check the call has metadata/labels update
        assert (
            mock_bigquery_client().update_table.call_args[0][0].friendly_name
            == "Test metadata file"
        )
        assert (
            mock_bigquery_client().update_table.call_args[0][0].description
            == "Test description"
        )
        assert mock_bigquery_client().update_table.call_args[0][0].labels == {
            "123-432": "valid",
            "1232341234": "valid",
            "1234_abcd": "valid",
            "incremental": "",
            "incremental_export": "",
            "number_string": "1234abcde",
            "number_value": "1234234",
            "owner1": "test1",
            "owner2": "test2",
            "public_json": "",
            "schedule": "daily",
        }
        assert (
            mock_bigquery_client()
            .get_table("moz-fx-data-test-project.test.view_with_metadata")
            .friendly_name
            == "Test metadata file"
        )
        assert mock_bigquery_table().friendly_name == "Test metadata file"
        assert mock_bigquery_table().description == "Test description"

    @patch("bigquery_etl.dryrun.DryRun")
    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_no_changes(self, mock_client, mock_dryrun, simple_view):
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", simple_view.content)
        deployed_view.schema = [SchemaField("a", "INT")]
        mock_client.return_value.get_table.return_value = deployed_view
        mock_dryrun.return_value.get_schema.return_value = {
            "fields": [{"name": "a", "type": "INT"}]
        }

        assert not simple_view.has_changes()

    def test_view_has_changes_non_matching_project(self, simple_view):
        assert not simple_view.has_changes(target_project="other-project")

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_new_view(self, mock_client, simple_view, capsys):
        mock_client.return_value.get_table.side_effect = NotFound("")

        assert simple_view.has_changes()
        assert "does not exist" in capsys.readouterr().out

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_changed_defn(self, mock_client, simple_view, capsys):
        deployed_view = Mock()
        deployed_view.view_query = """
        CREATE OR REPLACE VIEW
          `moz-fx-data-test-project.test.simple_view`
        AS
        SELECT
          99
        """
        mock_client.return_value.get_table.return_value = deployed_view

        assert simple_view.has_changes()
        assert "query" in capsys.readouterr().out

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_changed_metadata(
        self, mock_client, metadata_view, capsys
    ):
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", metadata_view.content)
        deployed_view.description = metadata_view.metadata.description
        deployed_view.friendly_name = metadata_view.metadata.friendly_name + "123"
        mock_client.return_value.get_table.return_value = deployed_view

        assert metadata_view.has_changes()
        assert "friendly_name" in capsys.readouterr().out

    @patch("bigquery_etl.dryrun.DryRun")
    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_changed_schema(
        self, mock_client, mock_dryrun, simple_view, capsys
    ):
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", simple_view.content)
        deployed_view.schema = [SchemaField("a", "INT")]
        mock_client.return_value.get_table.return_value = deployed_view
        mock_dryrun.return_value.get_schema.return_value = {
            "fields": [{"name": "a", "type": "INT"}, {"name": "b", "type": "INT"}]
        }

        assert simple_view.has_changes()
        assert "schema" in capsys.readouterr().out

    @patch("bigquery_etl.cli.view.get_id_token")
    def test_collect_views_authorized_only(self, mock_get_id_token, runner):
        """Test that authorized_only flag filters views correctly."""
        mock_get_id_token.return_value = None

        with runner.isolated_filesystem():
            # Create test directory structure
            Path("sql/moz-fx-data-shared-prod/test").mkdir(parents=True)

            # Create an authorized view
            authorized_view_dir = Path(
                "sql/moz-fx-data-shared-prod/test/authorized_view"
            )
            authorized_view_dir.mkdir()
            (authorized_view_dir / "view.sql").write_text(
                "CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.test.authorized_view` AS SELECT 1"
            )
            authorized_metadata = Metadata(
                friendly_name="Authorized View",
                description="Test authorized view",
                owners=["test@mozilla.com"],
                labels={"authorized": True},
            )
            authorized_metadata.write(authorized_view_dir / "metadata.yaml")

            # Create a non-authorized view
            regular_view_dir = Path("sql/moz-fx-data-shared-prod/test/regular_view")
            regular_view_dir.mkdir()
            (regular_view_dir / "view.sql").write_text(
                "CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.test.regular_view` AS SELECT 1"
            )
            regular_metadata = Metadata(
                friendly_name="Regular View",
                description="Test regular view",
                owners=["test@mozilla.com"],
            )
            regular_metadata.write(regular_view_dir / "metadata.yaml")

            # Test authorized_only=True
            views = _collect_views(
                name=None,
                sql_dir="sql",
                project_id="moz-fx-data-shared-prod",
                user_facing_only=False,
                skip_authorized=False,
                authorized_only=True,
            )
            assert len(views) == 1
            assert views[0].name == "authorized_view"

            # Test skip_authorized=True
            views = _collect_views(
                name=None,
                sql_dir="sql",
                project_id="moz-fx-data-shared-prod",
                user_facing_only=False,
                skip_authorized=True,
                authorized_only=False,
            )
            assert len(views) == 1
            assert views[0].name == "regular_view"

            # Test both flags False (get all views)
            views = _collect_views(
                name=None,
                sql_dir="sql",
                project_id="moz-fx-data-shared-prod",
                user_facing_only=False,
                skip_authorized=False,
                authorized_only=False,
            )
            assert len(views) == 2

    def test_publish_authorized_only_mutually_exclusive(self, runner):
        """Test that --authorized-only and --skip-authorized are mutually exclusive."""
        from bigquery_etl.cli.view import publish

        with runner.isolated_filesystem():
            Path("sql/moz-fx-data-shared-prod/test").mkdir(parents=True)

            result = runner.invoke(
                publish,
                [
                    "--authorized-only",
                    "--skip-authorized",
                    "--project-id=moz-fx-data-shared-prod",
                ],
            )
            assert result.exit_code != 0
            assert (
                "Cannot use both --skip-authorized and --authorized-only"
                in strip_ansi(result.output)
            )

    def test_clean_authorized_only_mutually_exclusive(self, runner):
        """Test that --authorized-only and --skip-authorized are mutually exclusive in clean."""
        from bigquery_etl.cli.view import clean

        with runner.isolated_filesystem():
            with pytest.raises(AssertionError):
                result = runner.invoke(
                    clean,
                    [
                        "--authorized-only",
                        "--skip-authorized",
                        "--target-project=moz-fx-data-shared-prod",
                    ],
                )
                assert result.exit_code != 0
                assert (
                    "Cannot use both --skip-authorized and --authorized-only"
                    in strip_ansi(result.output)
                )
