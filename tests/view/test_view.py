from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from click.testing import CliRunner
from click.utils import strip_ansi
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField

from bigquery_etl.cli.view import _collect_views, _list_managed_views
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

    @pytest.fixture
    def schema_view(self, tmp_path):
        """A view with a checked-in schema.yaml."""
        path = tmp_path / "moz-fx-data-test-project" / "test" / "schema_view"
        path.mkdir(parents=True)
        (path / "view.sql").write_text(
            "CREATE OR REPLACE VIEW\n"
            "  `moz-fx-data-test-project.test.schema_view`\n"
            "AS\n"
            "SELECT\n"
            "  1 AS a\n"
        )
        (path / "schema.yaml").write_text(
            yaml.dump(
                {
                    "fields": [
                        {
                            "name": "a",
                            "type": "INTEGER",
                            "description": "The letter a.",
                        }
                    ]
                }
            )
        )
        return View.from_file(path / "view.sql")

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
        """A view matching BigQuery in every compared attribute should report no changes."""
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", simple_view.content)
        deployed_view.schema = [SchemaField("a", "INT")]
        deployed_view.labels = {}
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

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_changed_schema(self, mock_client, schema_view, capsys):
        """A schema.yaml whose fields differ from the deployed view should report a change."""
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", schema_view.content)
        deployed_view.schema = [
            SchemaField("a", "INTEGER"),
            SchemaField("b", "INTEGER"),
        ]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view

        assert schema_view.has_changes()
        assert "schema" in capsys.readouterr().out

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_matching_schema(self, mock_client, schema_view):
        """A schema.yaml matching the deployed view should report no change."""
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", schema_view.content)
        deployed_view.schema = [
            SchemaField.from_api_repr(
                {
                    "name": "a",
                    "type": "INTEGER",
                    "mode": "NULLABLE",
                    "description": "The letter a.",
                }
            )
        ]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view

        assert not schema_view.has_changes()

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_missing_field_description(
        self, mock_client, schema_view, capsys
    ):
        """A description in schema.yaml absent from BigQuery should report a change."""
        # This is how the schemas written by `generate derived_view_schemas`
        # reach BigQuery, so skipping these views would drop descriptions.
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", schema_view.content)
        # from_api_repr, not the SchemaField constructor: a field parsed from a
        # real API response with no description omits the key entirely, which
        # is what lets the merge fill it in.
        deployed_view.schema = [
            SchemaField.from_api_repr(
                {"name": "a", "type": "INTEGER", "mode": "NULLABLE"}
            )
        ]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view

        assert schema_view.has_changes()
        assert "field descriptions" in capsys.readouterr().out

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_differing_field_description(
        self, mock_client, schema_view, capsys
    ):
        """A description differing from BigQuery's should report a change."""
        # CREATE OR REPLACE VIEW clears every column description, so publish
        # always reapplies schema.yaml's descriptions to the recreated view.
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", schema_view.content)
        deployed_view.schema = [
            SchemaField.from_api_repr(
                {
                    "name": "a",
                    "type": "INTEGER",
                    "mode": "NULLABLE",
                    "description": "stale",
                }
            )
        ]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view

        assert schema_view.has_changes()
        assert "field descriptions" in capsys.readouterr().out

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_description_removed_from_schema_yaml(
        self, mock_client, tmp_path, capsys
    ):
        """A description dropped from schema.yaml should report a change, since publish clears it."""
        path = tmp_path / "moz-fx-data-test-project" / "test" / "schema_view"
        path.mkdir(parents=True)
        (path / "view.sql").write_text(
            "CREATE OR REPLACE VIEW\n"
            "  `moz-fx-data-test-project.test.schema_view`\n"
            "AS\nSELECT\n  1 AS a\n"
        )
        (path / "schema.yaml").write_text(
            yaml.dump({"fields": [{"name": "a", "type": "INTEGER"}]})
        )
        view = View.from_file(path / "view.sql")

        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", view.content)
        deployed_view.schema = [
            SchemaField.from_api_repr(
                {
                    "name": "a",
                    "type": "INTEGER",
                    "mode": "NULLABLE",
                    "description": "will be cleared",
                }
            )
        ]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view

        assert view.has_changes()
        assert "field descriptions" in capsys.readouterr().out

    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_labels_without_metadata(
        self, mock_client, simple_view, capsys
    ):
        """A label differing on a view with no metadata.yaml should report a change."""
        # publish writes labels whether or not metadata.yaml exists, so this
        # check must not be nested under the metadata comparison.
        simple_view.labels["managed"] = ""
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", simple_view.content)
        deployed_view.schema = [SchemaField("a", "INT")]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view

        assert simple_view.metadata is None
        assert simple_view.has_changes()
        assert "labels" in capsys.readouterr().out

    @patch("bigquery_etl.dryrun.DryRun")
    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_upstream_drift_without_schema_file(
        self, mock_client, mock_dryrun, simple_view, capsys
    ):
        """A view with no schema.yaml should dry run to detect upstream drift."""
        # BigQuery caches a view's schema, so an upstream column addition leaves
        # the deployed view stale until it is recreated. The dry run is the only
        # way to see the schema the query produces now.
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", simple_view.content)
        deployed_view.schema = [SchemaField("a", "INT")]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view
        mock_dryrun.return_value.get_schema.return_value = {
            "fields": [{"name": "a", "type": "INT"}, {"name": "b", "type": "INT"}]
        }

        assert simple_view.has_changes()
        assert "schema" in capsys.readouterr().out

    @patch("bigquery_etl.dryrun.DryRun")
    @patch("google.cloud.bigquery.Client")
    def test_view_has_changes_when_dry_run_fails(
        self, mock_client, mock_dryrun, simple_view, capsys
    ):
        """A view whose dry run fails should report a change rather than be skipped."""
        # Failing toward republish keeps a flaky dry run from silently leaving a
        # stale schema deployed.
        deployed_view = Mock()
        deployed_view.view_query = CREATE_VIEW_PATTERN.sub("", simple_view.content)
        deployed_view.schema = [SchemaField("a", "INT")]
        deployed_view.labels = {}
        mock_client.return_value.get_table.return_value = deployed_view
        mock_dryrun.return_value.get_schema.side_effect = RuntimeError("boom")

        assert simple_view.has_changes()
        assert "could not dry run" in capsys.readouterr().out

    @patch("bigquery_etl.view.Schema.from_query_file")
    def test_dryrun_schema_uses_supplied_client_instead_of_cloud_function(
        self, mock_from_query_file, simple_view
    ):
        """A supplied client should make the dry run go direct to BigQuery."""
        client = Mock()
        simple_view._dryrun_schema(client=client, use_cloud_function=False)
        kwargs = mock_from_query_file.call_args.kwargs
        assert kwargs["client"] is client
        assert kwargs["use_cloud_function"] is False

    @patch("bigquery_etl.view.Schema.from_query_file")
    def test_dryrun_schema_without_client_uses_cloud_function(
        self, mock_from_query_file, simple_view
    ):
        """The default should leave the dry run on the cloud function."""
        simple_view._dryrun_schema()
        kwargs = mock_from_query_file.call_args.kwargs
        assert kwargs["client"] is None
        assert kwargs["use_cloud_function"] is True

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


class TestListManagedViews:
    """Region-wide managed-view listing used by `bqetl view clean`."""

    @staticmethod
    def _row(table_id, table_schema, is_authorized=False):
        row = Mock()
        row.table_id = table_id
        row.table_schema = table_schema
        row.is_authorized = is_authorized
        return row

    def _client(self, rows):
        client = Mock()
        client.query.return_value.result.return_value = rows
        return client

    @property
    def rows(self):
        return [
            self._row("p.telemetry.a", "telemetry"),
            self._row("p.telemetry_derived.b", "telemetry_derived"),
            self._row("p.telemetry.c", "telemetry", is_authorized=True),
        ]

    def test_queries_the_region_once(self):
        """Listing should issue one region-scoped query instead of one per dataset."""
        client = self._client(self.rows)
        _list_managed_views(client, "my-project", "us", None, False)

        assert client.query.call_count == 1
        sql = client.query.call_args[0][0]
        assert "`my-project.region-us.INFORMATION_SCHEMA.VIEWS`" in sql
        assert "`my-project.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`" in sql
        assert 'STRUCT("managed", "")' in sql

    def test_region_is_configurable(self):
        """The region option should select which region-qualified schema is queried."""
        client = self._client([])
        _list_managed_views(client, "my-project", "eu", None, False)
        assert "region-eu" in client.query.call_args[0][0]

    def test_skip_authorized(self):
        """skip_authorized should exclude views labeled authorized."""
        result = _list_managed_views(self._client(self.rows), "p", "us", None, True)
        assert result == {"p.telemetry.a", "p.telemetry_derived.b"}

    def test_authorized_only(self):
        """authorized_only should return only views labeled authorized."""
        result = _list_managed_views(
            self._client(self.rows), "p", "us", None, False, authorized_only=True
        )
        assert result == {"p.telemetry.c"}

    def test_user_facing_only_excludes_suffixed_datasets(self):
        """user_facing_only should exclude views in datasets with a non-user-facing suffix."""
        result = _list_managed_views(
            self._client(self.rows), "p", "us", None, True, user_facing_only=True
        )
        assert result == {"p.telemetry.a"}

    def test_no_filters_returns_everything(self):
        """No filters should return every managed view."""
        result = _list_managed_views(self._client(self.rows), "p", "us", None, False)
        assert len(result) == 3

    def test_name_pattern_filters(self):
        """A name pattern should filter to matching views."""
        # Regression: this path used to raise AttributeError via sql_table_id.
        result = _list_managed_views(
            self._client(self.rows), "p", "us", "telemetry.a", False
        )
        assert result == {"p.telemetry.a"}
