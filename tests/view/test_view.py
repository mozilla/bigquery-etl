from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from bigquery_etl.view import View

TEST_DIR = Path(__file__).parent.parent


class TestView:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_from_file(self):
        view_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "simple_view"
            / "view.sql"
        )

        view = View.from_file(view_file)
        assert view.dataset == "test"
        assert view.project == "moz-fx-data-test-project"
        assert view.name == "simple_view"
        assert view.view_identifier == "moz-fx-data-test-project.test.simple_view"
        assert view.is_user_facing

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
    def test_publish_valid_view(self, mock_bigquery_table, mock_bigquery_client):
        mock_bigquery_client().get_table.return_value = mock_bigquery_table()

        view = View.from_file(
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "view_with_metadata"
            / "view.sql"
        )

        assert view.is_valid()
        assert view.publish()
        assert mock_bigquery_client().update_table.call_count == 1
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

    def test_simple_views(self):
        view = View.from_file(
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "default"
            / "view.sql"
        )

        assert view.is_default_view
