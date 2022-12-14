from pathlib import Path

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

    @pytest.mark.java
    def test_view_valid(self, runner):
        with runner.isolated_filesystem():
            view = View.create("moz-fx-data-test-project", "test", "view", "sql")
            assert view.is_valid()

    @pytest.mark.java
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

    @pytest.mark.java
    def test_view_do_not_publish_invalid(self, runner):
        with runner.isolated_filesystem():
            view = View.create("moz-fx-data-test-project", "test", "view", "sql")
            assert view.is_valid()
            view.path.write_text("SELECT 1")
            assert view.is_valid() is False
            assert view.publish() is False
