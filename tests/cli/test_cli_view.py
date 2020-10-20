import pytest
from click.testing import CliRunner

from bigquery_etl.cli.view import view


class TestPublish:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_publish_invalid_view(self, runner, tmp_path):
        (tmp_path / "view.sql").write_text("SELECT 42 as test")
        result = runner.invoke(view, ["publish", tmp_path.as_posix(), "--dry-run"])
        assert result.exit_code == 1
        assert "does not appear to be a CREATE OR REPLACE VIEW" in result.output

    def test_publish_valid_view(self, runner, tmp_path):
        # In order to be agnostic with respect to individual projects in GCP,
        # we'll try to dry-run a query with a resource that certainly should not
        # exist.
        (tmp_path / "view.sql").write_text(
            """
            CREATE OR REPLACE VIEW test.test.test AS
            SELECT 42 as test
        """
        )
        result = runner.invoke(view, ["publish", tmp_path.as_posix(), "--dry-run"])
        assert result.exit_code == 1
        assert "Not found" in result.exc_info[1].message
