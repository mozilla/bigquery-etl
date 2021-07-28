import os
import pytest
from click.testing import CliRunner

from bigquery_etl.cli.view import publish


@pytest.mark.integration
class TestPublish:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_publish_invalid_view(self, runner):
        with runner.isolated_filesystem():
            path = "sql/moz-fx-data-shared-prod/test/sample_view"
            os.makedirs(path)
            with open(path + "/view.sql", "w") as f:
                f.write("SELECT 1")

            result = runner.invoke(publish, [path, "--dry-run"])
            assert result.exit_code == 1

    def test_publish_valid_view(self, runner):
        # In order to be agnostic with respect to individual projects in GCP,
        # we'll try to dry-run a query with a resource that certainly should not
        # exist.
        with runner.isolated_filesystem():
            path = "sql/moz-fx-data-shared-prod/test/test"
            os.makedirs(path)
            with open(path + "/view.sql", "w") as f:
                f.write(
                    """
                CREATE OR REPLACE VIEW test.test.test AS
                SELECT 42 as test
            """
                )

            result = runner.invoke(publish, [path, "--dry-run"])
            assert result.exit_code == 1
