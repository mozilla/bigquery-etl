import os
import pytest
from click.testing import CliRunner

from bigquery_etl.cli.query import create, schedule


class TestQuery:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_invalid_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(create, ["test.query_v1", "--path=foo.txt"])
            assert result.exit_code == 1

    def test_create_invalid_query_name(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(create, ["invalid_query_name"])
            assert result.exit_code == 1

    def test_create_query(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert os.listdir("sql") == ["test"]
            assert os.listdir("sql/test") == ["test_query_v1"]
            assert os.listdir("sql/test/test_query_v1") == [
                "query.sql",
                "metadata.yaml",
            ]

    def test_create_query_with_version(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            result = runner.invoke(create, ["test.test_query_v4"])
            assert result.exit_code == 0
            assert os.listdir("sql/test") == ["test_query_v4"]

    def test_create_derived_query_with_view(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("sql/test_derived")
            result = runner.invoke(create, ["test_derived.test_query"])
            assert result.exit_code == 0
            assert os.listdir("sql") == ["test_derived", "test"]
            assert os.listdir("sql/test_derived") == ["test_query_v1"]
            assert os.listdir("sql/test") == ["test_query"]
            assert os.listdir("sql/test/test_query") == ["view.sql"]

    def test_create_query_as_derived(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("sql/test_derived")
            os.mkdir("sql/test")
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert os.listdir("sql") == ["test_derived", "test"]
            assert os.listdir("sql/test_derived") == ["test_query_v1"]
            assert os.listdir("sql/test") == ["test_query"]
            assert os.listdir("sql/test/test_query") == ["view.sql"]

    def test_create_query_with_init(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            result = runner.invoke(create, ["test.test_query", "--init=True"])
            assert result.exit_code == 0
            assert os.listdir("sql/test") == ["test_query_v1"]
            assert os.listdir("sql/test/test_query_v1") == [
                "query.sql",
                "metadata.yaml",
                "init.sql",
            ]
