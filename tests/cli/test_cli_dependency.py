import os

import pytest
from click.testing import CliRunner
from jinja2.exceptions import TemplateNotFound

from bigquery_etl.dependency import show as dependency_show


class TestDependency:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_format_invalid_path(self, runner):
        result = runner.invoke(dependency_show, ["not-existing-path.sql"])
        assert result.exit_code == 1
        assert isinstance(result.exception, TemplateNotFound)

    def test_format(self, runner):
        with runner.isolated_filesystem():
            with open("foo.sql", "w") as f:
                f.write("SELECT 1 FROM test")

            result = runner.invoke(dependency_show, ["foo.sql"])
            assert "foo.sql: test\n" == result.output
            assert result.exit_code == 0

            os.mkdir("test")
            with open("test/foo.sql", "w") as f:
                f.write("SELECT 1 FROM test_foo")
            with open("test/bar.sql", "w") as f:
                f.write("SELECT 1 FROM test_bar")

            result = runner.invoke(dependency_show, ["test"])
            assert "test/bar.sql: test_bar\ntest/foo.sql: test_foo\n" == result.output
            assert result.exit_code == 0
