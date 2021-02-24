import os

import pytest
from click.testing import CliRunner

from bigquery_etl.cli.format import format as sql_format


class TestFormat:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_format_invalid_path(self, runner):
        result = runner.invoke(sql_format, ["not-existing-path.sql"])
        assert result.exit_code == 1

    def test_format(self, runner):
        with runner.isolated_filesystem():
            with open("foo.sql", "w") as f:
                f.write("SELECT 1 FROM test")

            result = runner.invoke(sql_format, ["foo.sql"])
            assert "1 file reformatted." in result.output
            assert result.exit_code == 0

            os.mkdir("test")
            with open("test/foo.sql", "w") as f:
                f.write("SELECT 1 FROM test")
            with open("test/bar.sql", "w") as f:
                f.write("SELECT 1 FROM test")

            result = runner.invoke(sql_format, ["test"])
            assert "2 files reformatted." in result.output
            assert result.exit_code == 0
