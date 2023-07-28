import subprocess
from textwrap import dedent

import pytest
from click.testing import CliRunner

from bigquery_etl.cli.check import _build_jinja_parameters, _parse_check_output


class TestCheck:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_parse_check_output(self):
        expected = "ETL Data Check Failed: a check failed"
        assert _parse_check_output(expected) == expected

        test2 = "remove prepended text ETL Data Check Failed: a check failed"
        assert _parse_check_output(test2) == expected

        test3 = "no match for text Data Check Failed: a check failed"
        assert _parse_check_output(test3) == test3

    def test_build_jinja_parameters(self):
        test = [
            "--parameter=submission_date::2023-06-01",
            "--parameter=id::asdf",
            "--use_legacy_sql=false",
            "--project_id=moz-fx-data-marketing-prod",
            "--debug",
        ]
        expected = {
            "submission_date": "2023-06-01",
            "id": "asdf",
            "use_legacy_sql": "false",
            "project_id": "moz-fx-data-marketing-prod",
        }
        assert _build_jinja_parameters(test) == expected

    # TODO: Keep getting file not found error, not clear what's causing this
    # def test_check_run(self, runner):
    #     result = runner.invoke(
    #         run,
    #         [
    #             "--project_id=moz-fx-data-shared-prod",
    #             "--sql-dir=tests/sql",
    #             "telemetry_derived.clients_daily_v6",
    #             "--dry-run",
    #         ],
    #     )

    #     assert result.exit_code == 0

    def test_check_render(self):
        cmd = [
            "./bqetl",
            "check",
            "render",
            "--project_id=moz-fx-data-shared-prod",
            "--sql-dir=tests/sql",
            "telemetry_derived.clients_daily_v6",
            "--parameter=submission_date:DATE:2023-07-01",
        ]
        result = subprocess.check_output(cmd)

        output = "".join(result.decode().partition("ASSERT")[1:])
        assert output == dedent(
            """\
        ASSERT(
          (
            SELECT
              COUNT(*)
            FROM
              `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
            WHERE
              submission_date = "2023-07-01"
          ) > 0
        )
        AS
          'ETL Data Check Failed: Table moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6 contains 0 rows for date: 2023-07-01.'
        """
        )
