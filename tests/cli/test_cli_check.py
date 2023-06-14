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
