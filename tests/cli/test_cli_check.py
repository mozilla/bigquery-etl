import pytest
from click.testing import CliRunner

from bigquery_etl.cli.check import _parse_check_output, _parse_partition_setting


class TestQuery:
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

    def test_parse_partition_setting(self):
        assert _parse_partition_setting("submission_date::2023-06-01") == {
            "submission_date": "2023-06-01"
        }
        assert _parse_partition_setting("submission_date2023-06-01") is None
        assert _parse_partition_setting("submission_date:2023-06-01") is None
        assert _parse_partition_setting("submission_date$::2023-06-01") is None
        assert _parse_partition_setting("submission_date::2023-13-01") is None
        assert _parse_partition_setting("submission_date::20230601") is None
