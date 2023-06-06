import pytest
from click.testing import CliRunner

from bigquery_etl.cli.check import (
    _build_parameters,
    _build_query_arguments,
    _parse_check_output,
    _parse_query_arguments,
)


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

    def test_parse_query_args(self):
        test = "--parameter=submission_date::2023-06-01", "--parameter=id::asdf"
        expected = {"submission_date": "2023-06-01", "id": "asdf"}
        assert _parse_query_arguments(test) == expected

        test = "--parameter=submission_date::2023-06-01", "--parameters=id::asdf"
        with pytest.raises(ValueError):
            _parse_query_arguments(test)

        test = ["submission_date::2023-06-01"]
        with pytest.raises(ValueError):
            _parse_query_arguments(test)

    def test_build_parameters(self):
        assert _build_parameters(parameters=["submission_date::2023-06-01"]) == {
            "submission_date": "2023-06-01"
        }
        assert _build_parameters(
            parameters=["submission_date::2023-06-01", "id:STRING:asdf"]
        ) == {"submission_date": "2023-06-01", "id": "asdf"}

        assert _build_parameters(parameters=[]) == {}
        assert _build_parameters(parameters=None) == {}

        with pytest.raises(ValueError):
            _build_parameters(parameters=["submission_date2023-06-01"])
            _build_parameters(parameters=["submission_date:2023-06-01"])
            _build_parameters(parameters=["submission_date:::2023-06-01"])
            _build_parameters(parameters=["submission_date$::2023-06-01"])
            _build_parameters(parameters=["submission_date::2023-13-01"])
            _build_parameters(parameters=["submission_date::20230601"])

    def test_build_query_arguments(self):
        parameters = {"submission_date": "2023-06-01", "id": "asdf"}
        assert _build_query_arguments(parameters) == [
            "--parameter=submission_date::2023-06-01",
            "--parameter=id::asdf",
        ]

        parameters = {"submission_date": "2023-06-01", "id": None}
        assert _build_query_arguments(parameters) == [
            "--parameter=submission_date::2023-06-01",
            "--parameter=id::None",
        ]
        assert _build_query_arguments({}) == []
        assert _build_query_arguments(None) == []
