import datetime

import pytest

from bigquery_etl.query_scheduling.formatters import (
    format_attr,
    format_date,
    format_optional_string,
    format_schedule_interval,
    format_timedelta,
)


class TestFormatters:
    def test_format_schedule_interval(self):
        assert "'@daily'" == format_schedule_interval("daily")
        assert "'@once'" == format_schedule_interval("once")
        assert "'* 1 * * *'" == format_schedule_interval("* 1 * * *")
        assert "'foo bar'" == format_schedule_interval("foo bar")

    def test_format_attr(self):
        assert format_attr({"a": 12}, "b", "format_schedule_interval") == {"a": 12}
        assert format_attr({"a": 12}, "a", "non_existing_formatter") == {"a": 12}
        assert format_attr(
            {"interval": "daily", "a": 12}, "interval", "format_schedule_interval"
        ) == {"interval": "'@daily'", "a": 12}

    def test_format_date(self):
        assert format_date(None) is None

        with pytest.raises(ValueError):
            assert format_date("March 12th 2020")

        with pytest.raises(ValueError):
            assert format_date("2020-13-01")

        assert format_date("2020-01-01") == datetime.datetime(2020, 1, 1, 0, 0)

    def test_format_timedelta(self):
        assert format_timedelta("string") == "string"

        with pytest.raises(TypeError):
            assert format_timedelta(123)

        assert format_timedelta("1h") == datetime.timedelta(seconds=3600)
        assert format_timedelta("15m") == datetime.timedelta(seconds=900)
        assert format_timedelta("1h15m") == datetime.timedelta(seconds=4500)
        assert format_timedelta("1s") == datetime.timedelta(seconds=1)
        assert format_timedelta("1h1m1s") == datetime.timedelta(seconds=3661)
        assert format_timedelta("1d1h1m1s") == "1d1h1m1s"

    def test_format_optional_string(self):
        assert format_optional_string(None) == "None"

        with pytest.raises(TypeError):
            assert format_optional_string(123)

        assert format_optional_string("test") == "'test'"
