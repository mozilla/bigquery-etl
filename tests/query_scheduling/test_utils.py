import datetime

import pytest

from bigquery_etl.query_scheduling.utils import (
    InvalidTimedeltaError,
    ReadableTimedelta,
    parse_timedelta,
    schedule_interval_delta,
)


class TestUtils:
    def test_schedule_interval_delta(self):
        assert schedule_interval_delta("0 1 * * *", "0 2 * * *") == "3600s"
        assert schedule_interval_delta("0 2 * * *", "0 0 * * *") == "-7200s"
        assert schedule_interval_delta("* * * * *", "* * * * *") == "0s"
        assert schedule_interval_delta("0 1 * * *", "0 1 * * *") == "0s"
        assert schedule_interval_delta("daily", "0 1 * * *") == "3600s"
        assert schedule_interval_delta("daily", "0 0 * * *") == "0s"

    @pytest.mark.parametrize("value", [-1, 0, 1.5, "-1", "-"])
    def test_parse_timedelta_invalid_value(self, value):
        with pytest.raises(InvalidTimedeltaError):
            parse_timedelta(value)


class TestReadableTimedelta:
    @pytest.mark.parametrize(
        "kwargs,expected_str,expected_repr",
        [
            ({}, "0:00:00", "datetime.timedelta(0)"),
            ({"hours": 1}, "1:00:00", "datetime.timedelta(hours=1)"),
            ({"hours": -1}, "-1:00:00", "datetime.timedelta(hours=-1)"),
            ({"seconds": -1}, "-0:00:01", "datetime.timedelta(seconds=-1)"),
            (
                {"minutes": -90},
                "-1:30:00",
                "datetime.timedelta(hours=-1, minutes=-30)",
            ),
            (
                {"days": 3, "hours": 1},
                "3 days, 1:00:00",
                "datetime.timedelta(days=3, hours=1)",
            ),
            ({"days": -1}, "-1 day, 0:00:00", "datetime.timedelta(days=-1)"),
            ({"weeks": -1}, "-7 days, 0:00:00", "datetime.timedelta(days=-7)"),
            (
                {"days": -1, "hours": -2, "minutes": -30},
                "-1 day, -2:30:00",
                "datetime.timedelta(days=-1, hours=-2, minutes=-30)",
            ),
            (
                {"days": -1, "microseconds": -5},
                "-1 day, -0:00:00.000005",
                "datetime.timedelta(days=-1, microseconds=-5)",
            ),
        ],
    )
    def test_str_and_repr(self, kwargs, expected_str, expected_repr):
        td = ReadableTimedelta(**kwargs)
        assert str(td) == expected_str
        assert repr(td) == expected_repr
        # The repr should round-trip back to an equal value.
        assert eval(repr(td), {"datetime": datetime}) == td
