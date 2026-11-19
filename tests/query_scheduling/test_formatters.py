import datetime
from pathlib import Path

import pytest

from bigquery_etl.metadata.parse_metadata import PartitionType
from bigquery_etl.query_scheduling.formatters import (
    format_attr,
    format_date,
    format_optional_string,
    format_schedule_interval,
    format_task_destination_partition,
    format_task_partition_parameter,
    format_timedelta,
)
from bigquery_etl.query_scheduling.task import Task
from bigquery_etl.query_scheduling.utils import InvalidTimedeltaError

QUERY_FILE = (
    Path(__file__).parent.parent
    / "data"
    / "test_sql"
    / "moz-fx-data-test-project"
    / "test"
    / "incremental_query_v1"
    / "query.sql"
)


def _task(**kwargs):
    return Task(
        dag_name="bqetl_test_dag",
        query_file=str(QUERY_FILE),
        owner="test@example.com",
        **kwargs,
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

        with pytest.raises(InvalidTimedeltaError):
            assert format_timedelta(123)

        assert format_timedelta("1h") == datetime.timedelta(hours=1)
        assert format_timedelta("-1h") == datetime.timedelta(hours=-1)
        assert format_timedelta("15m") == datetime.timedelta(minutes=15)
        assert format_timedelta("-15m") == datetime.timedelta(minutes=-15)
        assert format_timedelta("1h15m") == datetime.timedelta(hours=1, minutes=15)
        assert format_timedelta("-1h15m") == datetime.timedelta(hours=-1, minutes=-15)
        assert format_timedelta("1s") == datetime.timedelta(seconds=1)
        assert format_timedelta("-1s") == datetime.timedelta(seconds=-1)
        assert format_timedelta("1h1m1s") == datetime.timedelta(
            hours=1, minutes=1, seconds=1
        )
        assert format_timedelta("-1h1m1s") == datetime.timedelta(
            hours=-1, minutes=-1, seconds=-1
        )
        assert format_timedelta("1d1h1m1s") == datetime.timedelta(
            days=1, hours=1, minutes=1, seconds=1
        )

    def test_format_optional_string(self):
        assert format_optional_string(None) == "None"

        with pytest.raises(TypeError):
            assert format_optional_string(123)

        assert format_optional_string("test") == "'test'"

    @pytest.mark.parametrize(
        "partition_type,partition_parameter,partition_offset,expected",
        [
            (
                PartitionType.DAY,
                "submission_date",
                None,
                "{{ logical_date.format('YYYYMMDD') }}",
            ),
            # In the absence of partitioning metadata we assume day partitioning.
            (None, "submission_date", None, "{{ logical_date.format('YYYYMMDD') }}"),
            (
                PartitionType.MONTH,
                "submission_month",
                None,
                "{{ logical_date.format('YYYYMM') }}",
            ),
            (
                PartitionType.YEAR,
                "submission_year",
                None,
                "{{ logical_date.format('YYYY') }}",
            ),
            (
                PartitionType.HOUR,
                "submission_hour",
                None,
                "{{ logical_date.format('YYYYMMDDHH') }}",
            ),
            (
                PartitionType.DAY,
                "submission_date",
                "-2d",
                "{{ (logical_date + macros.timedelta(days=-2)).format('YYYYMMDD') }}",
            ),
            # A null partition parameter targets the whole table, unless there's a non-zero partition offset.
            (PartitionType.DAY, None, None, None),
            (PartitionType.DAY, None, "0d", None),
            (
                PartitionType.DAY,
                None,
                "-1d",
                "{{ (logical_date + macros.timedelta(days=-1)).format('YYYYMMDD') }}",
            ),
        ],
    )
    def test_format_task_destination_partition(
        self, partition_type, partition_parameter, partition_offset, expected
    ):
        task = _task(
            partition_type=partition_type,
            partition_parameter=partition_parameter,
            partition_offset=partition_offset,
        )
        assert format_task_destination_partition(task) == expected

    @pytest.mark.parametrize(
        "partition_type,partition_parameter,partition_offset,expected",
        [
            (
                PartitionType.DAY,
                "submission_date",
                None,
                "submission_date:DATE:{{ logical_date.format('YYYY-MM-DD') }}",
            ),
            (
                PartitionType.MONTH,
                "submission_month",
                None,
                "submission_month:DATE:{{ logical_date.format('YYYY-MM-01') }}",
            ),
            (
                PartitionType.YEAR,
                "submission_year",
                None,
                "submission_year:INTEGER:{{ logical_date.format('YYYY') }}",
            ),
            (
                PartitionType.HOUR,
                "submission_hour",
                None,
                "submission_hour:TIMESTAMP:{{ logical_date.format('YYYY-MM-DD HH:00:00') }}",
            ),
            (
                PartitionType.DAY,
                "first_seen_date",
                "-27d",
                "first_seen_date:DATE:{{ (logical_date + macros.timedelta(days=-27)).format('YYYY-MM-DD') }}",
            ),
            (PartitionType.DAY, None, None, None),
            (PartitionType.DAY, None, "-1d", None),
        ],
    )
    def test_format_task_partition_parameter(
        self, partition_type, partition_parameter, partition_offset, expected
    ):
        task = _task(
            partition_type=partition_type,
            partition_parameter=partition_parameter,
            partition_offset=partition_offset,
        )
        assert format_task_partition_parameter(task) == expected
