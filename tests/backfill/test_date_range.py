from datetime import date, timedelta

import pytest

from bigquery_etl.backfill.date_range import BackfillDateRange, get_backfill_partition
from bigquery_etl.metadata.parse_metadata import PartitionType


class TestBackfillDateRange:
    def test_basic_date_range(self):

        date_range = BackfillDateRange(
            start_date=date.fromisoformat("2020-01-01"),
            end_date=date.fromisoformat("2020-01-05"),
        )
        dates = list(date_range)
        assert dates == [
            date.fromisoformat("2020-01-01"),
            date.fromisoformat("2020-01-02"),
            date.fromisoformat("2020-01-03"),
            date.fromisoformat("2020-01-04"),
            date.fromisoformat("2020-01-05"),
        ]

    def test_excludes(self):
        date_range = BackfillDateRange(
            start_date=date.fromisoformat("2020-01-01"),
            end_date=date.fromisoformat("2020-01-05"),
            excludes=[
                date.fromisoformat("2020-01-02"),
                date.fromisoformat("2020-01-03"),
            ],
        )
        dates = list(date_range)

        assert dates == [
            date.fromisoformat("2020-01-01"),
            date.fromisoformat("2020-01-04"),
            date.fromisoformat("2020-01-05"),
        ]

    def test_monthly(self):
        date_range = BackfillDateRange(
            start_date=date.fromisoformat("2020-01-01"),
            end_date=date.fromisoformat("2020-03-05"),
            excludes=[date.fromisoformat("2020-02-02")],
            range_type=PartitionType.MONTH,
        )
        dates = list(date_range)

        assert dates == [
            date.fromisoformat("2020-01-01"),
            date.fromisoformat("2020-03-01"),
        ]


def test_get_backfill_partition():

    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-03"),
            partition_parameter="submission_date",
            partition_offset=timedelta(days=-1),
            partitioning_type=PartitionType.DAY,
        )
        == "20200102"
    )

    # Sub-day offsets only apply their whole days to backfill dates.
    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-03"),
            partition_parameter="submission_date",
            partition_offset=timedelta(hours=-1),
            partitioning_type=PartitionType.DAY,
        )
        == "20200103"
    )
    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-03"),
            partition_parameter="submission_date",
            partition_offset=timedelta(days=-1, hours=-1),
            partitioning_type=PartitionType.DAY,
        )
        == "20200102"
    )

    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-01"),
            partition_parameter="submission_date",
            partition_offset=None,
            partitioning_type=PartitionType.MONTH,
        )
        == "202001"
    )

    # A null partition parameter targets the whole table, unless there's a partition offset.
    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-01"),
            partition_parameter=None,
            partition_offset=None,
            partitioning_type=PartitionType.DAY,
        )
        is None
    )
    # An explicit zero partition offset is the same as no partition offset.
    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-01"),
            partition_parameter=None,
            partition_offset=timedelta(0),
            partitioning_type=PartitionType.DAY,
        )
        is None
    )
    assert (
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-03"),
            partition_parameter=None,
            partition_offset=timedelta(days=-1),
            partitioning_type=PartitionType.DAY,
        )
        == "20200102"
    )

    with pytest.raises(NotImplementedError):
        get_backfill_partition(
            backfill_date=date.fromisoformat("2020-01-01"),
            partition_parameter="submission_date",
            partition_offset=timedelta(days=-1),
            partitioning_type=PartitionType.MONTH,
        )
