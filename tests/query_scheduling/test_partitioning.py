import pytest

from bigquery_etl.metadata.parse_metadata import (
    BigQueryMetadata,
    Metadata,
    PartitionMetadata,
    PartitionType,
)
from bigquery_etl.query_scheduling.partitioning import (
    get_partition_type,
    resolve_partition_parameter_name,
    resolve_partition_parameter_type,
)


class TestPartitioning:
    def test_get_partition_type(self):
        assert get_partition_type(None) is None
        assert (
            get_partition_type(Metadata("test", "test", ["test@example.org"], {}))
            is None
        )
        assert (
            get_partition_type(
                Metadata(
                    "test",
                    "test",
                    ["test@example.org"],
                    {},
                    bigquery=BigQueryMetadata(
                        time_partitioning=PartitionMetadata(type=PartitionType.MONTH)
                    ),
                )
            )
            == PartitionType.MONTH
        )

    @pytest.mark.parametrize(
        "partition_type,expected",
        [
            (PartitionType.YEAR, "submission_year"),
            (PartitionType.MONTH, "submission_month"),
            (PartitionType.DAY, "submission_date"),
            (PartitionType.HOUR, "submission_hour"),
            # In the absence of partitioning metadata we assume day partitioning.
            (None, "submission_date"),
        ],
    )
    def test_resolve_partition_parameter_name_default(self, partition_type, expected):
        assert resolve_partition_parameter_name({}, partition_type) == expected

    def test_resolve_partition_parameter_name_explicit(self):
        assert (
            resolve_partition_parameter_name(
                {"partition_parameter": "first_seen_date"}, PartitionType.MONTH
            )
            == "first_seen_date"
        )
        # An explicit null targets the whole table.
        assert (
            resolve_partition_parameter_name(
                {"partition_parameter": None}, PartitionType.DAY
            )
            is None
        )
        # Explicit partition parameters also apply to scripts.
        assert (
            resolve_partition_parameter_name(
                {"partition_parameter": "date"}, PartitionType.DAY, is_script=True
            )
            == "date"
        )

    def test_resolve_partition_parameter_no_default(self):
        # Scripts and destination tables with an explicit partition decorator don't get a default.
        assert (
            resolve_partition_parameter_name({}, PartitionType.DAY, is_script=True)
            is None
        )
        assert (
            resolve_partition_parameter_name(
                {"destination_table": "table_v1${{ ds_nodash }}"}, PartitionType.DAY
            )
            is None
        )

    @pytest.mark.parametrize(
        "partition_type,expected",
        [
            (PartitionType.YEAR, "INTEGER"),
            (PartitionType.MONTH, "DATE"),
            (PartitionType.DAY, "DATE"),
            (PartitionType.HOUR, "TIMESTAMP"),
            # In the absence of partitioning metadata we assume day partitioning.
            (None, "DATE"),
        ],
    )
    def test_resolve_partition_parameter_type(self, partition_type, expected):
        assert resolve_partition_parameter_type(partition_type) == expected
