from datetime import datetime, time

from bigquery_etl.backfill.shredder_mitigation import (
    Column,
    ColumnStatus,
    ColumnType,
    DataTypeGroup,
    classify_columns,
    get_bigquery_type,
)


class TestClassifyColumns(object):
    def test_new_numeric_dimension(self):
        new_row = {
            "submission_date": "2024-01-01",
            "app_name": "Browser",
            "channel": "Beta",
            "first_seen_year": 2024,
            "os_build": "Windows 10",
            "metric_numeric": 10,
            "metric_float": 15.123456,
        }
        existing_columns = [
            "submission_date",
            "app_name",
            "channel",
            "first_seen_year",
            "os",
        ]
        new_columns = [
            "submission_date",
            "app_name",
            "channel",
            "first_seen_year",
            "os_build",
        ]

        expected_common_dimensions = [
            Column(
                name="app_name",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="channel",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="first_seen_year",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="submission_date",
                data_type=DataTypeGroup.DATE,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
        ]

        expected_added_dimensions = [
            Column(
                name="os_build",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            )
        ]

        expected_removed_dimensions = [
            Column(
                name="os",
                data_type=DataTypeGroup.UNDETERMINED,
                column_type=ColumnType.UNDETERMINED,
                status=ColumnStatus.REMOVED,
            )
        ]

        expected_metrics = [
            Column(
                name="metric_float",
                data_type=DataTypeGroup.FLOAT,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="metric_numeric",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
        ]

        common_dimensions, added_dimensions, removed_dimensions, metrics, undefined = (
            classify_columns(new_row, existing_columns, new_columns)
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_new_boolean_dimension(self):
        new_row = {
            "submission_date": "2024-01-01",
            "metric_float": 10.000,
            "channel": "Beta",
            "first_seen_year": 2024,
            "is_default_browser": False,
            "metric_numeric": 10,
        }
        existing_columns = ["submission_date", "channel", "first_seen_year"]
        new_columns = [
            "submission_date",
            "channel",
            "first_seen_year",
            "is_default_browser",
        ]

        expected_common_dimensions = [
            Column(
                name="channel",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="first_seen_year",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="submission_date",
                data_type=DataTypeGroup.DATE,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
        ]

        expected_added_dimensions = [
            Column(
                name="is_default_browser",
                data_type=DataTypeGroup.BOOLEAN,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            )
        ]

        expected_removed_dimensions = []

        expected_metrics = [
            Column(
                name="metric_float",
                data_type=DataTypeGroup.FLOAT,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="metric_numeric",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
        ]

        common_dimensions, added_dimensions, removed_dimensions, metrics, undefined = (
            classify_columns(new_row, existing_columns, new_columns)
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_removed_numeric_dimension(self):
        new_row = {
            "submission_date": "2024-01-01",
            "metric_float": 1.00003496056549640598605498605486,
            "channel": "Beta",
            "is_default_browser": False,
            "metric_numeric": 101927498327449035043865,
        }
        existing_columns = [
            "submission_date",
            "channel",
            "first_seen_year",
            "is_default_browser",
        ]
        new_columns = ["submission_date", "channel", "is_default_browser"]

        expected_common_dimensions = [
            Column(
                name="channel",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="is_default_browser",
                data_type=DataTypeGroup.BOOLEAN,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="submission_date",
                data_type=DataTypeGroup.DATE,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
        ]

        expected_added_dimensions = []

        expected_removed_dimensions = [
            Column(
                name="first_seen_year",
                data_type=DataTypeGroup.UNDETERMINED,
                column_type=ColumnType.UNDETERMINED,
                status=ColumnStatus.REMOVED,
            )
        ]

        expected_metrics = [
            Column(
                name="metric_float",
                data_type=DataTypeGroup.FLOAT,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="metric_numeric",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
        ]

        common_dimensions, added_dimensions, removed_dimensions, metrics, undefined = (
            classify_columns(new_row, existing_columns, new_columns)
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_new_multiple_dimensions_including_from_values_none(self):
        new_row = {
            "submission_date": "2024-01-01",
            "os_version": "10.1",
            "metric_float": 1.00003496056549640598605498605486,
            "channel": "Beta",
            "is_default_browser": False,
            "os_version_build": None,
            "segment": None,
        }
        existing_columns = [
            "submission_date",
            "channel",
            "first_seen_year",
            "is_default_browser",
        ]
        new_columns = [
            "submission_date",
            "channel",
            "is_default_browser",
            "os_version",
            "os_version_build",
            "segment",
        ]

        expected_common_dimensions = [
            Column(
                name="channel",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="is_default_browser",
                data_type=DataTypeGroup.BOOLEAN,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="submission_date",
                data_type=DataTypeGroup.DATE,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
        ]

        expected_added_dimensions = [
            Column(
                name="os_version",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            ),
            Column(
                name="os_version_build",
                data_type=DataTypeGroup.UNDETERMINED,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            ),
            Column(
                name="segment",
                data_type=DataTypeGroup.UNDETERMINED,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            ),
        ]

        expected_removed_dimensions = [
            Column(
                name="first_seen_year",
                data_type=DataTypeGroup.UNDETERMINED,
                column_type=ColumnType.UNDETERMINED,
                status=ColumnStatus.REMOVED,
            )
        ]

        expected_metrics = [
            Column(
                name="metric_float",
                data_type=DataTypeGroup.FLOAT,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            )
        ]

        common_dimensions, added_dimensions, removed_dimensions, metrics, undefined = (
            classify_columns(new_row, existing_columns, new_columns)
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_new_metrics(self):
        new_row = {
            "submission_date": "2024-01-01",
            "metric_float": 1.00003496056549640598605498605486,
            "channel": None,
            "metric_bigint": 10000349605654964059860549860520397593485486,
            "metric_int": 2024,
        }
        existing_columns = ["submission_date", "channel"]
        new_columns = ["submission_date", "channel"]

        expected_common_dimensions = [
            Column(
                name="channel",
                data_type=DataTypeGroup.UNDETERMINED,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="submission_date",
                data_type=DataTypeGroup.DATE,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.COMMON,
            ),
        ]

        expected_added_dimensions = []

        expected_removed_dimensions = []

        expected_metrics = [
            Column(
                name="metric_bigint",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="metric_float",
                data_type=DataTypeGroup.FLOAT,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
            Column(
                name="metric_int",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
        ]

        common_dimensions, added_dimensions, removed_dimensions, metrics, undefined = (
            classify_columns(new_row, existing_columns, new_columns)
        )
        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []


class TestGetBigqueryType(object):
    def test_numeric_group(self):
        assert get_bigquery_type(3) == DataTypeGroup.NUMERIC
        assert get_bigquery_type(2024) == DataTypeGroup.NUMERIC
        assert get_bigquery_type(9223372036854775807) == DataTypeGroup.NUMERIC
        assert get_bigquery_type(123456) == DataTypeGroup.NUMERIC
        assert get_bigquery_type(-123456) == DataTypeGroup.NUMERIC
        assert get_bigquery_type(789.01) == DataTypeGroup.FLOAT

        assert get_bigquery_type(1.00000000000000000000000456) == DataTypeGroup.FLOAT
        assert get_bigquery_type(999999999999999999999.999999999) == DataTypeGroup.FLOAT
        assert get_bigquery_type(-1.23456) == DataTypeGroup.FLOAT

    def test_boolean_group(self):
        assert get_bigquery_type(False) == DataTypeGroup.BOOLEAN
        assert get_bigquery_type(True) == DataTypeGroup.BOOLEAN

    def test_date_type(self):
        assert get_bigquery_type("2024-01-01") == DataTypeGroup.DATE
        assert get_bigquery_type("2024-01-01T10:00:00") == DataTypeGroup.DATE
        assert get_bigquery_type("2024-01-01T10:00:00Z") == DataTypeGroup.DATE
        assert get_bigquery_type("2024-01-01T10:00:00") == DataTypeGroup.DATE
        assert get_bigquery_type("2024-08-01 12:34:56 UTC") == DataTypeGroup.DATE
        assert get_bigquery_type("12:34:56") == DataTypeGroup.DATE
        assert get_bigquery_type(time(12, 34, 56)) == DataTypeGroup.DATE
        assert get_bigquery_type(datetime(2024, 12, 26)) == DataTypeGroup.DATE

    def test_other_types(self):
        assert get_bigquery_type("2024") == DataTypeGroup.STRING
        assert get_bigquery_type(None) == DataTypeGroup.UNDETERMINED
