import os
from datetime import datetime, time, timedelta
from pathlib import Path
from unittest import runner
from unittest.mock import patch

from google.cloud import bigquery

import pytest
from click.exceptions import ClickException

from bigquery_etl.backfill.shredder_mitigation import (
    PREVIOUS_DATE,
    Column,
    ColumnStatus,
    ColumnType,
    DataTypeGroup,
    classify_columns,
    get_bigquery_type,
    generate_query_with_shredder_mitigation,
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

    def test_matching_new_row_and_new_columns(self):
        new_row = {
            "submission_date": "2024-01-01",
            "channel": None,
            "os": "Mac",
            "metric_int": 2024,
        }
        existing_columns = ["submission_date", "channel"]
        new_columns = ["submission_date", "channel", "os", "is_default_browser"]
        expected_exception_text = (f"Inconsistent parameters. New dimension columns not found in new row: ['is_default_browser']"
                                   f"\n new_dimension_columns: {new_columns}"
                                   f"\n new_row: {new_row}")
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, existing_columns, new_columns)
        assert (str(e.value)) == expected_exception_text

    def test_missing_parameters(self):
        new_row = {}
        expected_exception_text = (
            f"\n\nMissing one or more required parameters. Received:\nnew_row= {new_row}\n"
            f"existing_dimension_columns= [],\nnew_dimension_columns= []."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, [], [])
        assert (str(e.value)) == expected_exception_text

        new_row = {"column_1": "2024-01-01", "column_2": "Windows"}
        new_columns = {"column_2"}
        expected_exception_text = (
            f"\n\nMissing one or more required parameters. Received:\nnew_row= {new_row}\n"
            f"existing_dimension_columns= [],\nnew_dimension_columns= {new_columns}."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, [], new_columns)
        assert (str(e.value)) == expected_exception_text

        new_row = {"column_1": "2024-01-01", "column_2": "Windows"}
        existing_columns = ["column_1"]
        expected_exception_text = (
            f"\n\nMissing one or more required parameters. Received:\nnew_row= {new_row}\n"
            f"existing_dimension_columns= {existing_columns},\nnew_dimension_columns= []."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, existing_columns, [])
        assert (str(e.value)) == expected_exception_text


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


class TestSubset(object):
    def test_get_query_path_results(self):
        assert True

    def test_generate_query(self):
        '''Test cases: aggregate, different columns, different metrics, missing metrics, added columns / metrics'''
        assert True

    def test_get_query_path(self):
        '''Test that path exists / test that the path contains a query file.'''
        assert True

    def test_get_query_path_results(self):
        '''Test with sample data and sample results'''
        assert True

    def test_generate_check_with_previous_version(self):
        assert True



class TestGenerateQueryWithShredderMitigation(object):
    '''Test the function that generates the query for the backfill.'''
    def test_missing_sql_path(self):
        assert True

    def test_missing_previous_version(self):
        assert False

    def test_missing_previous_version(self):
        assert False

    def test_invalid_group_by(self):
        previous_group_by = []
        new_group_by = []
        expected_exc = (f"An explicit list of columns is required in queries's GROUP BY. Avoid `GROUP BY ALL`."
                        f"\nPrevious query returns `GROUP BY {previous_group_by}`."
                        f"\nNew query returns `GROUP BY {new_group_by}`."
        )
        client = bigquery.Client()
        project_id = 'test_project'
        dataset = 'test_dataset_v1'
        destination_table = 'test_table_v1'
        generate_query_with_shredder_mitigation(client=client, project_id=project_id, dataset=dataset, destination_table=destination_table, backfill_date=PREVIOUS_DATE)

        assert False

    def test_columns_exist_in_versions(self):
        assert True

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.shredder_mitigation.extract_last_group_by_from_query")
    # @patch("bigquery_etl.cli.backfill.Schema.from_query_file")
    def sample_test(self, mock_extract_last_group_by_from_query, mock_client):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            os.makedirs(SQL_DIR)
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            v2_path = "sql/moz-fx-data-shared-prod/test/test_query_v2/query.sql"
            v1_path = "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql"

            with open(
                    v2_path, "w"
            ) as f:
                f.write("SELECT column_1, column_2 FROM upstream_1 GROUP BY column_1, 2")

            with open(
                    v1_path, "w"
            ) as f:
                f.write("SELECT column_1 FROM upstream_1 GROUP BY column_1")

            assert mock_extract_last_group_by_from_query.called

            result = runner.invoke(
                generate_query_with_shredder_mitigation,
                [
                    mock_client,
                    'moz-fx-data-shared-prod',
                    'test',
                    'test_query_v2',
                    PREVIOUS_DATE
                ]
            )

            mock_extract_last_group_by_from_query.assert_called_with(
                sql_path=v2_path
            )
            assert result.exit_code == 0
            assert "1 backfill(s) require processing." in result.output
            assert Path("tmp.json").exists()
