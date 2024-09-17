"""Test cases for shredder mitigation."""

import os
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import call, patch

import click
import pytest
import yaml
from click.exceptions import ClickException
from click.testing import CliRunner
from gcloud import bigquery  # type: ignore

from bigquery_etl.backfill.shredder_mitigation import (
    PREVIOUS_DATE,
    QUERY_WITH_MITIGATION_NAME,
    Column,
    ColumnStatus,
    ColumnType,
    DataTypeGroup,
    Subset,
    classify_columns,
    generate_query_with_shredder_mitigation,
    get_bigquery_type,
    validate_types,
)


class TestClassifyColumns:
    """Test cases for function classify_columns."""

    def test_new_numeric_dimension(self):
        """Test adding a numeric dimension."""
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
        existing_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("app_name", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("first_seen_year", "INT", "NULLABLE", None, None),
            bigquery.SchemaField("os", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
        ]
        new_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("app_name", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("first_seen_year", "INT", "NULLABLE", None, None),
            bigquery.SchemaField("os_build", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
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
                data_type=DataTypeGroup.INTEGER,
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
                data_type=DataTypeGroup.STRING,
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
            classify_columns(
                new_row, existing_columns, new_columns, existing_schema, new_schema
            )
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_new_boolean_dimension(self):
        """Test adding a boolean dimension."""
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
        existing_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("first_seen_year", "INT", "NULLABLE", None, None),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
        ]
        new_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("first_seen_year", "INT", "NULLABLE", None, None),
            bigquery.SchemaField(
                "is_default_browser", "BOOLEAN", "NULLABLE", None, None
            ),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
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
                data_type=DataTypeGroup.INTEGER,
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
            classify_columns(
                new_row, existing_columns, new_columns, existing_schema, new_schema
            )
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_removed_numeric_dimension(self):
        """Test removing numeric dimension and metric_integer."""
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
        existing_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("first_seen_year", "INT", "NULLABLE", None, None),
            bigquery.SchemaField(
                "is_default_browser", "BOOLEAN", "NULLABLE", None, None
            ),
            bigquery.SchemaField("metric_integer", "INTEGER", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
        ]
        new_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField(
                "is_default_browser", "BOOLEAN", "NULLABLE", None, None
            ),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
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

        expected_added_dimensions = []

        expected_removed_dimensions = [
            Column(
                name="first_seen_year",
                data_type=DataTypeGroup.INTEGER,
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
            classify_columns(
                new_row, existing_columns, new_columns, existing_schema, new_schema
            )
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_new_multiple_dimensions_including_from_values_none(self):
        """Test adding multiple dimensions."""
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
        existing_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("first_seen_year", "INT", "NULLABLE", None, None),
            bigquery.SchemaField(
                "is_default_browser", "BOOLEAN", "NULLABLE", None, None
            ),
            bigquery.SchemaField("metric_integer", "INTEGER", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
        ]
        new_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField(
                "is_default_browser", "BOOLEAN", "NULLABLE", None, None
            ),
            bigquery.SchemaField("os_version", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("os_version_build", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("segment", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
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
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            ),
            Column(
                name="segment",
                data_type=DataTypeGroup.STRING,
                column_type=ColumnType.DIMENSION,
                status=ColumnStatus.ADDED,
            ),
        ]

        expected_removed_dimensions = [
            Column(
                name="first_seen_year",
                data_type=DataTypeGroup.INTEGER,
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
            classify_columns(
                new_row, existing_columns, new_columns, existing_schema, new_schema
            )
        )

        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_new_metrics(self):
        """Test that metrics added in new version are identified correctly."""
        new_row = {
            "submission_date": "2024-01-01",
            "metric_float": 10000349605654964059860549860.520397593485486,
            "channel": None,
            "metric_bigint": 10000349605654964059860549860520397593485486,
            "metric_numeric": 10000349605654964059860549860.520397593485486,
        }
        existing_columns = ["submission_date", "channel"]
        new_columns = ["submission_date", "channel"]
        existing_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_bigint", "INTEGER", "NULLABLE", None, None),
        ]
        new_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
            bigquery.SchemaField("metric_numeric", "NUMERIC", "NULLABLE", None, None),
            bigquery.SchemaField("metric_bigint", "INTEGER", "NULLABLE", None, None),
        ]

        expected_common_dimensions = [
            Column(
                name="channel",
                data_type=DataTypeGroup.STRING,
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
                data_type=DataTypeGroup.INTEGER,
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
                name="metric_numeric",
                data_type=DataTypeGroup.NUMERIC,
                column_type=ColumnType.METRIC,
                status=ColumnStatus.COMMON,
            ),
        ]

        common_dimensions, added_dimensions, removed_dimensions, metrics, undefined = (
            classify_columns(
                new_row, existing_columns, new_columns, existing_schema, new_schema
            )
        )
        assert common_dimensions == expected_common_dimensions
        assert added_dimensions == expected_added_dimensions
        assert removed_dimensions == expected_removed_dimensions
        assert metrics == expected_metrics
        assert undefined == []

    def test_not_matching_new_row_and_new_columns(self):
        """Test output when data in bigquery doesn't match columns in group by."""
        new_row = {
            "submission_date": "2024-01-01",
            "channel": None,
            "os": "Mac",
            "metric_int": 2024,
        }
        existing_columns = ["submission_date", "channel"]
        new_columns = ["submission_date", "channel", "os", "is_default_browser"]
        existing_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
        ]
        new_schema = [
            bigquery.SchemaField("submission_date", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("channel", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField("os", "STRING", "NULLABLE", None, None),
            bigquery.SchemaField(
                "is_default_browser", "BOOLEAN", "NULLABLE", None, None
            ),
            bigquery.SchemaField("metric_float", "FLOAT", "NULLABLE", None, None),
        ]
        expected_exception_text = (
            "Existing dimensions don't match columns retrieved by query."
            " Missing ['is_default_browser']."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(
                new_row, existing_columns, new_columns, existing_schema, new_schema
            )
        assert (str(e.value.message)) == expected_exception_text

    def test_missing_parameters(self):
        """Test output when the parameters to classify columns are incomplete."""
        new_row = {}
        expected_exception_text = (
            f"\n\nMissing one or more required parameters. Received:\nnew_row= {new_row}\n"
            f"existing_dimension_columns= [],\nnew_dimension_columns= []."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, [], [], None, None)
        assert (str(e.value.message)) == expected_exception_text

        new_row = {"column_1": "2024-01-01", "column_2": "Windows"}
        new_columns = {"column_2"}
        expected_exception_text = (
            f"\n\nMissing one or more required parameters. Received:\nnew_row= {new_row}\n"
            f"existing_dimension_columns= [],\nnew_dimension_columns= {new_columns}."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, [], new_columns, None, None)
        assert (str(e.value.message)) == expected_exception_text

        new_row = {"column_1": "2024-01-01", "column_2": "Windows"}
        existing_columns = ["column_1"]
        existing_schema = [
            bigquery.SchemaField("column1", "DATE", "NULLABLE", None, None)
        ]
        new_schema = [
            bigquery.SchemaField("column1", "DATE", "NULLABLE", None, None),
            bigquery.SchemaField("column2", "STRING", "NULLABLE", None, None),
        ]
        expected_exception_text = (
            f"\n\nMissing one or more required parameters. Received:\nnew_row= {new_row}\n"
            f"existing_dimension_columns= {existing_columns},\nnew_dimension_columns= []."
        )
        with pytest.raises(ClickException) as e:
            classify_columns(new_row, existing_columns, [], existing_schema, new_schema)
        assert (str(e.value.message)) == expected_exception_text


class TestGetBigqueryType:
    """Test cases for function get_bigquery_type"""

    def test_numeric_group(self):
        """Test function get_bigquery_type with numeric types."""
        assert get_bigquery_type(3) == DataTypeGroup.INTEGER
        assert get_bigquery_type(2024) == DataTypeGroup.INTEGER
        assert get_bigquery_type(9223372036854775807) == DataTypeGroup.INTEGER
        assert get_bigquery_type(123456) == DataTypeGroup.INTEGER
        assert get_bigquery_type(-123456) == DataTypeGroup.INTEGER
        assert get_bigquery_type(789.01) == DataTypeGroup.FLOAT
        assert get_bigquery_type(1.00000000000000000000000456) == DataTypeGroup.FLOAT
        assert get_bigquery_type(999999999999999999999.999999999) == DataTypeGroup.FLOAT
        assert get_bigquery_type(-1.23456) == DataTypeGroup.FLOAT
        assert get_bigquery_type(100000000000000000000.123456789) == DataTypeGroup.FLOAT

    def test_boolean_group(self):
        """Test function get_bigquery_type with boolean types."""
        assert get_bigquery_type(False) == DataTypeGroup.BOOLEAN
        assert get_bigquery_type(True) == DataTypeGroup.BOOLEAN

    def test_date_type(self):
        """Test function get_bigquery_type with datetime types."""
        assert get_bigquery_type("2024-01-01") == DataTypeGroup.DATE
        assert get_bigquery_type("2024-01-01T10:00:00") == DataTypeGroup.DATETIME
        assert get_bigquery_type("2024-01-01T10:00:00Z") == DataTypeGroup.TIMESTAMP
        assert get_bigquery_type("2024-01-01T10:00:00") == DataTypeGroup.DATETIME
        assert get_bigquery_type("2024-08-01 12:34:56 UTC") == DataTypeGroup.TIMESTAMP
        assert get_bigquery_type("2024-09-02 14:30:45") == DataTypeGroup.DATETIME
        assert get_bigquery_type("12:34:56") == DataTypeGroup.TIME
        assert get_bigquery_type(time(12, 34, 56)) == DataTypeGroup.TIME
        assert get_bigquery_type(datetime(2024, 12, 26)) == DataTypeGroup.DATETIME
        assert get_bigquery_type(date(2024, 12, 26)) == DataTypeGroup.DATE

    def test_other_types(self):
        """Test function get_bigquery_type with other types."""
        assert get_bigquery_type("2024") == DataTypeGroup.STRING
        assert get_bigquery_type(None) == DataTypeGroup.UNDETERMINED


class TestValidateTypes:
    """Test cases for function ValidateTypes"""

    columns = ["column1", "column2", "column3"]
    schema = [
        bigquery.SchemaField("column1", "STRING", "NULLABLE", None, None),
        bigquery.SchemaField("column2", "STRING", "NULLABLE", None, None),
        bigquery.SchemaField("column3", "NUMERIC", "NULLABLE", None, None),
    ]

    def test_validate_types_match(self):
        """ "Test that types match the expected result."""
        sample_row = {"column1": "abcd", "column2": "wxyz", "column3": 1234.105}
        result = validate_types(self.columns, self.schema, sample_row)

        assert result == {
            "column1": DataTypeGroup.STRING,
            "column2": DataTypeGroup.STRING,
            "column3": DataTypeGroup.NUMERIC,
        }

    def test_validate_types_mismatch(self):
        """ "Test that type in the schema is returned even if sample row returns a different type."""
        sample_row = {"column1": "abcd", "column2": "wxyz", "column3": "I am a STRING"}
        result = validate_types(self.columns, self.schema, sample_row)

        assert result == {
            "column1": DataTypeGroup.STRING,
            "column2": DataTypeGroup.STRING,
            "column3": DataTypeGroup.NUMERIC,
        }

    def test_validate_types_missing_column_in_schema(self):
        """Test function get_bigquery_type when columns are not found in schema file."""
        names = ["column1", "column2", "column_not_in_schema"]
        schema = [
            bigquery.SchemaField("column1", "STRING", "NULLABLE", None),
            bigquery.SchemaField("column2", "STRING", "NULLABLE", None),
        ]
        sample_row = {
            "column1": "abcd",
            "column2": "wxyz",
            "column_not_in_schema": 1234,
        }
        result = validate_types(names, schema, sample_row)

        assert result == {
            "column1": DataTypeGroup.STRING,
            "column2": DataTypeGroup.STRING,
            "column_not_in_schema": DataTypeGroup.INTEGER,
        }

    def test_validate_types_missing_data(self):
        """Test function get_bigquery_type when columns are not found in bigquery data."""
        sample_row = {"column1": "abcd", "column2": None}
        result = validate_types(self.columns, self.schema, sample_row)

        assert result == {
            "column1": DataTypeGroup.STRING,
            "column2": DataTypeGroup.STRING,
            "column3": DataTypeGroup.NUMERIC,
        }


class TestSubset:
    """Test cases for the methods in class Subset."""

    project_id = "moz-fx-data-shared-prod"
    dataset = "test"
    destination_table = "test_query_v2"
    destination_table_previous = "test_query_v1"
    path = Path("sql") / project_id / dataset / destination_table
    path_previous = Path("sql") / project_id / dataset / destination_table_previous

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @patch("google.cloud.bigquery.Client")
    def test_version(self, mock_client):
        """Test valid version in destinatino table."""
        test_tables_correct = [
            ("test_v1", 1),
            ("test_v10", 10),
            ("test_v0", 0),
        ]
        for table, expected in test_tables_correct:
            test_subset = Subset(
                mock_client, table, None, self.dataset, self.project_id, None
            )
            assert test_subset.version == expected

        test_tables_incorrect = [
            ("test_v-19", 1),
            ("test_v", 1),
            ("test_3", None),
            ("test_10", None),
            ("test_3", 1),
        ]
        for table, expected in test_tables_incorrect:
            test_subset = Subset(
                mock_client, table, None, self.dataset, self.project_id, None
            )
            with pytest.raises(click.ClickException) as e:
                _ = test_subset.version
            assert e.type == click.ClickException
            assert (
                e.value.message
                == f"Invalid or missing table version in {test_subset.destination_table}."
            )

    @patch("google.cloud.bigquery.Client")
    def test_partitioning(self, mock_client, runner):
        """Test that partitioning type and value associated to a subset are returned as expected."""
        test_subset = Subset(
            mock_client,
            self.destination_table,
            None,
            self.dataset,
            self.project_id,
            None,
        )

        with runner.isolated_filesystem():
            os.makedirs(Path(self.path), exist_ok=True)
            with open(Path(self.path) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    field: submission_date\n"
                    "    require_partition_filter: true"
                )
            assert test_subset.partitioning == {
                "field": "submission_date",
                "type": "DAY",
            }

            with open(Path(self.path) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    "
                    "field: first_seen_date\n    require_partition_filter: true"
                )
            assert test_subset.partitioning == {
                "field": "first_seen_date",
                "type": "DAY",
            }

            with open(Path(self.path) / "metadata.yaml", "w") as f:
                f.write(
                    "friendly_name: ABCD\ndescription: ABCD\nlabels:\n  incremental: true"
                )
            assert test_subset.partitioning == {"field": None, "type": None}

    @patch("google.cloud.bigquery.Client")
    def test_generate_query(self, mock_client):
        """Test method generate_query with expected subset queries and exceptions."""
        test_subset = Subset(
            mock_client,
            self.destination_table,
            None,
            self.dataset,
            self.project_id,
            None,
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1"],
            from_clause=f"{self.destination_table_previous}",
            group_by_clause="ALL",
        )
        assert test_subset_query == (
            f"SELECT column_1 FROM {self.destination_table_previous}" f" GROUP BY ALL"
        )

        test_subset_query = test_subset.generate_query(
            select_list=[1, 2, 3],
            from_clause=f"{self.destination_table_previous}",
            order_by_clause="1, 2, 3",
        )
        assert test_subset_query == (
            f"SELECT 1, 2, 3 FROM {self.destination_table_previous}"
            f" ORDER BY 1, 2, 3"
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1", 2, 3],
            from_clause=f"{self.destination_table_previous}",
            group_by_clause="1, 2, 3",
        )
        assert test_subset_query == (
            f"SELECT column_1, 2, 3 FROM {self.destination_table_previous}"
            f" GROUP BY 1, 2, 3"
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1"], from_clause=f"{self.destination_table_previous}"
        )
        assert (
            test_subset_query
            == f"SELECT column_1 FROM {self.destination_table_previous}"
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1"],
            from_clause=f"{self.destination_table_previous}",
            where_clause="column_1 IS NOT NULL",
            group_by_clause="1",
            order_by_clause="1",
        )
        assert test_subset_query == (
            f"SELECT column_1 FROM {self.destination_table_previous}"
            f" WHERE column_1 IS NOT NULL GROUP BY 1 ORDER BY 1"
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1"],
            from_clause=f"{self.destination_table_previous}",
            group_by_clause="1",
            order_by_clause="1",
        )
        assert test_subset_query == (
            f"SELECT column_1 FROM {self.destination_table_previous}"
            f" GROUP BY 1 ORDER BY 1"
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1"],
            from_clause=f"{self.destination_table_previous}",
            having_clause="column_1 > 1",
        )
        assert (
            test_subset_query
            == f"SELECT column_1 FROM {self.destination_table_previous}"
        )

        test_subset_query = test_subset.generate_query(
            select_list=["column_1"],
            from_clause=f"{self.destination_table_previous}",
            group_by_clause="1",
            having_clause="column_1 > 1",
        )
        assert test_subset_query == (
            f"SELECT column_1 FROM {self.destination_table_previous}"
            f" GROUP BY 1 HAVING column_1 > 1"
        )

        with pytest.raises(ClickException) as e:
            test_subset.generate_query(
                select_list=[],
                from_clause=f"{self.destination_table_previous}",
                group_by_clause="1",
                having_clause="column_1 > 1",
            )
        assert str(e.value.message) == (
            f"Missing required clause to generate query.\n"
            f"Actuals: SELECT: [], FROM: {test_subset.full_table_id}"
        )

    @patch("google.cloud.bigquery.Client")
    def test_get_query_path_results(self, mock_client, runner):
        """Test expected results for a mocked BigQuery call."""
        test_subset = Subset(
            mock_client,
            self.destination_table,
            None,
            self.dataset,
            self.project_id,
            None,
        )
        expected = [{"column_1": "1234"}]

        with runner.isolated_filesystem():
            os.makedirs(self.path, exist_ok=True)
            with open(Path(self.path) / "query.sql", "w") as f:
                f.write("SELECT column_1 WHERE submission_date = @submission_date")

            with pytest.raises(FileNotFoundError) as e:
                test_subset.get_query_path_results(None)
                assert "metadata.yaml" in str(e)

            with open(Path(self.path) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    field: submission_date"
                )
            mock_query = mock_client.query
            mock_query.return_value.result.return_value = iter(expected)
            result = test_subset.get_query_path_results(None)
            assert result == expected

    def test_generate_check_with_previous_version(self):
        """Test the auto-generation of data checks."""
        assert True


class TestGenerateQueryWithShredderMitigation:
    """Test function generate_query_with_shredder_mitigation and returned query for backfill."""

    project_id = "moz-fx-data-shared-prod"
    dataset = "test"
    destination_table = "test_query_v2"
    destination_table_previous = "test_query_v1"
    path = Path("sql") / project_id / dataset / destination_table
    path_previous = Path("sql") / project_id / dataset / destination_table_previous

    @pytest.fixture
    def runner(self):
        """Runner"""
        return CliRunner()

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.backfill.shredder_mitigation.classify_columns")
    def test_generate_query_as_expected(
        self, mock_classify_columns, mock_client, runner
    ):
        """Test that query is generated as expected given a set of mock dimensions and metrics."""

        expected = (
            Path("sql")
            / self.project_id
            / self.dataset
            / self.destination_table
            / f"{QUERY_WITH_MITIGATION_NAME}.sql",
            """-- Query generated using a template for shredder mitigation.
                        WITH new_version AS (
                          SELECT
                            column_1,
                            column_2,
                            metric_1
                          FROM
                            upstream_1
                          GROUP BY
                            column_1,
                            column_2
                        ),
                        new_agg AS (
                          SELECT
                            submission_date,
                            COALESCE(column_1, '???????') AS column_1,
                            SUM(metric_1) AS metric_1
                          FROM
                            new_version
                          GROUP BY
                            ALL
                        ),
                        previous_agg AS (
                          SELECT
                            submission_date,
                            COALESCE(column_1, '???????') AS column_1,
                            SUM(metric_1) AS metric_1
                          FROM
                            `moz-fx-data-shared-prod.test.test_query_v1`
                          WHERE
                            submission_date = @submission_date
                          GROUP BY
                            ALL
                        ),
                        shredded AS (
                          SELECT
                            previous_agg.submission_date,
                            previous_agg.column_1,
                            CAST(NULL AS STRING) AS column_2,
                            COALESCE(previous_agg.metric_1, 0) - COALESCE(new_agg.metric_1, 0) AS metric_1
                          FROM
                            previous_agg
                          LEFT JOIN
                            new_agg
                            ON previous_agg.submission_date = new_agg.submission_date
                            AND previous_agg.column_1 = new_agg.column_1
                          WHERE
                            COALESCE(previous_agg.metric_1, 0) > COALESCE(new_agg.metric_1, 0)
                        )
                        SELECT
                          IF(column_1 = '???????', CAST(NULL AS STRING), column_1) AS column_1,
                          IF(column_2 = '???????', CAST(NULL AS STRING), column_2) AS column_2,
                          metric_1
                        FROM
                          new_version
                        UNION ALL
                        SELECT
                          IF(column_1 = '???????', CAST(NULL AS STRING), column_1) AS column_1,
                          IF(column_2 = '???????', CAST(NULL AS STRING), column_2) AS column_2,
                          metric_1
                        FROM
                          shredded""",
        )

        existing_schema = {
            "fields": [
                {"name": "column_1", "type": "DATE", "mode": "NULLABLE"},
                {"name": "metric_1", "type": "INTEGER", "mode": "NULLABLE"},
            ]
        }
        new_schema = {
            "fields": [
                {"name": "column_1", "type": "DATE", "mode": "NULLABLE"},
                {"name": "column_2", "type": "STRING", "mode": "NULLABLE"},
                {"name": "metric_1", "type": "INTEGER", "mode": "NULLABLE"},
            ]
        }

        with runner.isolated_filesystem():
            os.makedirs(self.path, exist_ok=True)
            os.makedirs(self.path_previous, exist_ok=True)
            with open(self.path / "query.sql", "w") as f:
                f.write(
                    "SELECT column_1, column_2, metric_1 FROM upstream_1"
                    " GROUP BY column_1, column_2"
                )
            with open(self.path_previous / "query.sql", "w") as f:
                f.write("SELECT column_1, metric_1 FROM upstream_1 GROUP BY column_1")

            with open(self.path / "schema.yaml", "w") as f:
                f.write(yaml.safe_dump(new_schema))

            with open(self.path_previous / "schema.yaml", "w") as f:
                f.write(yaml.safe_dump(existing_schema))

            with open(Path(self.path) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    "
                    "field: submission_date\n    require_partition_filter: true"
                )
            with open(Path(self.path_previous) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    "
                    "field: submission_date\n    require_partition_filter: true"
                )

            mock_classify_columns.return_value = (
                [
                    Column(
                        "column_1",
                        DataTypeGroup.STRING,
                        ColumnType.DIMENSION,
                        ColumnStatus.COMMON,
                    )
                ],
                [
                    Column(
                        "column_2",
                        DataTypeGroup.STRING,
                        ColumnType.DIMENSION,
                        ColumnStatus.ADDED,
                    )
                ],
                [],
                [
                    Column(
                        "metric_1",
                        DataTypeGroup.INTEGER,
                        ColumnType.METRIC,
                        ColumnStatus.COMMON,
                    )
                ],
                [],
            )

            with patch.object(
                Subset,
                "get_query_path_results",
                return_value=[{"column_1": "ABC", "column_2": "DEF", "metric_1": 10.0}],
            ):
                assert os.path.isfile(self.path / "query.sql")
                assert os.path.isfile(self.path_previous / "query.sql")
                result = generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=self.project_id,
                    dataset=self.dataset,
                    destination_table=self.destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
                assert result[0] == expected[0]
                assert result[1] == expected[1].replace("                        ", "")

    @patch("google.cloud.bigquery.Client")
    def test_missing_previous_version(self, mock_client, runner):
        """Test that the process raises an exception when previous query version is missing."""
        expected_exc = (
            "Function extract_last_group_by_from_query() called without an sql file or "
            "text to extract the group by."
        )

        with runner.isolated_filesystem():
            path = f"sql/{self.project_id}/{self.dataset}/{self.destination_table}"
            os.makedirs(path, exist_ok=True)
            with open(Path(path) / "query.sql", "w") as f:
                f.write("SELECT column_1, column_2 FROM upstream_1 GROUP BY column_1")

            with pytest.raises(ClickException) as e:
                generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=self.project_id,
                    dataset=self.dataset,
                    destination_table=self.destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
            assert (str(e.value.message)) == expected_exc
            assert (e.type) == ClickException

    @patch("google.cloud.bigquery.Client")
    def test_invalid_group_by(self, mock_client, runner):
        """Test that the process raises an exception when the GROUP BY is invalid for any query."""
        expected_exc = (
            "GROUP BY must use an explicit list of columns. "
            "Avoid expressions like `GROUP BY ALL` or `GROUP BY 1, 2, 3`."
        )
        # client = bigquery.Client()
        project_id = "moz-fx-data-shared-prod"
        dataset = "test"
        destination_table = "test_query_v2"
        destination_table_previous = "test_query_v1"

        # GROUP BY including a number
        with runner.isolated_filesystem():
            previous_group_by = "column_1, column_2, column_3"
            new_group_by = "3, column_4, column_5"
            path = f"sql/{project_id}/{dataset}/{destination_table}"
            path_previous = f"sql/{project_id}/{dataset}/{destination_table_previous}"
            os.makedirs(path, exist_ok=True)
            os.makedirs(path_previous, exist_ok=True)
            with open(Path(path) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {new_group_by}"
                )
            with open(Path(path_previous) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {previous_group_by}"
                )

            with pytest.raises(ClickException) as e:
                generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=project_id,
                    dataset=dataset,
                    destination_table=destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
            assert (str(e.value.message)) == expected_exc

            # GROUP BY 1, 2, 3
            previous_group_by = "1, 2, 3"
            new_group_by = "column_1, column_2, column_3"
            with open(Path(path) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {new_group_by}"
                )
            with open(Path(path_previous) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {previous_group_by}"
                )
            with pytest.raises(ClickException) as e:
                generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=project_id,
                    dataset=dataset,
                    destination_table=destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
            assert (str(e.value.message)) == expected_exc

            # GROUP BY ALL
            previous_group_by = "column_1, column_2, column_3"
            new_group_by = "ALL"
            with open(Path(path) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {new_group_by}"
                )
            with open(Path(path_previous) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {previous_group_by}"
                )
            with pytest.raises(ClickException) as e:
                generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=project_id,
                    dataset=dataset,
                    destination_table=destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
            assert (str(e.value.message)) == expected_exc

            # GROUP BY is missing
            previous_group_by = "column_1, column_2, column_3"
            with open(Path(path) / "query.sql", "w") as f:
                f.write("SELECT column_1, column_2 FROM upstream_1")
            with open(Path(path_previous) / "query.sql", "w") as f:
                f.write(
                    f"SELECT column_1, column_2 FROM upstream_1 GROUP BY {previous_group_by}"
                )
            with pytest.raises(ClickException) as e:
                generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=project_id,
                    dataset=dataset,
                    destination_table=destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
            assert (str(e.value.message)) == expected_exc

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.backfill.shredder_mitigation.classify_columns")
    def test_generate_query_called_with_correct_parameters(
        self, mock_classify_columns, mock_client, runner
    ):
        """Test that function generate_query is called with the correct parameters."""
        existing_schema = {
            "fields": [
                {"name": "column_1", "type": "DATE", "mode": "NULLABLE"},
                {"name": "metric_1", "type": "INTEGER", "mode": "NULLABLE"},
            ]
        }
        new_schema = {
            "fields": [
                {"name": "column_1", "type": "DATE", "mode": "NULLABLE"},
                {"name": "column_2", "type": "STRING", "mode": "NULLABLE"},
                {"name": "metric_1", "type": "INTEGER", "mode": "NULLABLE"},
            ]
        }

        with runner.isolated_filesystem():
            os.makedirs(self.path, exist_ok=True)
            os.makedirs(self.path_previous, exist_ok=True)
            with open(Path(self.path) / "query.sql", "w") as f:
                f.write("SELECT column_1 FROM upstream_1 GROUP BY column_1")
            with open(Path(self.path) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    "
                    "field: submission_date\n    require_partition_filter: true"
                )
            with open(Path(self.path_previous) / "query.sql", "w") as f:
                f.write("SELECT column_1 FROM upstream_1 GROUP BY column_1")
            with open(Path(self.path_previous) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    "
                    "field: submission_date\n    require_partition_filter: true"
                )
            with open(self.path / "schema.yaml", "w") as f:
                f.write(yaml.safe_dump(new_schema))
            with open(
                self.path_previous / "schema.yaml",
                "w",
            ) as f:
                f.write(yaml.safe_dump(existing_schema))

            mock_classify_columns.return_value = (
                [
                    Column(
                        "column_1",
                        DataTypeGroup.STRING,
                        ColumnType.DIMENSION,
                        ColumnStatus.COMMON,
                    )
                ],
                [
                    Column(
                        "column_2",
                        DataTypeGroup.STRING,
                        ColumnType.DIMENSION,
                        ColumnStatus.ADDED,
                    )
                ],
                [],
                [
                    Column(
                        "metric_1",
                        DataTypeGroup.INTEGER,
                        ColumnType.METRIC,
                        ColumnStatus.COMMON,
                    )
                ],
                [],
            )

            with patch.object(
                Subset,
                "get_query_path_results",
                return_value=[{"column_1": "ABC", "column_2": "DEF", "metric_1": 10.0}],
            ):
                with patch.object(Subset, "generate_query") as mock_generate_query:
                    generate_query_with_shredder_mitigation(
                        client=mock_client,
                        project_id=self.project_id,
                        dataset=self.dataset,
                        destination_table=self.destination_table,
                        backfill_date=PREVIOUS_DATE,
                    )
                    assert mock_generate_query.call_count == 3
                    assert mock_generate_query.call_args_list == (
                        [
                            call(
                                select_list=[
                                    "submission_date",
                                    "COALESCE(column_1, '???????') AS column_1",
                                    "SUM(metric_1) AS metric_1",
                                ],
                                from_clause="new_version",
                                group_by_clause="ALL",
                            ),
                            call(
                                select_list=[
                                    "submission_date",
                                    "COALESCE(column_1, '???????') AS column_1",
                                    "SUM(metric_1) AS metric_1",
                                ],
                                from_clause="`moz-fx-data-shared-prod.test.test_query_v1`",
                                where_clause="submission_date = @submission_date",
                                group_by_clause="ALL",
                            ),
                            call(
                                select_list=[
                                    "previous_agg.submission_date",
                                    "previous_agg.column_1",
                                    "CAST(NULL AS STRING) AS column_2",
                                    "COALESCE(previous_agg.metric_1, 0) - "
                                    "COALESCE(new_agg.metric_1, 0) AS metric_1",
                                ],
                                from_clause="previous_agg LEFT JOIN new_agg ON "
                                "previous_agg.submission_date = new_agg.submission_date"
                                " AND previous_agg.column_1 = new_agg.column_1 ",
                                where_clause="COALESCE(previous_agg.metric_1, 0) >"
                                " COALESCE(new_agg.metric_1, 0)",
                            ),
                        ]
                    )
