import os
from datetime import datetime, time
from pathlib import Path
from unittest.mock import call, patch

import pytest
from click.exceptions import ClickException
from click.testing import CliRunner

from bigquery_etl.backfill.shredder_mitigation import (
    PREVIOUS_DATE,
    Column,
    ColumnStatus,
    ColumnType,
    DataTypeGroup,
    Subset,
    classify_columns,
    generate_query_with_shredder_mitigation,
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

    def test_matching_new_row_and_new_columns(self):
        new_row = {
            "submission_date": "2024-01-01",
            "channel": None,
            "os": "Mac",
            "metric_int": 2024,
        }
        existing_columns = ["submission_date", "channel"]
        new_columns = ["submission_date", "channel", "os", "is_default_browser"]
        expected_exception_text = "Some dimension columns are not returned when running the query: ['is_default_browser']"
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
        assert get_bigquery_type("12:34:56") == DataTypeGroup.TIME
        assert get_bigquery_type(time(12, 34, 56)) == DataTypeGroup.TIME
        assert get_bigquery_type(datetime(2024, 12, 26)) == DataTypeGroup.DATE

    def test_other_types(self):
        assert get_bigquery_type("2024") == DataTypeGroup.STRING
        assert get_bigquery_type(None) == DataTypeGroup.UNDETERMINED


class TestSubset(object):
    # TODO:
    def test_generate_query(self):
        """Test cases: aggregate, different columns, different metrics, missing metrics, added columns / metrics"""
        assert True

    def test_get_query_path(self):
        """Test that path exists / test that the path contains a query file."""
        assert True

    def test_get_query_path_results(self):
        """Test with sample data and sample results"""
        assert True

    def test_generate_check_with_previous_version(self):
        assert True


class TestGenerateQueryWithShredderMitigation(object):
    """Test the function that generates the query for the backfill."""

    project_id = "moz-fx-data-shared-prod"
    dataset = "test"
    destination_table = "test_query_v2"
    destination_table_previous = "test_query_v1"
    path_new = Path("sql") / project_id / dataset / destination_table
    path_previous = Path("sql") / project_id / dataset / destination_table_previous

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.backfill.shredder_mitigation.classify_columns")
    def test_generate_query_as_expected(
        self, mock_classify_columns, mock_client, runner
    ):
        """Test that query is generated as expected given a set of mock dimensions and metrics."""

        with runner.isolated_filesystem():
            os.makedirs(self.path_new, exist_ok=True)
            os.makedirs(self.path_previous, exist_ok=True)
            with open(
                Path("sql")
                / self.project_id
                / self.dataset
                / self.destination_table
                / "query.sql",
                "w",
            ) as f:
                f.write(
                    "SELECT column_1, column_2, metric_1 FROM upstream_1 GROUP BY column_1, column_2"
                )
            with open(
                Path("sql")
                / self.project_id
                / self.dataset
                / self.destination_table_previous
                / "query.sql",
                "w",
            ) as f:
                f.write("SELECT column_1, metric_1 FROM upstream_1 GROUP BY column_1")

            with open(Path(self.path_new) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    field: submission_date\n    require_partition_filter: true"
                )
            with open(Path(self.path_previous) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    field: submission_date\n    require_partition_filter: true"
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
                        DataTypeGroup.NUMERIC,
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
                result = generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=self.project_id,
                    dataset=self.dataset,
                    destination_table=self.destination_table,
                    backfill_date=PREVIOUS_DATE,
                )

                expected = """-- Query generated from a template that mitigates the effect of shredder.
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
                    COALESCE(column_1, '??') AS column_1,
                    SUM(metric_1) AS metric_1
                  FROM
                    new_version
                  GROUP BY
                    ALL
                ),
                previous_agg AS (
                  SELECT
                    submission_date,
                    COALESCE(column_1, '??') AS column_1,
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
                    previous_agg.metric_1 - IFNULL(new_agg.metric_1, 0) AS metric_1
                  FROM
                    previous_agg
                  LEFT JOIN
                    new_agg
                    ON previous_agg.submission_date = new_agg.submission_date
                    AND previous_agg.column_1 = new_agg.column_1
                  WHERE
                    previous_agg.metric_1 > IFNULL(new_agg.metric_1, 0)
                )
                SELECT
                  column_1,
                  column_2,
                  metric_1
                FROM
                  new_version
                UNION ALL
                SELECT
                  column_1,
                  column_2,
                  metric_1
                FROM
                  shredded;"""
                assert result == expected.replace("                ", "")

    @patch("google.cloud.bigquery.Client")
    def test_missing_previous_version(self, mock_client, runner):
        """Test that the process raises an exception when previous query version is missing."""
        expected_exc = f"Query file not found for `{self.project_id}.{self.dataset}.{self.destination_table_previous}`."

        with runner.isolated_filesystem():
            path_new = f"sql/{self.project_id}/{self.dataset}/{self.destination_table}"
            os.makedirs(path_new, exist_ok=True)
            with open(Path(path_new) / "query.sql", "w") as f:
                f.write("SELECT column_1, column_2 FROM upstream_1 GROUP BY column_1")

            with pytest.raises(ClickException) as e:
                generate_query_with_shredder_mitigation(
                    client=mock_client,
                    project_id=self.project_id,
                    dataset=self.dataset,
                    destination_table=self.destination_table,
                    backfill_date=PREVIOUS_DATE,
                )
            assert (str(e.value)) == expected_exc
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
            path_new = f"sql/{project_id}/{dataset}/{destination_table}"
            path_previous = f"sql/{project_id}/{dataset}/{destination_table_previous}"
            os.makedirs(path_new, exist_ok=True)
            os.makedirs(path_previous, exist_ok=True)
            with open(Path(path_new) / "query.sql", "w") as f:
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
            assert (str(e.value)) == expected_exc

            # GROUP BY 1, 2, 3
            previous_group_by = "1, 2, 3"
            new_group_by = "column_1, column_2, column_3"
            with open(Path(path_new) / "query.sql", "w") as f:
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
            assert (str(e.value)) == expected_exc

            # GROUP BY ALL
            previous_group_by = "column_1, column_2, column_3"
            new_group_by = "ALL"
            with open(Path(path_new) / "query.sql", "w") as f:
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
            assert (str(e.value)) == expected_exc

            # GROUP BY is missing
            previous_group_by = "column_1, column_2, column_3"
            with open(Path(path_new) / "query.sql", "w") as f:
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
            assert (str(e.value)) == expected_exc

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.backfill.shredder_mitigation.classify_columns")
    def test_generate_query_called_with_correct_parameters(
        self, mock_classify_columns, mock_client, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs(self.path_new, exist_ok=True)
            os.makedirs(self.path_previous, exist_ok=True)
            with open(Path(self.path_new) / "query.sql", "w") as f:
                f.write("SELECT column_1 FROM upstream_1 GROUP BY column_1")
            with open(Path(self.path_new) / "metadata.yaml", "w") as f:
                f.write(
                    "bigquery:\n  time_partitioning:\n    type: day\n    field: submission_date\n    require_partition_filter: true"
                )
            with open(Path(self.path_previous) / "query.sql", "w") as f:
                f.write("SELECT column_1 FROM upstream_1 GROUP BY column_1")
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
                        DataTypeGroup.NUMERIC,
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
                    mock_generate_query.assert_has_calls(
                        [
                            call(
                                select_list=[
                                    "submission_date",
                                    "COALESCE(column_1, '??') AS column_1",
                                    "SUM(metric_1) AS metric_1",
                                ],
                                from_clause="FROM new_version",
                                group_by_clause="GROUP BY ALL",
                            ),
                            call(
                                select_list=[
                                    "submission_date",
                                    "COALESCE(column_1, '??') AS column_1",
                                    "SUM(metric_1) AS metric_1",
                                ],
                                from_clause="FROM `moz-fx-data-shared-prod.test.test_query_v1`",
                                where_clause="WHERE submission_date = @submission_date",
                                group_by_clause="GROUP BY ALL",
                            ),
                            call(
                                select_list=[
                                    "previous_agg.submission_date",
                                    "previous_agg.column_1",
                                    "CAST(NULL AS STRING) AS column_2",
                                    "previous_agg.metric_1 - IFNULL(new_agg.metric_1, 0) AS metric_1",
                                ],
                                from_clause="FROM previous_agg LEFT JOIN new_agg ON previous_agg.submission_date = "
                                "new_agg.submission_date AND previous_agg.column_1 = new_agg.column_1 ",
                                where_clause="WHERE previous_agg.metric_1 > IFNULL(new_agg.metric_1, 0)",
                            ),
                        ]
                    )
