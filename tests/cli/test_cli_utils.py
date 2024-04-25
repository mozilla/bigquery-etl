from pathlib import Path

import pytest
from click.exceptions import BadParameter

from bigquery_etl.cli.utils import (
    is_authenticated,
    is_valid_dir,
    is_valid_file,
    is_valid_project,
    qualify_table_references,
    table_matches_patterns,
)

TEST_DIR = Path(__file__).parent.parent


class TestUtils:
    def test_is_valid_dir(self):
        with pytest.raises(BadParameter):
            assert is_valid_dir(None, None, "invalid")
        with pytest.raises(BadParameter):
            assert is_valid_dir(None, None, str(TEST_DIR / "data" / "dags.yaml"))
        assert is_valid_dir(None, None, str(TEST_DIR)) == str(TEST_DIR)

    def test_is_valid_file(self):
        with pytest.raises(BadParameter):
            assert is_valid_file(None, None, "invalid")
        with pytest.raises(BadParameter):
            assert is_valid_file(None, None, str(TEST_DIR))
        assert is_valid_file(None, None, str(TEST_DIR / "data" / "dags.yaml")) == str(
            TEST_DIR / "data" / "dags.yaml"
        )

    @pytest.mark.integration
    def test_is_authenticated(self):
        assert is_authenticated()

    def test_is_valid_project(self):
        assert is_valid_project(None, None, "mozfun")
        assert is_valid_project(None, None, "moz-fx-data-shared-prod")
        assert is_valid_project(None, None, "moz-fx-data-backfill-1")
        with pytest.raises(BadParameter):
            assert is_valid_project(None, None, "not-existing")

    def test_table_matches_patterns(self):
        assert not table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern=["telemetry_live.main_v4", "telemetry_live.event_v4"],
            invert=True,
        )
        assert not table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern="telemetry_live.main_v4",
            invert=True,
        )

        assert table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern=["telemetry_live.first_shutdown_v4", "telemetry_live.event_v4"],
            invert=True,
        )
        assert table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern="telemetry_live.event_v4",
            invert=True,
        )

    def test_qualify_table_references_tables(self):
        """Ensure tables/views are properly qualified with dataset and project id."""
        default_project = "default-project"
        default_dataset = "default_dataset"

        input_sql = """
        WITH has_project AS (
            SELECT 1 FROM proj-ect.dataset.table1
        ),
        has_dataset AS (
            SELECT 1 FROM dataset.table1
        ),
        no_dataset AS (
            SELECT 1 AS table1 FROM table1
        ),
        has_join AS (
            SELECT
                1
            FROM
                table1
            LEFT JOIN table2
            USING (_id)
        ),
        dataset_backticks AS (
            SELECT 1 FROM `dataset.table1`
        ),
        dataset_backticks2 AS (
            SELECT 1 FROM `dataset`.`table1`
        ),
        alias AS (
            SELECT 1 FROM table1 AS table1
        ),
        table_name_cte AS (
            SELECT 1 FROM dataset.table_name_cte
        ),
        table_name_column AS (
            SELECT table_column, table1, table2 FROM dataset.table_column
        ),
        implicit_cross_join AS (
            SELECT
                *
            FROM
                table1,
                table2
        )
        SELECT 1 FROM table_name_cte CROSS JOIN has_dataset
        """

        expected = f"""
        WITH has_project AS (
            SELECT 1 FROM proj-ect.dataset.table1
        ),
        has_dataset AS (
            SELECT 1 FROM `{default_project}`.`dataset`.`table1`
        ),
        no_dataset AS (
            SELECT 1 AS table1 FROM `{default_project}`.`{default_dataset}`.`table1`
        ),
        has_join AS (
            SELECT
                1
            FROM
                `{default_project}`.`{default_dataset}`.`table1`
            LEFT JOIN `{default_project}`.`{default_dataset}`.`table2`
            USING (_id)
        ),
        dataset_backticks AS (
            SELECT 1 FROM `{default_project}`.`dataset`.`table1`
        ),
        dataset_backticks2 AS (
            SELECT 1 FROM `{default_project}`.`dataset`.`table1`
        ),
        alias AS (
            SELECT 1 FROM `{default_project}`.`{default_dataset}`.`table1` AS table1
        ),
        table_name_cte AS (
            SELECT 1 FROM `{default_project}`.`dataset`.`table_name_cte`
        ),
        table_name_column AS (
            SELECT table_column, table1, table2 FROM `{default_project}`.`dataset`.`table_column`
        ),
        implicit_cross_join AS (
            SELECT
                *
            FROM
                `{default_project}`.`{default_dataset}`.`table1`,
                `{default_project}`.`{default_dataset}`.`table2`
        )
        SELECT 1 FROM table_name_cte CROSS JOIN has_dataset
        """

        actual = qualify_table_references(input_sql, default_project, default_dataset)

        assert actual == expected

    def test_qualify_table_references_information_schema(self):
        """Ensure information schema are region part is parsed correctly."""
        default_project = "default-project"
        default_dataset = "default_dataset"

        input_sql = """
        WITH info_schema_region AS (
            SELECT 1 FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        ),
        info_schema_project AS (
            SELECT 1 FROM proj.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        ),
        info_schema_full AS (
            SELECT * FROM `project.region-us.INFORMATION_SCHEMA.SCHEMATA`
        ),
        info_schema_none AS (
            SELECT * FROM INFORMATION_SCHEMA.SCHEMATA
        )
        SELECT 1
        """

        expected = f"""
        WITH info_schema_region AS (
            SELECT 1 FROM `{default_project}`.`region-us`.`INFORMATION_SCHEMA`.`JOBS_BY_PROJECT`
        ),
        info_schema_project AS (
            SELECT 1 FROM proj.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        ),
        info_schema_full AS (
            SELECT * FROM `project.region-us.INFORMATION_SCHEMA.SCHEMATA`
        ),
        info_schema_none AS (
            SELECT * FROM `{default_project}`.`INFORMATION_SCHEMA`.`SCHEMATA`
        )
        SELECT 1
        """

        actual = qualify_table_references(input_sql, default_project, default_dataset)

        assert actual == expected

    def test_qualify_table_references_udfs(self):
        """Ensure udf are properly qualified with dataset and project id."""
        default_project = "default-project"
        default_dataset = "default_dataset"

        input_sql = """
        CREATE TEMP FUNCTION temp_cte() AS (
            123
        );

        SELECT
            temp_cte(),
            ARRAY_LENGTH(l),
            mozfun.dataset.func(1),
            udf.func_1(1),
            udf.nested1(udf.nested2(2)),
            `udf.func_1(3)`,
            my-project.udf.func_3(3),
            udf_js.js_func(),
        FROM
            table1
        """

        expected = f"""
        CREATE TEMP FUNCTION temp_cte() AS (
            123
        );

        SELECT
            temp_cte(),
            ARRAY_LENGTH(l),
            mozfun.dataset.func(1),
            `{default_project}.udf.func_1`(1),
            `{default_project}.udf.nested1`(`{default_project}.udf.nested2`(2)),
            `{default_project}.udf.func_1`(3)`,
            my-project.udf.func_3(3),
            `{default_project}.udf_js.js_func`(),
        FROM
            `{default_project}`.`{default_dataset}`.`table1`
        """

        actual = qualify_table_references(input_sql, default_project, default_dataset)

        assert actual == expected
