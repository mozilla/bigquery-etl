import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from bigquery_etl.cli.utils import is_valid_dir
from bigquery_etl.util.common import (
    extract_last_group_by_from_query,
    project_dirs,
    qualify_table_references_in_file,
    render,
)


class TestUtilCommon:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_project_dirs(self):
        assert project_dirs("test") == ["sql/test"]

        existing_projects = project_dirs()
        assert "sql/moz-fx-data-shared-prod" in existing_projects
        assert "sql/bigconfig.yml" not in existing_projects

    def test_metrics_render(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['days_of_use'],
                    platform='firefox_desktop'
                ) }}
            )
        """
        )
        rendered_sql = render(file_path.name, template_folder=file_path.parent)
        assert r"{{ metrics.calculate" not in rendered_sql
        assert "days_of_use" in rendered_sql

    def test_non_existing_metrics_render(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['not-existing'],
                    platform='firefox_desktop'
                ) }}
            )
        """
        )

        with pytest.raises(ValueError):
            render(file_path.name, template_folder=file_path.parent)

    def test_render_multiple_metrics(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['days_of_use', 'uri_count', 'ad_clicks'],
                    platform='firefox_desktop',
                    group_by={'sample_id': 'sample_id'},
                    where='submission_date = "2023-01-01"'
                ) }}
            )
        """
        )
        rendered_sql = render(file_path.name, template_folder=file_path.parent)
        assert "metrics.calculate" not in rendered_sql
        assert r"{{" not in rendered_sql
        assert "days_of_use" in rendered_sql
        assert "clients_daily" in rendered_sql
        assert "uri_count" in rendered_sql
        assert "ad_clicks" in rendered_sql
        assert "mozdata.search.search_clients_engines_sources_daily" in rendered_sql
        assert 'submission_date = "2023-01-01"' in rendered_sql
        assert "sample_id" in rendered_sql

    def test_render_data_source(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.data_source(
                    data_source="main",
                    platform='firefox_desktop',
                    where='submission_date = "2023-01-01"'
                ) }}
            )
        """
        )
        rendered_sql = render(file_path.name, template_folder=file_path.parent)
        assert "metrics.data_source" not in rendered_sql
        assert r"{{" not in rendered_sql
        assert "main" in rendered_sql
        assert 'submission_date = "2023-01-01"' in rendered_sql

    def test_checks_render(self, tmp_path):
        file_path = tmp_path / "checks.sql"
        file_path.write_text(
            r"""
            {{ min_row_count(1, "submission_date = @submission_date") }}
        """
        )
        kwargs = {
            "project_id": "project",
            "dataset_id": "dataset",
            "table_name": "table",
        }
        rendered_sql = render(
            file_path.name, template_folder=file_path.parent, **kwargs
        )
        assert (
            r"""{{ min_row_count(1, "submission_date = @submission_date") }}"""
            not in rendered_sql
        )
        assert "SELECT" in rendered_sql
        assert "`project.dataset.table`" in rendered_sql

    def test_qualify_table_references_in_file(self, tmp_path):
        query = "SELECT * FROM test LEFT JOIN other.joined_query"
        query_path = tmp_path / "project" / "dataset" / "test"
        query_path.mkdir(parents=True, exist_ok=True)
        query_path = query_path / "query.sql"
        query_path.write_text(query)
        result = qualify_table_references_in_file(query_path)
        expected = "SELECT * FROM `project.dataset.test` LEFT JOIN `project.other.joined_query`"
        assert result == expected

        query = "SELECT * FROM region-US.INFORMATION_SCHEMA.JOBS_BY_USER"
        query_path.write_text(query)
        result = qualify_table_references_in_file(query_path)
        expected = "SELECT * FROM `project.region-US.INFORMATION_SCHEMA.JOBS_BY_USER`"
        assert result == expected

        query = "{% if is_init() %} SELECT * FROM test {% else %} SELECT * FROM other {% endif %}"
        query_path.write_text(query)
        result = qualify_table_references_in_file(query_path)
        expected = "{% if is_init() %} SELECT * FROM `project.dataset.test` {% else %} SELECT * FROM `project.dataset.other` {% endif %}"
        assert result == expected

    def test_qualify_table_references_in_file_udf(self, tmp_path):
        udf = "SELECT * FROM test"
        udf_path = tmp_path / "project" / "dataset" / "test"
        udf_path.mkdir(parents=True, exist_ok=True)
        udf_path = udf_path / "udf.sql"
        udf_path.write_text(udf)

        with pytest.raises(NotImplementedError):
            qualify_table_references_in_file(udf_path)

    def test_qualify_table_references_in_file_scripts(self, tmp_path):
        script = "SELECT * FROM test"
        script_path = tmp_path / "project" / "dataset" / "test"
        script_path.mkdir(parents=True, exist_ok=True)
        script_path = script_path / "script.sql"
        script_path.write_text(script)

        with pytest.raises(NotImplementedError):
            qualify_table_references_in_file(script_path)

        query = "DECLARE some_var; SELECT * FROM test"
        query_path = tmp_path / "project" / "dataset" / "test"
        query_path.mkdir(parents=True, exist_ok=True)
        query_path = query_path / "query.sql"
        query_path.write_text(query)

        with pytest.raises(NotImplementedError):
            qualify_table_references_in_file(query_path)

    def test_qualify_table_references_in_file_array_fields(self, tmp_path):
        query = """
        WITH data AS (
            SELECT ARRAY_AGG(DISTINCT some_field IGNORE NULLS) AS array_field,
        )
        SELECT
            @submission_date AS submission_date,
            udf.some_udf(
                ARRAY(
                    SELECT
                        STRUCT(key, CAST(TRUE AS INT64) AS value)
                    FROM
                        data.array_field AS key
                )
            ) AS array_field
        FROM data
        """
        query_path = tmp_path / "project" / "dataset" / "test"
        query_path.mkdir(parents=True, exist_ok=True)
        query_path = query_path / "query.sql"
        query_path.write_text(query)
        result = qualify_table_references_in_file(query_path)
        expected = """
        WITH data AS (
            SELECT ARRAY_AGG(DISTINCT some_field IGNORE NULLS) AS array_field,
        )
        SELECT
            @submission_date AS submission_date,
            `project.udf.some_udf`(
                ARRAY(
                    SELECT
                        STRUCT(key, CAST(TRUE AS INT64) AS value)
                    FROM
                        data.array_field AS key
                )
            ) AS array_field
        FROM data
        """
        assert result == expected

    def test_qualify_table_references_tables(self, tmp_path):
        """Ensure tables/views are properly qualified with dataset and project id."""
        default_project = "default-project"
        default_dataset = "default_dataset"

        query_path = tmp_path / default_project / default_dataset / "test"
        query_path.mkdir(parents=True, exist_ok=True)

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
            SELECT 1 FROM `dataset.table1`
        ),
        alias AS (
            SELECT 1 FROM table1
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

        query_path = query_path / "query.sql"
        query_path.write_text(input_sql)

        expected = f"""
        WITH has_project AS (
            SELECT 1 FROM proj-ect.dataset.table1
        ),
        has_dataset AS (
            SELECT 1 FROM `{default_project}.dataset.table1`
        ),
        no_dataset AS (
            SELECT 1 AS table1 FROM `{default_project}.{default_dataset}.table1`
        ),
        has_join AS (
            SELECT
                1
            FROM
                `{default_project}.{default_dataset}.table1`
            LEFT JOIN `{default_project}.{default_dataset}.table2`
            USING (_id)
        ),
        dataset_backticks AS (
            SELECT 1 FROM `{default_project}.dataset.table1`
        ),
        dataset_backticks2 AS (
            SELECT 1 FROM `{default_project}.dataset.table1`
        ),
        alias AS (
            SELECT 1 FROM `{default_project}.{default_dataset}.table1`
        ),
        table_name_cte AS (
            SELECT 1 FROM `{default_project}.dataset.table_name_cte`
        ),
        table_name_column AS (
            SELECT table_column, table1, table2 FROM `{default_project}.dataset.table_column`
        ),
        implicit_cross_join AS (
            SELECT
                *
            FROM
                `{default_project}.{default_dataset}.table1`,
                `{default_project}.{default_dataset}.table2`
        )
        SELECT 1 FROM table_name_cte CROSS JOIN has_dataset
        """

        actual = qualify_table_references_in_file(query_path)

        assert actual == expected

    def test_qualify_table_references_information_schema(self, tmp_path):
        """Ensure information schema are region part is parsed correctly."""
        default_project = "default-project"
        default_dataset = "default_dataset"

        query_path = tmp_path / default_project / default_dataset / "test"
        query_path.mkdir(parents=True, exist_ok=True)

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

        query_path = query_path / "query.sql"
        query_path.write_text(input_sql)

        expected = f"""
        WITH info_schema_region AS (
            SELECT 1 FROM `{default_project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        ),
        info_schema_project AS (
            SELECT 1 FROM proj.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        ),
        info_schema_full AS (
            SELECT * FROM `project.region-us.INFORMATION_SCHEMA.SCHEMATA`
        ),
        info_schema_none AS (
            SELECT * FROM `{default_project}.INFORMATION_SCHEMA.SCHEMATA`
        )
        SELECT 1
        """

        actual = qualify_table_references_in_file(query_path)

        assert actual == expected

    def test_qualify_table_references_udfs(self, tmp_path):
        """Ensure udf are properly qualified with dataset and project id."""
        default_project = "default-project"
        default_dataset = "default_dataset"

        query_path = tmp_path / default_project / default_dataset / "test"
        query_path.mkdir(parents=True, exist_ok=True)

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

        query_path = query_path / "query.sql"
        query_path.write_text(input_sql)

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
            `{default_project}.{default_dataset}.table1`
        """

        actual = qualify_table_references_in_file(query_path)

        assert actual == expected

    def test_extract_last_group_by_from_query_file(self, runner):
        """Test cases using a sql file path."""
        with runner.isolated_filesystem():
            test_path = (
                "sql/moz-fx-data-shared-prod/test_shredder_mitigation/test_query_v1"
            )
            os.makedirs(test_path)
            assert os.path.exists(test_path)
            assert "test_shredder_mitigation" in os.listdir(
                "sql/moz-fx-data-shared-prod"
            )
            assert is_valid_dir(None, None, test_path)

            sql_path = Path(test_path) / "query.sql"
            with open(sql_path, "w") as f:
                f.write("SELECT column_1 FROM test_table group by ALL")
            assert ["ALL"] == extract_last_group_by_from_query(sql_path=sql_path)

            with open(sql_path, "w") as f:
                f.write(
                    "SELECT column_1 FROM test_table GROUP BY (column_1) LIMIT (column_1);"
                )
            assert ["column_1"] == extract_last_group_by_from_query(sql_path=sql_path)

    def test_extract_last_group_by_from_query_sql(self):
        """Test cases using a sql text."""
        assert ["ALL"] == extract_last_group_by_from_query(
            sql_text="SELECT column_1 FROM test_table GROUP BY ALL"
        )
        assert ["1"] == extract_last_group_by_from_query(
            sql_text="SELECT column_1, SUM(metric_1) AS metric_1 FROM test_table GROUP BY 1;"
        )
        assert ["1", "2", "3"] == extract_last_group_by_from_query(
            sql_text="SELECT column_1 FROM test_table GROUP BY 1, 2, 3"
        )
        assert ["1", "2", "3"] == extract_last_group_by_from_query(
            sql_text="SELECT column_1 FROM test_table GROUP BY 1, 2, 3"
        )
        assert ["column_1", "column_2"] == extract_last_group_by_from_query(
            sql_text="""SELECT column_1, column_2 FROM test_table GROUP BY column_1, column_2 ORDER BY 1 LIMIT 100"""
        )
        assert [] == extract_last_group_by_from_query(
            sql_text="SELECT column_1 FROM test_table"
        )
        assert [] == extract_last_group_by_from_query(
            sql_text="SELECT column_1 FROM test_table;"
        )
        assert ["column_1"] == extract_last_group_by_from_query(
            sql_text="SELECT column_1 FROM test_table GROUP BY column_1"
        )
        assert ["column_1", "column_2"] == extract_last_group_by_from_query(
            sql_text="SELECT column_1, column_2 FROM test_table GROUP BY (column_1, column_2)"
        )
        assert ["column_1"] == extract_last_group_by_from_query(
            sql_text="""WITH cte AS (SELECT column_1 FROM test_table GROUP BY column_1)
            SELECT column_1 FROM cte"""
        )
        assert ["column_1"] == extract_last_group_by_from_query(
            sql_text="""WITH cte AS (SELECT column_1 FROM test_table GROUP BY column_1),
            cte2 AS (SELECT column_1, column2 FROM test_table GROUP BY column_1, column2)
            SELECT column_1 FROM cte2 GROUP BY column_1 ORDER BY 1 DESC LIMIT 1;"""
        )
        assert ["column_3"] == extract_last_group_by_from_query(
            sql_text="""WITH cte1 AS (SELECT column_1, column3 FROM test_table GROUP BY column_1, column3),
            cte3 AS (SELECT column_1, column3 FROM cte1 group by column_3) SELECT column_1 FROM cte3 limit 2;"""
        )
        assert ["column_2"] == extract_last_group_by_from_query(
            sql_text="""WITH cte1 AS (SELECT column_1 FROM test_table GROUP BY column_1),
            'cte2 AS (SELECT column_2 FROM test_table GROUP BY column_2),
            cte3 AS (SELECT column_1 FROM cte1 UNION ALL SELECT column2 FROM cte2) SELECT * FROM cte3"""
        )
        assert ["column_2"] == extract_last_group_by_from_query(
            sql_text="""WITH cte1 AS (SELECT column_1 FROM test_table GROUP BY column_1),
            cte2 AS (SELECT column_1 FROM test_table GROUP BY column_2) SELECT * FROM cte2;"""
        )

        assert ["COLUMN"] == extract_last_group_by_from_query(
            sql_text="""WITH cte1 AS (SELECT COLUMN FROM test_table GROUP BY COLUMN),
            cte2 AS (SELECT COLUMN FROM test_table GROUP BY COLUMN) SELECT * FROM cte2;"""
        )

        assert ["COLUMN"] == extract_last_group_by_from_query(
            sql_text="""WITH cte1 AS (SELECT COLUMN FROM test_table GROUP BY COLUMN),
            cte2 AS (SELECT COLUMN FROM test_table group by COLUMN) SELECT * FROM cte2;"""
        )
