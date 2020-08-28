from pathlib import Path
from google.cloud import bigquery
import os
import pytest
import subprocess

ENTRYPOINT_SCRIPT = Path(__file__).parent.parent / "script" / "entrypoint"


class TestEntrypoint:
    @pytest.mark.integration
    def test_run_query(self, tmp_path, project_id):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 1 AS a, 'abc' AS b;")

        result = subprocess.check_call(
            [ENTRYPOINT_SCRIPT, "query", "--project_id=" + project_id, str(query_file)]
        )
        assert result == 0

    @pytest.mark.integration
    def test_run_query_write_to_table(
        self, tmp_path, bigquery_client, project_id, temporary_dataset
    ):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 'foo' AS a")

        schema = [bigquery.SchemaField("a", "STRING", mode="NULLABLE")]
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.query_v1", schema=schema
        )
        bigquery_client.create_table(table)

        result = subprocess.check_call(
            [
                ENTRYPOINT_SCRIPT,
                "query",
                "--dataset_id=" + temporary_dataset,
                "--destination_table=query_v1",
                "--project_id=" + project_id,
                "--replace",
                str(query_file),
            ]
        )
        assert result == 0

        result = bigquery_client.query(
            f"SELECT a FROM {project_id}.{temporary_dataset}.query_v1"
        ).result()
        assert result.total_rows == 1
        for row in result:
            assert row.a == "foo"

    @pytest.mark.integration
    def test_run_query_no_query_file(self):
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.check_call([ENTRYPOINT_SCRIPT, "query"])
