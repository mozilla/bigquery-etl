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

        try:
            result = subprocess.check_output(
                [
                    ENTRYPOINT_SCRIPT,
                    "query",
                    "--project_id=" + project_id,
                    str(query_file),
                ],
                stderr=subprocess.STDOUT,
            )
            assert b"Current status: DONE" in result
            assert b"No metadata.yaml found for {}" in result
        except subprocess.CalledProcessError as e:
            # running bq in CircleCI will fail since it's not installed
            # but the error output can be checked for whether bq was called
            assert b"No such file or directory: 'bq'" in e.output
            assert b"No metadata.yaml found for {}" in e.output
            assert (
                b'subprocess.check_call(["bq"] + query_arguments, stdin=query_stream)'
                in e.output
            )

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

        try:
            result = subprocess.check_output(
                [
                    ENTRYPOINT_SCRIPT,
                    "query",
                    "--dataset_id=" + temporary_dataset,
                    "--destination_table=query_v1",
                    "--project_id=" + project_id,
                    "--replace",
                    str(query_file),
                ],
                stderr=subprocess.STDOUT,
            )
            assert b"Current status: DONE" in result
            assert b"No metadata.yaml found for {}" in result

            result = bigquery_client.query(
                f"SELECT a FROM {project_id}.{temporary_dataset}.query_v1"
            ).result()
            assert result.total_rows == 1
            for row in result:
                assert row.a == "foo"
        except subprocess.CalledProcessError as e:
            assert b"No such file or directory: 'bq'" in e.output
            assert b"No metadata.yaml found for {}" in e.output
            assert (
                b'subprocess.check_call(["bq"] + query_arguments, stdin=query_stream)'
                in e.output
            )

    @pytest.mark.integration
    def test_run_query_no_query_file(self):
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.check_call([ENTRYPOINT_SCRIPT, "query"])
