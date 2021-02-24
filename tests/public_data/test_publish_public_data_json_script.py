import json
import subprocess
from datetime import datetime
from pathlib import Path

import pytest
from google.cloud import bigquery

TEST_DIR = Path(__file__).parent.parent


@pytest.mark.integration
class TestPublishJsonScript(object):
    def test_script_incremental_query(
        self,
        storage_client,
        test_bucket,
        temporary_gcs_folder,
        project_id,
        temporary_dataset,
        bigquery_client,
    ):
        incremental_sql_path = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--parameter=a:INT64:9",
                "--parameter=submission_date:DATE:2020-03-15",
                "--query_file=" + str(incremental_sql_path),
                "--target_bucket=" + test_bucket.name,
                "--gcs_path=" + temporary_gcs_folder,
                "--public_project_id=" + project_id,
            )
        )

        assert res.returncode == 0

        gcp_path = (
            f"{temporary_gcs_folder}api/v1/tables/"
            + "test/incremental_query/v1/files/2020-03-15/"
        )
        blobs = storage_client.list_blobs(test_bucket, prefix=gcp_path)

        blob_count = 0

        for blob in blobs:
            blob_count += 1
            content = json.loads(blob.download_as_string().decode("utf-8"))
            assert blob.content_type == "application/json"
            assert blob.content_encoding == "gzip"
            assert len(content) == 3

        assert blob_count == 1

        gcp_path = (
            f"{temporary_gcs_folder}api/v1/tables/test/"
            + "incremental_query/v1/last_updated"
        )
        blobs = storage_client.list_blobs(test_bucket, prefix=gcp_path)

        blob_count = 0

        for blob in blobs:
            blob_count += 1
            last_updated = json.loads(blob.download_as_string().decode("utf-8"))
            datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")

        assert blob_count == 1

    def test_script_incremental_query_no_parameter(
        self,
        test_bucket,
        temporary_gcs_folder,
        project_id,
        temporary_dataset,
        bigquery_client,
    ):
        incremental_sql_path = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--query_file=" + str(incremental_sql_path),
                "--target_bucket=" + test_bucket.name,
                "--gcs_path=" + temporary_gcs_folder,
                "--public_project_id=" + project_id,
            )
        )

        assert res.returncode == 1

    def test_query_without_metadata(self):
        no_metadata_sql_path = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "no_metadata_query_v1"
            / "query.sql"
        )

        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--query_file=" + str(no_metadata_sql_path),
            )
        )

        assert res.returncode == 0

    def test_script_non_incremental_query(
        self,
        bigquery_client,
        storage_client,
        test_bucket,
        temporary_gcs_folder,
        project_id,
        temporary_dataset,
    ):
        non_incremental_sql_path = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "non_incremental_query_v1"
            / "query.sql"
        )

        non_incremental_table = (
            f"{project_id}.{temporary_dataset}.non_incremental_query_v1"
        )
        date_partition = bigquery.table.TimePartitioning(field="d")
        job_config = bigquery.QueryJobConfig(
            destination=non_incremental_table, time_partitioning=date_partition
        )

        with open(non_incremental_sql_path) as query_stream:
            query = query_stream.read()
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()

        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--query_file=" + str(non_incremental_sql_path),
                "--target_bucket=" + test_bucket.name,
                "--gcs_path=" + temporary_gcs_folder,
                "--public_project_id=" + project_id,
            )
        )

        assert res.returncode == 0

        gcp_path = (
            f"{temporary_gcs_folder}api/v1/tables/test/"
            + "non_incremental_query/v1/files/"
        )
        blobs = storage_client.list_blobs(test_bucket, prefix=gcp_path)

        blob_count = 0

        for blob in blobs:
            blob_count += 1
            content = json.loads(blob.download_as_string().decode("utf-8"))
            assert blob.content_type == "application/json"
            assert blob.content_encoding == "gzip"
            assert len(content) == 3

        assert blob_count == 1

    def test_script_non_incremental_export(
        self,
        storage_client,
        test_bucket,
        project_id,
        bigquery_client,
        temporary_gcs_folder,
        temporary_dataset,
    ):
        incremental_non_incremental_export_sql_path = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_non_incremental_export_v1"
            / "query.sql"
        )

        incremental_non_incremental_export_table = (
            f"{project_id}.{temporary_dataset}."
            "incremental_query_non_incremental_export_v1"
        )

        date_partition = bigquery.table.TimePartitioning(field="d")
        job_config = bigquery.QueryJobConfig(
            destination=incremental_non_incremental_export_table,
            time_partitioning=date_partition,
        )

        # create table for non-incremental query
        with open(incremental_non_incremental_export_sql_path) as query_stream:
            query = query_stream.read()
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()

        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--parameter=a:INT64:9",
                "--query_file=" + str(incremental_non_incremental_export_sql_path),
                "--target_bucket=" + test_bucket.name,
                "--gcs_path=" + temporary_gcs_folder,
                "--public_project_id=" + project_id,
                "--parameter=submission_date:DATE:2020-03-15",
            )
        )
        assert res.returncode == 0

        gcp_path = (
            f"{temporary_gcs_folder}api/v1/tables/test/"
            + "incremental_query_non_incremental_export/v1/files/"
        )

        blobs = storage_client.list_blobs(test_bucket, prefix=gcp_path)

        blob_count = 0

        for blob in blobs:
            blob_count += 1
            content = json.loads(blob.download_as_string().decode("utf-8"))
            assert blob.content_type == "application/json"
            assert blob.content_encoding == "gzip"
            assert len(content) == 3

        assert blob_count == 1
