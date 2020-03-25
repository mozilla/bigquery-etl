import json
import os
import pytest
import subprocess
import zlib

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound


@pytest.mark.integration
class TestPublishJsonScript(object):
    test_bucket = "bigquery-etl-integration-test-bucket"
    project_id = os.environ["GOOGLE_PROJECT_ID"]

    non_incremental_sql_path = (
        "tests/publish_public_data_json/test_sql/test/"
        "non_incremental_query_v1/query.sql"
    )

    incremental_sql_path = (
        "tests/publish_public_data_json/test_sql/test/incremental_query_v1/query.sql"
    )
    incremental_parameter = "submission_date:DATE:2020-03-15"

    no_metadata_sql_path = (
        "tests/publish_public_data_json/test_sql/test/no_metadata_query_v1/query.sql"
    )

    client = bigquery.Client(project_id)
    storage_client = storage.Client()
    bucket = storage_client.bucket(test_bucket)

    temp_table = f"{project_id}.tmp.incremental_query_v1_20200315_temp"
    non_incremental_table = f"{project_id}.test.non_incremental_query_v1"

    @pytest.fixture(autouse=True)
    def setup(self):
        # remove tables that might be there from previous failed tests
        try:
            self.client.delete_table(self.temp_table)
        except NotFound:
            pass

        try:
            self.client.get_table(self.non_incremental_table)
        except NotFound:
            job_config = bigquery.QueryJobConfig(destination=self.non_incremental_table)

            # create table for non-incremental query
            with open(self.non_incremental_sql_path) as query_stream:
                query = query_stream.read()
                query_job = self.client.query(query, job_config=job_config)
                query_job.result()

        # remove json uploaded to storage by previous tests
        try:
            blob = self.bucket.blob("api/")
            blob.delete()
        except NotFound:
            pass

    @pytest.mark.dependency(name="test_script_incremental_query")
    def test_script_incremental_query(self):
        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--parameter=" + self.incremental_parameter,
                "--query_file=" + self.incremental_sql_path,
                "--target_bucket=" + self.test_bucket,
                "--project_id=" + self.project_id,
            )
        )

        assert res.returncode == 0

    def test_script_incremental_query_no_parameter(self):
        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--query_file=" + self.incremental_sql_path,
                "--target_bucket=" + self.test_bucket,
                "--project_id=" + self.project_id,
            )
        )

        assert res.returncode == 1

    def test_query_without_metadata(self):
        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--query_file=" + self.no_metadata_sql_path,
            )
        )

        assert res.returncode == 0

    @pytest.mark.dependency(name="test_script_non_incremental_query")
    def test_script_non_incremental_query(self):
        res = subprocess.run(
            (
                "./script/publish_public_data_json",
                "publish_json",
                "--query_file=" + self.non_incremental_sql_path,
                "--target_bucket=" + self.test_bucket,
                "--project_id=" + self.project_id,
            )
        )

        assert res.returncode == 0

    @pytest.mark.dependency(depends=["test_script_incremental_query"])
    def test_temporary_tables_removed(self):
        with pytest.raises(NotFound):
            self.client.get_table(self.temp_table)

    @pytest.mark.dependency(depends=["test_script_non_incremental_query"])
    def test_non_incremental_query_gcs(self):
        gcp_path = "api/v1/tables/test/non_incremental_query/v1/files/"
        blobs = self.storage_client.list_blobs(self.test_bucket, prefix=gcp_path)

        for blob in blobs:
            compressed = blob.download_as_string()
            uncompressed = zlib.decompress(compressed, 16 + zlib.MAX_WBITS)
            content = json.loads(uncompressed.decode("utf-8").strip())
            assert len(content) == 3

    @pytest.mark.dependency(depends=["test_script_non_incremental_query"])
    def test_incremental_query_gcs(self):
        gcp_path = "api/v1/tables/test/incremental_query/v1/files/2020-03-15/"
        blobs = self.storage_client.list_blobs(self.test_bucket, prefix=gcp_path)

        for blob in blobs:
            compressed = blob.download_as_string()
            uncompressed = zlib.decompress(compressed, 16 + zlib.MAX_WBITS)
            content = json.loads(uncompressed.decode("utf-8").strip())
            assert len(content) == 3
