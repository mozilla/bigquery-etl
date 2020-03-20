import json
import pytest
import subprocess

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

from bigquery_etl.publish_json import JsonPublisher


class TestPublishJson(object):
    test_bucket = "moz-fx-data-stage-bigquery-etl"
    project_id = "moz-fx-data-shar-nonprod-efed"

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
    api_version = "v1"

    @pytest.fixture(autouse=True)
    def setup(self):
        pass

    def test_dataset_table_version_splitting(self):
        publisher = JsonPublisher(
            self.project_id,
            self.non_incremental_sql_path,
            self.api_version,
            self.test_bucket,
        )
        assert publisher.dataset == "test"
        assert publisher.table == "non_incremental_query"
        assert publisher.version == "v1"

    def test_invalid_query_file(self):
        with pytest.raises(FileNotFoundError) as e:
            JsonPublisher(
                self.project_id,
                "invalid_path",
                self.api_version,
                self.test_bucket,
            )

        assert e.type == FileNotFoundError

    def test_parameter_date(self):
        publisher = JsonPublisher(
            self.project_id,
            self.incremental_sql_path,
            self.api_version,
            self.test_bucket,
            "submission_date:DATE:2020-11-02"
        )

        assert publisher.date == "2020-11-02"
