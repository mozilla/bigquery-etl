import json
import pytest
import subprocess
from unittest.mock import patch, Mock, PropertyMock

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

    mock_blob = Mock()
    mock_blob.download_as_string.return_value = bytes("[]", 'utf-8')

    mock_bucket = Mock()
    mock_bucket.blob.return_value = mock_blob

    extract_table_mock = Mock()
    extract_table_mock.result.return_value = None

    mock_client = Mock()
    mock_client.get_table.return_value = None
    mock_client.extract_table.return_value = extract_table_mock

    mock_storage_client = Mock()
    mock_storage_client.list_blobs.return_value = [mock_blob]
    mock_storage_client.bucket.return_value = mock_bucket

    temp_table = f"{project_id}.tmp.incremental_query_v1_20200315_temp"
    non_incremental_table = f"{project_id}.test.non_incremental_query_v1"
    api_version = "v1"

    @pytest.fixture(autouse=True)
    def setup(self):
        pass

    def test_dataset_table_version_splitting(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
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
                self.mock_client,
                self.mock_storage_client,
                self.project_id,
                "invalid_path",
                self.api_version,
                self.test_bucket,
            )

        assert e.type == FileNotFoundError

    def test_parameter_date(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
            self.project_id,
            self.incremental_sql_path,
            self.api_version,
            self.test_bucket,
            "submission_date:DATE:2020-11-02",
        )

        assert publisher.date == "2020-11-02"

    def test_no_date_parameter_incremental(self):
        with pytest.raises(SystemExit) as e:
            publisher = JsonPublisher(
                self.mock_client,
                self.mock_storage_client,
                self.project_id,
                self.incremental_sql_path,
                self.api_version,
                self.test_bucket,
                None,
            )
            publisher.publish_json()

        assert e.type == SystemExit
        assert e.value.code == 1

    def test_write_results_to_temp_table(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
            self.project_id,
            self.incremental_sql_path,
            self.api_version,
            self.test_bucket,
            "submission_date:DATE:2020-03-15",
        )
        publisher._write_results_to_temp_table()

        assert publisher.temp_table == self.temp_table
        self.mock_client.query.assert_called_once()

    def test_convert_ndjson_to_json(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
            self.project_id,
            self.incremental_sql_path,
            self.api_version,
            self.test_bucket,
            "submission_date:DATE:2020-03-15",
        )

        prefix = f"api/{self.api_version}/tables/test/incremental_query/v1/files/2020-03-15/"
        publisher._gcp_convert_ndjson_to_json(prefix)
        self.mock_storage_client.list_blobs.assert_called_with(self.test_bucket, prefix=prefix)
        self.mock_blob.download_as_string.assert_called_once()
        # todo
        self.mock_blob.upload_from_string.assert_called_once()

    def test_publish_table_as_json(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
            self.project_id,
            self.incremental_sql_path,
            self.api_version,
            self.test_bucket,
            "submission_date:DATE:2020-03-15",
        )

        publisher._publish_table_as_json("result_table")

        self.mock_client.get_table.assert_called_with("result_table")
        self.mock_client.extract_table.assert_called_once()
        self.extract_table_mock.result.assert_called_once()
