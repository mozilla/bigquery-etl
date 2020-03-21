import pytest
from unittest.mock import Mock

from bigquery_etl.publish_json import JsonPublisher


class TestPublishJson(object):
    test_bucket = "test-bucket"
    project_id = "test-project-id"

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
    mock_blob.download_as_string.return_value = bytes("[]", "utf-8")
    mock_blob.name = "blob_path"

    mock_bucket = Mock()
    mock_bucket.blob.return_value = mock_blob
    mock_bucket.rename_blob.return_value = None

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
