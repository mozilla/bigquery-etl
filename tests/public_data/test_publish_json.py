import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, call

import pytest
import smart_open

from bigquery_etl.public_data.publish_json import JsonPublisher

TEST_DIR = Path(__file__).parent.parent


class TestPublishJson(object):
    test_bucket = "test-bucket"
    project_id = "test-project-id"

    non_incremental_sql_path = (
        TEST_DIR
        / "data"
        / "test_sql"
        / "moz-fx-data-test-project"
        / "test"
        / "non_incremental_query_v1"
        / "query.sql"
    )

    incremental_sql_path = (
        TEST_DIR
        / "data"
        / "test_sql"
        / "moz-fx-data-test-project"
        / "test"
        / "incremental_query_v1"
        / "query.sql"
    )

    incremental_parameter = "submission_date:DATE:2020-03-15"

    mock_blob = Mock()
    mock_blob.download_as_string.return_value = bytes("[]", "utf-8")
    mock_blob.name = "blob_path/000000000000.ndjson"

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

    temp_table = f"{project_id}.tmp.incremental_query_v1_20200315"
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
            str(self.non_incremental_sql_path),
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
            str(self.incremental_sql_path),
            self.api_version,
            self.test_bucket,
            ["submission_date:DATE:2020-11-02"],
        )

        assert publisher.date == "2020-11-02"

    def test_no_date_parameter_incremental(self):
        with pytest.raises(SystemExit) as e:
            publisher = JsonPublisher(
                self.mock_client,
                self.mock_storage_client,
                self.project_id,
                str(self.incremental_sql_path),
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
            str(self.incremental_sql_path),
            self.api_version,
            self.test_bucket,
            ["submission_date:DATE:2020-03-15"],
        )
        publisher._write_results_to_temp_table()

        assert publisher.temp_table.startswith(self.temp_table)
        self.mock_client.query.assert_called_once()

    def test_gcp_convert_ndjson_to_json(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
            self.project_id,
            str(self.incremental_sql_path),
            self.api_version,
            self.test_bucket,
            ["submission_date:DATE:2020-03-15"],
        )

        mock_out = MagicMock(side_effect=[['{"a": 1}', '{"b": "cc"}'], None])
        mock_out.__iter__ = Mock(return_value=iter(['{"a": 1}', '{"b": "cc"}']))
        file_handler = MagicMock()
        file_handler.__enter__.return_value = mock_out

        smart_open.open = MagicMock(return_value=file_handler)

        publisher._gcp_convert_ndjson_to_json("test_path")

        smart_open.open.assert_has_calls(
            [
                call("gs://test-bucket/blob_path/000000000000.ndjson"),
                call("gs://test-bucket/blob_path/000000000000.json.tmp.gz", "w"),
            ]
        )

        file_handler.write.assert_has_calls(
            [call("["), call('{"a": 1}'), call(","), call('{"b": "cc"}'), call("]")]
        )

    def test_publish_last_updated_to_gcs(self):
        publisher = JsonPublisher(
            self.mock_client,
            self.mock_storage_client,
            self.project_id,
            str(self.incremental_sql_path),
            self.api_version,
            self.test_bucket,
            ["submission_date:DATE:2020-03-15"],
        )

        mock_out = MagicMock(side_effect=[['{"a": 1}', '{"b": "cc"}'], None])
        mock_out.__iter__ = Mock(return_value=iter(['{"a": 1}', '{"b": "cc"}']))
        file_handler = MagicMock()
        file_handler.__enter__.return_value = mock_out

        smart_open.open = MagicMock(return_value=file_handler)

        publisher.publish_json()

        assert publisher.last_updated is not None
        mock_out.write.assert_called_with(
            json.dumps(publisher.last_updated.strftime("%Y-%m-%d %H:%M:%S"))
        )
