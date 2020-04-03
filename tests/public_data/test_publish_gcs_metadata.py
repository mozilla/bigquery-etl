import json
import pytest
import smart_open

from unittest.mock import call, Mock, MagicMock

import bigquery_etl.public_data.publish_gcs_metadata as pgm


class TestPublishGcsMetadata(object):
    test_bucket = "test-bucket"
    project_id = "test-project-id"
    api_version = "v1"
    endpoint = "http://test.endpoint.mozilla.com/"
    target_dir = "tests/public_data/test_sql/"

    mock_blob1 = Mock()
    mock_blob1.name = (
        "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
    )

    mock_blob2 = Mock()
    mock_blob2.name = "api/v1/tables/test/non_incremental_query/v1/files"

    mock_storage_client = Mock()
    mock_storage_client.list_blobs.return_value = [mock_blob1, mock_blob2]

    @pytest.fixture(autouse=True)
    def setup(self):
        pass

    def test_dataset_table_version_from_gcs_path(self):
        result = pgm.dataset_table_version_from_gcs_path(
            "api/v1/tables/telemetry_derived/ssl_ratios/v1/files/2020-04-01/000000001228.json.gz"
        )

        assert result[0] == "telemetry_derived"
        assert result[1] == "ssl_ratios"
        assert result[2] == "v1"

        result = pgm.dataset_table_version_from_gcs_path(
            "api/v1/tables/telemetry_derived/ssl_ratios/v1/files/000000001228.json.gz"
        )

        assert result[0] == "telemetry_derived"
        assert result[1] == "ssl_ratios"
        assert result[2] == "v1"

        result = pgm.dataset_table_version_from_gcs_path(
            "api/v1/tables/telemetry_derived/ssl_ratios/v1/files/"
        )

        assert result == None

    def test_gcs_table_metadata_no_files(self):
        with pytest.raises(Exception):
            pgm.GcsTableMetadata([], self.endpoint, self.target_dir)

    def test_gcs_table_metadata(self):
        files = [
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
        ]
        files_path = "api/v1/tables/test/non_incremental_query/v1/files"
        gcs_table_metadata = pgm.GcsTableMetadata(files, self.endpoint, self.target_dir)

        assert gcs_table_metadata.files == files
        assert gcs_table_metadata.endpoint == self.endpoint
        assert gcs_table_metadata.files_path == files_path
        assert gcs_table_metadata.files_uri == self.endpoint + files_path
        assert gcs_table_metadata.dataset == "test"
        assert gcs_table_metadata.table == "non_incremental_query"
        assert gcs_table_metadata.version == "v1"
        assert gcs_table_metadata.metadata.is_incremental() == False
        assert gcs_table_metadata.metadata.is_incremental_export() == False
        assert gcs_table_metadata.metadata.review_bug() == "1999999"

    def test_gcs_table_metadata_to_json(self):
        files = [
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
        ]
        files_path = "api/v1/tables/test/non_incremental_query/v1/files"
        gcs_table_metadata = pgm.GcsTableMetadata(files, self.endpoint, self.target_dir)

        result = gcs_table_metadata.table_metadata_to_json()

        assert len(result.items()) == 6
        assert result["description"] == "Test table for a non-incremental query\n"
        assert result["friendly_name"] == "Test table for a non-incremental query\n"
        assert result["incremental"] == False
        assert result["incremental_export"] == False
        assert (
            result["review_link"]
            == "https://bugzilla.mozilla.org/show_bug.cgi?id=1999999"
        )
        assert result["files_uri"] == self.endpoint + files_path

    def test_gcs_files_metadata_to_json(self):
        files = [
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
        ]
        json_expected = [
            self.endpoint
            + "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
        ]

        gcs_table_metadata = pgm.GcsTableMetadata(files, self.endpoint, self.target_dir)

        result = gcs_table_metadata.files_metadata_to_json()

        assert result == json_expected

    def test_gcs_files_metadata_to_json_incremental(self):
        files = [
            "api/v1/tables/test/incremental_query/v1/files/2020-03-15/000000000000.json.gz",
            "api/v1/tables/test/incremental_query/v1/files/2020-03-15/000000000001.json.gz",
            "api/v1/tables/test/incremental_query/v1/files/2020-03-16/000000000000.json.gz",
        ]
        json_expected = {
            "2020-03-15": [
                self.endpoint
                + "api/v1/tables/test/incremental_query/v1/files/2020-03-15/000000000000.json.gz",
                self.endpoint
                + "api/v1/tables/test/incremental_query/v1/files/2020-03-15/000000000001.json.gz",
            ],
            "2020-03-16": [
                self.endpoint
                + "api/v1/tables/test/incremental_query/v1/files/2020-03-16/000000000000.json.gz"
            ],
        }

        gcs_table_metadata = pgm.GcsTableMetadata(files, self.endpoint, self.target_dir)

        result = gcs_table_metadata.files_metadata_to_json()

        assert result == json_expected

    def test_get_public_gcs_table_metadata(self):
        result = list(
            pgm.get_public_gcs_table_metadata(
                self.mock_storage_client,
                self.test_bucket,
                self.api_version,
                self.endpoint,
                self.target_dir,
            )
        )

        assert len(result) == 1
        assert (
            result[0].files_uri
            == self.endpoint + "api/v1/tables/test/non_incremental_query/v1/files"
        )

    def test_publish_all_datasets_metadata(self):
        files1 = [
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
        ]
        files2 = [
            "api/v1/tables/test/incremental_query/v1/files/2020-03-15/000000000001.json.gz"
        ]
        gcs_table_metadata = [
            pgm.GcsTableMetadata(files1, self.endpoint, self.target_dir),
            pgm.GcsTableMetadata(files2, self.endpoint, self.target_dir),
        ]

        mock_out = MagicMock()
        file_handler = MagicMock()
        file_handler.__enter__.return_value = mock_out
        smart_open.open = MagicMock(return_value=file_handler)

        pgm.publish_all_datasets_metadata(gcs_table_metadata, "output.txt")

        with open("tests/public_data/all_datasets.json") as f:
            expected_json = json.load(f)
            mock_out.write.assert_called_with(json.dumps(expected_json))

    def test_publish_table_metadata(self):
        files1 = [
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json.gz"
        ]
        files2 = [
            "api/v1/tables/test/incremental_query/v1/files/2020-03-15/000000000001.json.gz"
        ]
        gcs_table_metadata = [
            pgm.GcsTableMetadata(files1, self.endpoint, self.target_dir),
            pgm.GcsTableMetadata(files2, self.endpoint, self.target_dir),
        ]

        mock_out = MagicMock()
        file_handler = MagicMock()
        file_handler.__enter__.return_value = mock_out
        smart_open.open = MagicMock(return_value=file_handler)

        pgm.publish_table_metadata(gcs_table_metadata, self.test_bucket)

        with open("tests/public_data/incremental_query_gcs_metadata.json") as f:
            expected_incremental_query_json = json.load(f)

        with open("tests/public_data/non_incremental_query_gcs_metadata.json") as f:
            expected_non_incremental_query_json = json.load(f)

        mock_out.write.assert_has_calls(
            [
                call(json.dumps(expected_non_incremental_query_json)),
                call(json.dumps(expected_incremental_query_json)),
            ]
        )
