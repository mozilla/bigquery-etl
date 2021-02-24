import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, call

import pytest
import smart_open

import bigquery_etl.public_data.publish_gcs_metadata as pgm

TEST_DIR = Path(__file__).parent.parent


class TestPublishGcsMetadata(object):
    test_bucket = "test-bucket"
    project_id = "test-project-id"
    api_version = "v1"
    endpoint = "https://test.endpoint.mozilla.com/"
    sql_dir = TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project"

    mock_blob1 = Mock()
    mock_blob1.name = (
        "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
    )
    mock_blob1.updated = datetime(2020, 4, 3, 11, 30, 1)

    mock_blob2 = Mock()
    mock_blob2.name = "api/v1/tables/test/non_incremental_query/v1/files"
    mock_blob2.updated = datetime(2020, 4, 3, 11, 25, 5)

    mock_storage_client = Mock()
    mock_storage_client.list_blobs.return_value = [mock_blob1, mock_blob2]

    @pytest.fixture(autouse=True)
    def setup(self):
        pass

    def test_dataset_table_version_from_gcs_blob(self):
        mock_blob = Mock()
        mock_blob.name = (
            "api/v1/tables/telemetry_derived/ssl_ratios/v1/files/"
            "2020-04-01/000000001228.json"
        )

        result = pgm.dataset_table_version_from_gcs_blob(mock_blob)

        assert result[0] == "telemetry_derived"
        assert result[1] == "ssl_ratios"
        assert result[2] == "v1"

        mock_blob.name = (
            "api/v1/tables/telemetry_derived/ssl_ratios/v1/files/000000001228.json"
        )

        result = pgm.dataset_table_version_from_gcs_blob(mock_blob)

        assert result[0] == "telemetry_derived"
        assert result[1] == "ssl_ratios"
        assert result[2] == "v1"

        mock_blob.name = "api/v1/tables/telemetry_derived/ssl_ratios/v1/files/"

        result = pgm.dataset_table_version_from_gcs_blob(mock_blob)

        assert result is None

    def test_gcs_table_metadata_no_files(self):
        with pytest.raises(Exception):
            pgm.GcsTableMetadata([], self.endpoint, self.target_dir)

    def test_gcs_table_metadata(self):
        mock_blob = Mock()
        mock_blob.name = (
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
        )
        mock_blob.updated = datetime(2020, 4, 3, 11, 30, 1)

        files_path = "api/v1/tables/test/non_incremental_query/v1/files"
        last_updated_path = "api/v1/tables/test/non_incremental_query/v1/last_updated"
        gcs_table_metadata = pgm.GcsTableMetadata(
            [mock_blob], self.endpoint, self.sql_dir
        )

        assert gcs_table_metadata.blobs == [mock_blob]
        assert gcs_table_metadata.endpoint == self.endpoint
        assert gcs_table_metadata.files_path == files_path
        assert gcs_table_metadata.files_uri == self.endpoint + files_path
        assert gcs_table_metadata.dataset == "test"
        assert gcs_table_metadata.table == "non_incremental_query"
        assert gcs_table_metadata.version == "v1"
        assert gcs_table_metadata.metadata.is_incremental() is False
        assert gcs_table_metadata.metadata.is_incremental_export() is False
        assert gcs_table_metadata.metadata.review_bugs() == ["1999999", "12121212"]
        assert gcs_table_metadata.last_updated_path == last_updated_path
        assert gcs_table_metadata.last_updated_uri == self.endpoint + last_updated_path

    def test_gcs_table_metadata_to_json(self):
        mock_blob = Mock()
        mock_blob.name = (
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
        )
        mock_blob.updated = datetime(2020, 4, 3, 11, 25, 5)
        files_path = "api/v1/tables/test/non_incremental_query/v1/files"
        last_updated_path = "api/v1/tables/test/non_incremental_query/v1/last_updated"
        gcs_table_metadata = pgm.GcsTableMetadata(
            [mock_blob], self.endpoint, self.sql_dir
        )

        result = gcs_table_metadata.table_metadata_to_json()

        assert len(result.items()) == 7
        assert result["description"] == "Test table for a non-incremental query"
        assert result["friendly_name"] == "Test table for a non-incremental query"
        assert result["incremental"] is False
        assert result["incremental_export"] is False
        review_link = [
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1999999",
            "https://bugzilla.mozilla.org/show_bug.cgi?id=12121212",
        ]
        assert result["review_links"] == review_link
        assert result["files_uri"] == self.endpoint + files_path
        assert result["last_updated"] == self.endpoint + last_updated_path

    def test_gcs_files_metadata_to_json(self):
        mock_blob = Mock()
        mock_blob.name = (
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
        )
        mock_blob.updated = datetime(2020, 4, 3, 11, 25, 5)

        json_expected = [
            (
                f"{self.endpoint}"
                "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
            )
        ]

        gcs_table_metadata = pgm.GcsTableMetadata(
            [mock_blob], self.endpoint, self.sql_dir
        )

        result = gcs_table_metadata.files_metadata_to_json()

        assert result == json_expected

    def test_gcs_files_metadata_to_json_incremental(self):
        files = [
            (
                "api/v1/tables/test/incremental_query/v1/files/2020-03-15/"
                "000000000000.json"
            ),
            (
                "api/v1/tables/test/incremental_query/v1/files/2020-03-15/"
                "000000000001.json"
            ),
            (
                "api/v1/tables/test/incremental_query/v1/files/2020-03-16/"
                "000000000000.json"
            ),
        ]

        blobs = []
        for file in files:
            mock_blob = Mock()
            mock_blob.name = file
            mock_blob.updated = datetime(2020, 4, 3, 11, 25, 5)
            blobs.append(mock_blob)

        json_expected = {
            "2020-03-15": [
                (
                    f"{self.endpoint}"
                    "api/v1/tables/test/incremental_query/v1/files/"
                    "2020-03-15/000000000000.json"
                ),
                (
                    f"{self.endpoint}"
                    "api/v1/tables/test/incremental_query/v1/files/"
                    "2020-03-15/000000000001.json"
                ),
            ],
            "2020-03-16": [
                (
                    f"{self.endpoint}"
                    "api/v1/tables/test/incremental_query/v1/files/"
                    "2020-03-16/000000000000.json"
                )
            ],
        }

        gcs_table_metadata = pgm.GcsTableMetadata(blobs, self.endpoint, self.sql_dir)

        result = gcs_table_metadata.files_metadata_to_json()

        assert result == json_expected

    def test_get_public_gcs_table_metadata(self):
        result = list(
            pgm.get_public_gcs_table_metadata(
                self.mock_storage_client,
                self.test_bucket,
                self.api_version,
                self.endpoint,
                self.sql_dir,
            )
        )

        expected = self.endpoint + "api/v1/tables/test/non_incremental_query/v1/files"
        assert len(result) == 1
        assert result[0].files_uri == expected

    def test_publish_all_datasets_metadata(self):
        mock_blob1 = Mock()
        mock_blob1.name = (
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
        )
        mock_blob1.updated = datetime(2020, 4, 3, 11, 25, 5)

        mock_blob2 = Mock()
        mock_blob2.name = (
            "api/v1/tables/test/incremental_query/v1/files/2020-03-15/"
            "000000000001.json"
        )
        mock_blob2.updated = datetime(2020, 4, 3, 11, 25, 5)

        gcs_table_metadata = [
            pgm.GcsTableMetadata([mock_blob1], self.endpoint, self.sql_dir),
            pgm.GcsTableMetadata([mock_blob2], self.endpoint, self.sql_dir),
        ]

        mock_out = MagicMock()
        file_handler = MagicMock()
        file_handler.__enter__.return_value = mock_out
        smart_open.open = MagicMock(return_value=file_handler)

        pgm.publish_all_datasets_metadata(gcs_table_metadata, "output.txt")

        all_datasets = TEST_DIR / "data" / "all_datasets.json"
        with open(all_datasets) as f:
            expected_json = json.load(f)
            mock_out.write.assert_called_with(json.dumps(expected_json, indent=4))

    def test_publish_table_metadata(self):
        mock_blob1 = Mock()
        mock_blob1.name = (
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
        )
        mock_blob1.updated = datetime(2020, 4, 3, 11, 25, 5)

        mock_blob2 = Mock()
        mock_blob2.name = (
            "api/v1/tables/test/incremental_query/v1/files/2020-03-15/"
            "000000000001.json"
        )
        mock_blob2.updated = datetime(2020, 4, 3, 11, 25, 5)

        gcs_table_metadata = [
            pgm.GcsTableMetadata([mock_blob1], self.endpoint, self.sql_dir),
            pgm.GcsTableMetadata([mock_blob2], self.endpoint, self.sql_dir),
        ]

        mock_out = MagicMock()
        file_handler = MagicMock()
        file_handler.__enter__.return_value = mock_out
        smart_open.open = MagicMock(return_value=file_handler)

        pgm.publish_table_metadata(
            self.mock_storage_client, gcs_table_metadata, self.test_bucket
        )

        metadata_file = TEST_DIR / "data" / "incremental_query_gcs_metadata.json"
        with open(metadata_file) as f:
            expected_incremental_query_json = json.load(f)

        metadata_file = TEST_DIR / "data" / "non_incremental_query_gcs_metadata.json"
        with open(metadata_file) as f:
            expected_non_incremental_query_json = json.load(f)

        mock_out.write.assert_has_calls(
            [
                call(json.dumps(expected_non_incremental_query_json, indent=4)),
                call(json.dumps(expected_incremental_query_json, indent=4)),
            ]
        )

    def test_get_public_gcs_table_metadata_different_projects(self):
        mock_blob1 = Mock()
        mock_blob1.name = (
            "api/v1/tables/test/non_incremental_query/v1/files/000000000000.json"
        )
        mock_blob1.updated = datetime(2020, 4, 3, 11, 30, 1)

        mock_blob2 = Mock()
        mock_blob2.name = "api/v1/tables/test/not_existing/v1/files/000000000000.json"
        mock_blob2.updated = datetime(2020, 4, 3, 11, 25, 5)

        mock_storage_client = Mock()
        mock_storage_client.list_blobs.return_value = [mock_blob1, mock_blob2]

        result = list(
            pgm.get_public_gcs_table_metadata(
                mock_storage_client,
                self.test_bucket,
                self.api_version,
                self.endpoint,
                self.sql_dir,
            )
        )

        expected = self.endpoint + "api/v1/tables/test/non_incremental_query/v1/files"
        assert len(result) == 1
        assert result[0].files_uri == expected
