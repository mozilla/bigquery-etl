from pathlib import Path
from unittest.mock import patch

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.publish_metadata import publish_metadata

TEST_DIR = Path(__file__).parent.parent


class TestPublishMetadata(object):
    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_publish_metadata(self, mock_bigquery_table, mock_bigquery_client):
        mock_bigquery_client().get_table.return_value = mock_bigquery_table()

        metadata_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "metadata.yaml"
        )
        metadata = Metadata.from_file(metadata_file)

        project = "test_project"
        dataset = "test_dataset"
        table = "test_table"
        publish_metadata(mock_bigquery_client(), project, dataset, table, metadata)

        assert mock_bigquery_client().update_table.call_count == 1
        assert (
            mock_bigquery_client().update_table.call_args[0][0].friendly_name
            == "Test table for an incremental query"
        )
        assert (
            mock_bigquery_client().update_table.call_args[0][0].description
            == "Test table for an incremental query"
        )
        assert mock_bigquery_client().update_table.call_args[0][0].labels == {
            "dag": "bqetl_events",
            "deprecated": "true",
            "incremental": "",
            "incremental_export": "",
            "monitoring": "true",
            "owner1": "test",
            "public_json": "",
            "schedule": "daily",
        }
        assert (
            mock_bigquery_client()
            .get_table("moz-fx-data-test-project.test.incremental_query_v1")
            .friendly_name
            == "Test table for an incremental query"
        )
        assert (
            mock_bigquery_table().friendly_name == "Test table for an incremental query"
        )
        assert (
            mock_bigquery_table().description == "Test table for an incremental query"
        )
        assert mock_bigquery_table().labels == {
            "dag": "bqetl_events",
            "deprecated": "true",
            "incremental": "",
            "incremental_export": "",
            "monitoring": "true",
            "owner1": "test",
            "public_json": "",
            "schedule": "daily",
        }
