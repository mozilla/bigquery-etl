import os
from unittest.mock import patch

import pytest
import yaml

from bigquery_etl.run_query import run


class TestRunQuery:
    def test_raises_if_cant_qualify_destination_table(self, tmp_path):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 1")

        with patch("google.cloud.bigquery.Client"):

            with pytest.raises(ValueError):
                run(query_file, dataset_id="test", destination_table=None)

            with pytest.raises(ValueError):
                run(query_file, dataset_id=None, destination_table="query_v1")

    def test_public_query_uses_public_project(self, tmp_path):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 1")

        metadata_conf = {
            "friendly_name": "test",
            "description": "test",
            "owners": ["test@example.org"],
            "labels": {"public_bigquery": True, "review_bugs": [222222]},
        }

        metadata_file = query_file_path / "metadata.yaml"
        metadata_file.write_text(yaml.dump(metadata_conf))

        with patch("google.cloud.bigquery.Client") as mock_client:

            run(
                query_file,
                project_id="moz-fx-data-shared-prod",
                public_project_id="moz-public",
                dataset_id="test",
                destination_table="query_v1",
            )
            assert mock_client.call_args.args == ("moz-public",)

        metadata_conf = {
            "friendly_name": "test",
            "description": "test",
            "owners": ["test@example.org"],
            "labels": {"public_bigquery": True},  # No review bugs (invalid)
        }

        metadata_file = query_file_path / "metadata.yaml"
        metadata_file.write_text(yaml.dump(metadata_conf))

        with pytest.raises(SystemExit):
            run(
                query_file,
                project_id="moz-fx-data-shared-prod",
                public_project_id="moz-public",
                dataset_id="test",
                destination_table="query_v1",
            )
