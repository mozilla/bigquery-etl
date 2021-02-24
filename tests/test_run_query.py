import os
from unittest.mock import patch

import pytest
import yaml

from bigquery_etl.run_query import run


class TestRunQuery:
    def test_run_query(self, tmp_path):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 1")

        metadata_conf = {
            "friendly_name": "test",
            "description": "test",
            "owners": ["test@example.org"],
        }

        metadata_file = query_file_path / "metadata.yaml"
        metadata_file.write_text(yaml.dump(metadata_conf))

        with patch("subprocess.check_call") as mock_call:
            mock_call.return_value = True
            run(query_file, "test", "query_v1", [])

            assert mock_call.call_args.args == (
                ["bq", "--dataset_id=test", "--destination_table=query_v1"],
            )
            assert "stdin" in mock_call.call_args.kwargs

    def test_run_query_public_project(self, tmp_path):
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

        with patch("subprocess.check_call") as mock_call:
            mock_call.return_value = True
            run(query_file, "test", "query_v1", [])

            assert mock_call.call_args.args == (
                [
                    "bq",
                    "--dataset_id=test",
                    "--destination_table=mozilla-public-data:test.query_v1",
                ],
            )
            assert "stdin" in mock_call.call_args.kwargs

    def test_run_query_public_project_no_dataset(self, tmp_path):
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

        with patch("subprocess.check_call") as mock_call:
            mock_call.return_value = True
            with pytest.raises(SystemExit):
                run(query_file, None, "query_v1", [])

            with pytest.raises(SystemExit):
                run(query_file, "test", None, [])
