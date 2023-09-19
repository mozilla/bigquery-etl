import os
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from bigquery_etl.cli.query import run


class TestRunQuery:
    def test_run_query(self, tmp_path):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"
        query_file.write_text("-- comment \n SELECT 1")

        metadata_conf = {
            "friendly_name": "test",
            "description": "test",
            "owners": ["test@example.org"],
        }

        metadata_file = query_file_path / "metadata.yaml"
        metadata_file.write_text(yaml.dump(metadata_conf))

        runner = CliRunner()
        with patch("subprocess.check_call") as mock_call:
            mock_call.return_value = True
            result = runner.invoke(
                run,
                [str(query_file), "--dataset_id=test", "--destination_table=query_v1"],
            )

            assert result.exit_code == 0

            assert mock_call.call_args.args == (
                [
                    "bq",
                    "query",
                    "--dataset_id=test",
                    "--destination_table=query_v1",
                ],
            )
            assert "stdin" in mock_call.call_args.kwargs

    def test_run_query_qualified_destination_table(self, tmp_path):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"
        query_file.write_text("-- comment \n SELECT 1")

        metadata_conf = {
            "friendly_name": "test",
            "description": "test",
            "owners": ["test@example.org"],
        }

        metadata_file = query_file_path / "metadata.yaml"
        metadata_file.write_text(yaml.dump(metadata_conf))

        runner = CliRunner()
        with patch("subprocess.check_call") as mock_call:
            mock_call.return_value = True
            result = runner.invoke(
                run,
                [
                    str(query_file),
                    "--dataset_id=test",
                    "--destination_table=mozdata:test.query_v1",
                ],
            )

            assert result.exit_code == 0

            assert mock_call.call_args.args == (
                [
                    "bq",
                    "query",
                    "--dataset_id=test",
                    "--destination_table=mozdata:test.query_v1",
                ],
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
            runner = CliRunner()
            result = runner.invoke(
                run,
                [str(query_file), "--dataset_id=test", "--destination_table=query_v1"],
            )

            assert result.exit_code == 0

            assert mock_call.call_args.args == (
                [
                    "bq",
                    "query",
                    "--dataset_id=test",
                    "--destination_table=mozilla-public-data:test.query_v1",
                ],
            )

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
        runner = CliRunner()

        with patch("subprocess.check_call") as mock_call:
            mock_call.return_value = True
            result = runner.invoke(
                run, [str(query_file), "--destination_table=query_v1"]
            )
            result.exit_code == 1

            result = runner.invoke(run, [str(query_file), "--dataset_id=test"])
            result.exit_code == 1
