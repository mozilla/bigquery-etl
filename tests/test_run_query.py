import os
from textwrap import dedent
from unittest.mock import Mock, patch

import yaml
from click.testing import CliRunner

from bigquery_etl.cli.query import extract_and_run_temp_udfs, run


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
                    "--destination_table=query_v1",
                    "--dataset_id=test",
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
                    "--destination_table=mozdata:test.query_v1",
                    "--dataset_id=test",
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
                    "--destination_table=mozilla-public-data:test.query_v1",
                    "--dataset_id=test",
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
            assert result.exit_code == 1

            result = runner.invoke(run, [str(query_file), "--dataset_id=test"])
            assert result.exit_code == 1

    @patch("subprocess.check_call")
    @patch("google.cloud.bigquery.Client")
    def test_run_query_billing_project(
        self, mock_client, mock_subprocess_call, tmp_path
    ):
        query_file_path = (
            tmp_path / "sql" / "moz-fx-data-shared-prod" / "dataset_1" / "query_v1"
        )
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 1")

        runner = CliRunner()

        # bigquery.client().query().session_info.session_id = ...
        mock_query_call = Mock()
        mock_query_call.return_value.session_info.session_id = "1234567890"
        mock_client.return_value.query = mock_query_call

        mock_subprocess_call.return_value = 1
        result = runner.invoke(
            run,
            [
                str(query_file),
                "--billing-project=project-2",
                "--project-id=moz-fx-data-shared-prod",
            ],
        )
        assert result.exit_code == 0

        assert mock_query_call.call_count == 1
        query_text, query_job_config = mock_query_call.call_args.args
        assert (
            query_text
            == "SET @@dataset_project_id = 'moz-fx-data-shared-prod';\nSET @@dataset_id = 'dataset_1'"
        )
        assert query_job_config.create_session is True

        assert mock_subprocess_call.call_args.args == (
            [
                "bq",
                "query",
                "--project_id=project-2",
                "--session_id=1234567890",
            ],
        )

    @patch("subprocess.check_call")
    @patch("google.cloud.bigquery.Client")
    def test_run_query_billing_project_temp_udf(
        self, mock_client, mock_subprocess_call, tmp_path
    ):
        query_file_path = (
            tmp_path / "sql" / "moz-fx-data-shared-prod" / "dataset_1" / "query_v1"
        )
        os.makedirs(query_file_path)
        query_file = query_file_path / "query.sql"

        temp_udf_sql = "CREATE TEMP FUNCTION fn(param INT) AS (1);"
        query_file.write_text(f"{temp_udf_sql} SELECT 1")

        runner = CliRunner()

        # bigquery.client().query().session_info.session_id = ...
        mock_query_call = Mock()
        mock_query_call.return_value.session_info.session_id = "1234567890"
        mock_client.return_value.query = mock_query_call

        mock_query_and_wait_call = Mock()
        session_id = "1234567890"
        mock_client.return_value.query_and_wait = mock_query_and_wait_call

        mock_subprocess_call.return_value = 1
        result = runner.invoke(
            run,
            [
                str(query_file),
                "--billing-project=project-2",
                "--project-id=moz-fx-data-shared-prod",
                "--destination-table=proj.dataset.table",
            ],
        )
        assert result.exit_code == 0

        assert mock_query_call.call_count == 1

        # udf define query
        assert mock_query_and_wait_call.call_count == 1
        query_text = mock_query_and_wait_call.call_args[0][0]
        query_job_config = mock_query_and_wait_call.call_args[1]["job_config"]
        assert query_text == temp_udf_sql
        assert query_job_config.connection_properties[0].value == session_id

    @patch("google.cloud.bigquery.Client")
    def test_extract_and_run_temp_udfs(self, mock_client):
        mock_client.return_value = mock_client

        udf_sql = dedent(
            """
            CREATE TEMP FUNCTION f1(arr ARRAY<INT64>) AS (
              (SELECT SUM(a) FROM UNNEST(arr) AS a)
            );

            CREATE  -- inline comment
              TEMPORARY
                FUNCTION f2() AS (
              (SELECT 1)
            );

            CREATE TEMP FUNCTION f3() RETURNS INT64 AS (1);

            -- javascript
            CREATE TEMP FUNCTION f4(input JSON)
            RETURNS JSON
            DETERMINISTIC
            LANGUAGE js
            AS
            \"\"\"
                return "abc";
            \"\"\";

            CREATE TEMP FUNCTION f5()
            RETURNS STRING LANGUAGE js
            AS "return 'abc'";
            """
        )
        query_sql = dedent(
            """
            WITH abc AS (
              SELECT * FROM UNNEST([1, 2, 3]) AS n
            )
            SELECT "CREATE TEMP FUNCTION f3() AS (1);", * FROM abc
            """
        )
        sql = f"{udf_sql}{query_sql}"

        updated_query = extract_and_run_temp_udfs(
            sql, project_id="project-1", session_id="123"
        )

        assert updated_query == query_sql.strip()

        mock_client.assert_called_once_with(project="project-1")

        assert mock_client.query_and_wait.call_count == 1

        # remove empty lines for comparison
        udf_sql = "\n".join([line for line in udf_sql.split("\n") if line.strip()])
        assert mock_client.query_and_wait.call_args[0][0] == udf_sql
