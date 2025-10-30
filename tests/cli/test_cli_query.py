import os
import types
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.query import (
    NBR_DAYS_RETAINED,
    _update_query_schema_with_base_schemas,
    backfill,
    create,
    deploy,
    info,
    initialize,
    materialized_view_has_changes,
    paths_matching_name_pattern,
    schedule,
    update,
)
from bigquery_etl.metadata.publish_metadata import attach_metadata
from bigquery_etl.schema import Schema

DEFAULT_SAMPLING_BATCH_SIZE = 4
TOTAL_SAMPLE_ID_COUNT = 100


class TestQuery:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def create_test_dag(self, dag_name):
        os.mkdir("dags")

        dag_conf = {
            f"{dag_name}": {
                "schedule_interval": "daily",
                "default_args": {
                    "owner": "test@example.com",
                    "start_date": "2020-03-29",
                    "email": ["test@example.org"],
                    "retries": 1,
                },
            }
        }

        with open("dags.yaml", "w") as f:
            f.write(yaml.dump(dag_conf))

    def test_create_invalid_path(self, runner):
        with runner.isolated_filesystem():
            with open("foo.txt", "w") as f:
                f.write("")
            result = runner.invoke(create, ["test.query_v1", "--sql_dir=foo.txt"])
            assert result.exit_code == 2

    def test_create_invalid_query_name(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(create, ["invalid_query_name"])
            assert result.exit_code == 2

    def test_create_query(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            result = runner.invoke(create, ["test.test_query", "--no_schedule"])
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod") == ["test"]
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query_v1",
            ]
            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

    def test_create_query_in_default_dag(self, runner):
        with runner.isolated_filesystem():
            self.create_test_dag("bqetl_default")
            os.makedirs("sql/moz-fx-data-shared-prod")
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod") == ["test"]
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query_v1",
            ]
            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml"
            ) as file:
                exists = "dag_name: bqetl_default" in file.read()
                assert exists

    def test_create_query_in_named_dag(self, runner):
        with runner.isolated_filesystem():
            self.create_test_dag("bqetl_test")
            os.makedirs("sql/moz-fx-data-shared-prod")
            result = runner.invoke(create, ["test.test_query", "--dag=bqetl_test"])
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod") == ["test"]
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query_v1",
            ]
            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml"
            ) as file:
                exists = "dag_name: bqetl_test" in file.read()
                assert exists

    def test_create_query_with_version(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            result = runner.invoke(create, ["test.test_query_v4", "--no_schedule"])
            assert result.exit_code == 0
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query_v4",
            ]

    def test_create_derived_query_with_view(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test_derived")
            result = runner.invoke(create, ["test_derived.test_query", "--no_schedule"])
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql/moz-fx-data-shared-prod")
            assert "test" in os.listdir("sql/moz-fx-data-shared-prod")
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test_derived")) == [
                "dataset_metadata.yaml",
                "test_query_v1",
            ]
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query",
            ]
            assert sorted(
                os.listdir("sql/moz-fx-data-shared-prod/test/test_query")
            ) == ["metadata.yaml", "view.sql"]

    def test_create_derived_query_with_existing_view(self, runner):
        with runner.isolated_filesystem():
            project_dir = Path("sql/moz-fx-data-shared-prod")
            existing_query = (
                project_dir / "test_derived" / "test_query_v1" / "query.sql"
            )
            existing_query.parent.mkdir(parents=True)
            existing_query.touch()

            existing_view = project_dir / "test" / "test_query" / "view.sql"
            existing_view.parent.mkdir(parents=True)
            existing_view.touch()

            result = runner.invoke(
                create, ["test_derived.test_query_v2", "--no_schedule"]
            )
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql/moz-fx-data-shared-prod")
            assert "test" in os.listdir("sql/moz-fx-data-shared-prod")
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test_derived")) == [
                "dataset_metadata.yaml",
                "test_query_v1",
                "test_query_v2",
            ]
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query",
            ]
            assert os.listdir("sql/moz-fx-data-shared-prod/test/test_query") == [
                "view.sql"
            ]
            assert existing_view.read_text() == ""

    def test_create_query_as_derived(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            os.mkdir("sql/moz-fx-data-shared-prod/test_derived")
            os.mkdir("sql/moz-fx-data-shared-prod/test")
            result = runner.invoke(create, ["test.test_query", "--no_schedule"])
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql/moz-fx-data-shared-prod")
            assert "test" in os.listdir("sql/moz-fx-data-shared-prod")
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test_derived")) == [
                "dataset_metadata.yaml",
                "test_query_v1",
            ]
            assert sorted(os.listdir("sql/moz-fx-data-shared-prod/test")) == [
                "dataset_metadata.yaml",
                "test_query",
            ]
            assert sorted(
                os.listdir("sql/moz-fx-data-shared-prod/test/test_query")
            ) == ["metadata.yaml", "view.sql"]

    def test_schedule_invalid_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(schedule, ["/test/query_v1"])
            assert result.exit_code == 2

    def test_schedule_query_non_existing_dag(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/query_v1")
            open("sql/moz-fx-data-shared-prod/test/query_v1/query.sql", "a").close()
            result = runner.invoke(schedule, ["test.query_v1", "--dag=foo"])
            assert result.exit_code == 1

    def test_schedule_query(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            os.mkdir("dags")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            dag_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.com",
                        "start_date": "2020-03-29",
                        "email": ["test@example.org"],
                        "retries": 1,
                    },
                }
            }

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dag_conf))

            result = runner.invoke(
                schedule, ["telemetry_derived.query_v1", "--dag=bqetl_test"]
            )

            print(result.output)
            assert result.exit_code == 0

    def test_reschedule_query(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            os.mkdir("dags")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            dag_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.com",
                        "start_date": "2020-03-29",
                        "email": ["test@example.org"],
                        "retries": 1,
                    },
                }
            }

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dag_conf))

            result = runner.invoke(schedule, ["telemetry_derived.query_v1"])

            assert result.exit_code == 0

    def test_query_info(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            result = runner.invoke(info, ["*.telemetry_derived.query_v1"])
            assert result.exit_code == 0
            assert "No metadata" in result.output
            assert "path:" in result.output

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            result = runner.invoke(info, ["*.telemetry_derived.query_v1"])
            assert result.exit_code == 0
            assert "No metadata" not in result.output
            assert "description" in result.output
            assert "dag_name: bqetl_test" in result.output

    def test_info_name_pattern(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            os.mkdir("sql/moz-fx-data-shared-prod/telemetry_derived/query_v2")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            os.makedirs("sql/moz-fx-data-shared-prod/foo_derived/query_v2")
            with open(
                "sql/moz-fx-data-shared-prod/foo_derived/query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            result = runner.invoke(info, ["*.query_*"])
            assert result.exit_code == 0
            assert "foo_derived.query_v2" in result.output
            assert "telemetry_derived.query_v2" in result.output
            assert "telemetry_derived.query_v1" in result.output

            result = runner.invoke(
                info, ["foo_derived.*", "--project-id=moz-fx-data-shared-prod"]
            )
            assert result.exit_code == 0
            assert "foo_derived.query_v2" in result.output
            assert "telemetry_derived.query_v2" not in result.output
            assert "telemetry_derived.query_v1" not in result.output

            result = runner.invoke(info, ["*.query_v2"])
            assert result.exit_code == 0
            assert "foo_derived.query_v2" in result.output
            assert "telemetry_derived.query_v2" in result.output
            assert "telemetry_derived.query_v1" not in result.output

    def testpaths_matching_name_pattern(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            os.mkdir("sql/moz-fx-data-shared-prod/telemetry_derived/query_v2")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            os.makedirs("sql/moz-fx-data-shared-prod/foo_derived/query_v2")
            with open(
                "sql/moz-fx-data-shared-prod/foo_derived/query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            os.makedirs("sql/moz-fx-data-test-project/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-test-project/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert len(paths_matching_name_pattern("*", "sql/", None)) == 4
            assert (
                len(
                    paths_matching_name_pattern(
                        "*.sql", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 0
            )
            assert (
                len(
                    paths_matching_name_pattern(
                        "test", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 0
            )
            assert (
                len(
                    paths_matching_name_pattern(
                        "foo_derived", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 0
            )
            assert (
                len(
                    paths_matching_name_pattern(
                        "foo_derived*", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 1
            )
            assert len(paths_matching_name_pattern("*query*", "sql/", None)) == 4
            assert (
                len(
                    paths_matching_name_pattern(
                        "foo_derived.query_v2", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 1
            )

            assert (
                len(
                    paths_matching_name_pattern(
                        "telemetry_derived.query_v1", "sql/", "moz-fx-data-test-project"
                    )
                )
                == 1
            )

            assert (
                len(
                    paths_matching_name_pattern(
                        "moz-fx-data-test-project.telemetry_derived.query_v1",
                        "sql/",
                        None,
                    )
                )
                == 1
            )

            assert (
                len(
                    paths_matching_name_pattern(
                        "moz-fx-data-test-project.telemetry_derived.*", "sql/", None
                    )
                )
                == 1
            )

    def test_attach_metadata_labels(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
                "labels": {"test": 123, "foo": "abc", "review_bugs": [1234, 1254]},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            table = types.SimpleNamespace()
            attach_metadata(
                Path(
                    "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql"
                ),
                table,
            )

            assert "test" in table.labels
            assert table.labels["test"] == "123"
            assert "foo" in table.labels
            assert table.labels["foo"] == "abc"
            assert "review_bugs" not in table.labels

    def test_query_backfill_with_offset(self, runner):
        with (
            runner.isolated_filesystem(),
            # Mock client to avoid NotFound
            patch("google.cloud.bigquery.Client", autospec=True),
            patch("subprocess.check_call", autospec=True) as check_call,
        ):
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write(
                    "SELECT DATE('2021-01-01') as submission_date WHERE submission_date = @submission_date"
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test", "date_partition_offset": -2},
                "bigquery": {"time_partitioning": {"type": "day"}},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            result = runner.invoke(
                backfill,
                [
                    "telemetry_derived.query_v1",
                    "--project_id=moz-fx-data-shared-prod",
                    "--start_date=2021-01-05",
                    "--end_date=2021-01-09",
                    "--exclude=2021-01-06",
                    "--parallelism=0",
                    "--billing-project=backfill-project",
                    "--override-retention-range-limit",
                ],
            )

            assert result.exit_code == 0

            expected_submission_date_params = [
                f"--parameter=submission_date:DATE:2021-01-0{day}"
                for day in (3, 5, 6, 7)
            ]

            assert check_call.call_count == 4
            for call in check_call.call_args_list:
                submission_date_params = [
                    arg for arg in call.args[0] if "--parameter=submission_date" in arg
                ]
                assert len(submission_date_params) == 1
                assert submission_date_params[0] in expected_submission_date_params

    def test_query_backfill_with_scheduling_overrides(self, runner):
        with (
            runner.isolated_filesystem(),
            # Mock client to avoid NotFound
            patch("google.cloud.bigquery.Client", autospec=True),
            patch("subprocess.check_call", autospec=True) as check_call,
        ):
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write(
                    "SELECT DATE('2021-01-01') as submission_date WHERE submission_date = @submission_date"
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {
                    "dag_name": "bqetl_test",
                    "date_partition_parameter": None,
                    "parameters": [
                        "submission_date:DATE:{{(execution_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d')}}",
                    ],
                },
                "bigquery": {"time_partitioning": {"type": "day"}},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            result = runner.invoke(
                backfill,
                [
                    "telemetry_derived.query_v1",
                    "--project_id=moz-fx-data-shared-prod",
                    "--start_date=2021-01-05",
                    "--end_date=2021-01-06",
                    "--parallelism=0",
                    """--scheduling_overrides={"parameters": ["test:INT64:30"], "date_partition_parameter": "submission_date"}""",
                    "--override-retention-range-limit",
                ],
            )
            assert result.exit_code == 0

            expected_submission_date_params = [
                f"--parameter=submission_date:DATE:2021-01-0{day}" for day in (5, 6)
            ]
            assert check_call.call_count == 2

            for call in check_call.call_args_list:
                submission_date_params = [
                    arg for arg in call.args[0] if "--parameter=submission_date" in arg
                ]
                assert len(submission_date_params) == 1
                assert submission_date_params[0] in expected_submission_date_params

                test_params = [arg for arg in call.args[0] if "--parameter=test" in arg]
                assert len(test_params) == 1
                assert test_params[0] == "--parameter=test:INT64:30"

    def test_query_backfill_unpartitioned_with_parameters(self, runner):
        with (
            runner.isolated_filesystem(),
            # Mock client to avoid NotFound
            patch("google.cloud.bigquery.Client", autospec=True),
            patch("subprocess.check_call", autospec=True) as check_call,
        ):
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write(
                    "SELECT DATE('2021-01-01') as submission_date WHERE submission_date = @submission_date"
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {
                    "dag_name": "bqetl_test",
                    "date_partition_parameter": None,
                    "parameters": [
                        "submission_date:DATE:{{ds}}",
                        "conversion_window:INT64:30",
                    ],
                },
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            result = runner.invoke(
                backfill,
                [
                    "telemetry_derived.query_v1",
                    "--project_id=moz-fx-data-shared-prod",
                    "--start_date=2021-01-05",
                    "--end_date=2021-01-09",
                    "--parallelism=0",
                    "--override-retention-range-limit",
                ],
            )

            expected_destination_arg = (
                "--destination_table=moz-fx-data-shared-prod:telemetry_derived.query_v1"
            )
            expected_submission_date_params = [
                f"--parameter=submission_date:DATE:2021-01-0{day}"
                for day in range(5, 10)
            ]

            assert result.exit_code == 0

            assert check_call.call_count == 5
            for call in check_call.call_args_list:
                destination_table_params = [
                    arg for arg in call.args[0] if "--destination_table" in arg
                ]
                assert len(destination_table_params) == 1
                assert destination_table_params[0] == expected_destination_arg

                submission_date_params = [
                    arg for arg in call.args[0] if "--parameter=submission_date" in arg
                ]
                assert len(submission_date_params) == 1
                assert submission_date_params[0] in expected_submission_date_params

                conversion_params = [
                    arg
                    for arg in call.args[0]
                    if "--parameter=conversion_window" in arg
                ]
                assert len(conversion_params) == 1
                assert conversion_params[0] == "--parameter=conversion_window:INT64:30"

    @patch("bigquery_etl.cli.query.get_credentials")
    @patch("bigquery_etl.cli.query.get_id_token")
    @patch("bigquery_etl.cli.query.deploy_table")
    def test_deploy(
        self, mock_deploy_table, mock_get_id_token, mock_get_credentials, runner
    ):
        mock_deploy_table.return_value = None
        mock_get_id_token.return_value = None
        mock_get_credentials.return_value = None

        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
                "labels": {"test": 123, "foo": "abc", "review_bugs": [1234, 1254]},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            result = runner.invoke(deploy, ["telemetry_derived.query_v1"])

            assert result.exit_code == 0
            mock_deploy_table.assert_called_with(
                Path(
                    "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql"
                ),
                destination_table=None,
                force=False,
                use_cloud_function=True,
                skip_existing=False,
                skip_external_data=False,
                respect_dryrun_skip=True,
                sql_dir="sql/",
                credentials=None,
                id_token=None,
            )
            mock_get_id_token.assert_called_once()
            mock_get_credentials.assert_called_once()

    @patch("bigquery_etl.cli.query.get_credentials")
    @patch("bigquery_etl.cli.query.get_id_token")
    @patch("bigquery_etl.cli.query.deploy_table")
    def test_deploy_schema(
        self, mock_deploy_table, mock_get_id_token, mock_get_credentials, runner
    ):
        mock_deploy_table.return_value = None
        mock_get_id_token.return_value = None
        mock_get_credentials.return_value = None

        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/schema.yaml",
                "w",
            ) as f:
                f.write(
                    """
                fields:
                - name: x
                  type: INTEGER
                  mode: NULLABLE
                """
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
                "labels": {"test": 123, "foo": "abc", "review_bugs": [1234, 1254]},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            result = runner.invoke(deploy, ["telemetry_derived.query_v1"])

            assert result.exit_code == 0
            mock_deploy_table.assert_called_with(
                Path(
                    "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml"
                ),
                destination_table=None,
                force=False,
                use_cloud_function=True,
                skip_existing=False,
                skip_external_data=False,
                respect_dryrun_skip=True,
                sql_dir="sql/",
                credentials=None,
                id_token=None,
            )
            mock_get_id_token.assert_called_once()
            mock_get_credentials.assert_called_once()

    @patch("bigquery_etl.cli.query.get_credentials")
    @patch("bigquery_etl.cli.query.get_id_token")
    @patch("bigquery_etl.cli.query.deploy_table")
    def test_deploy_schema_no_duplicate(
        self, mock_deploy_table, mock_get_id_token, mock_get_credentials, runner
    ):
        mock_deploy_table.return_value = None
        mock_get_id_token.return_value = None
        mock_get_credentials.return_value = None

        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/schema.yaml",
                "w",
            ) as f:
                f.write(
                    """
                fields:
                - name: x
                  type: INTEGER
                  mode: NULLABLE
                """
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
                "labels": {"test": 123, "foo": "abc", "review_bugs": [1234, 1254]},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            result = runner.invoke(deploy, ["telemetry_derived.query_v1"])

            assert result.exit_code == 0
            mock_deploy_table.assert_called_once()
            mock_deploy_table.assert_called_with(
                Path(
                    "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql"
                ),
                destination_table=None,
                force=False,
                use_cloud_function=True,
                skip_existing=False,
                skip_external_data=False,
                respect_dryrun_skip=True,
                sql_dir="sql/",
                credentials=None,
                id_token=None,
            )
            mock_get_id_token.assert_called_once()
            mock_get_credentials.assert_called_once()

    @patch("bigquery_etl.cli.query.get_credentials")
    @patch("bigquery_etl.cli.query.get_id_token")
    @patch("bigquery_etl.cli.query.deploy_table")
    def test_prevent_deploy_for_views(
        self, mock_deploy_table, mock_get_id_token, mock_get_credentials, runner
    ):
        mock_deploy_table.return_value = None
        mock_get_id_token.return_value = None
        mock_get_credentials.return_value = None

        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/view.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/schema.yaml",
                "w",
            ) as f:
                f.write(
                    """
                fields:
                - name: x
                  type: INTEGER
                  mode: NULLABLE
                """
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
                "labels": {"test": 123, "foo": "abc", "review_bugs": [1234, 1254]},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            result = runner.invoke(deploy, ["telemetry_derived.query_v1"])

            assert result.exit_code == 0
            mock_deploy_table.assert_not_called()

    def test_materialized_view_has_changes(self):
        deployed_sql = " SELECT  * FROM project.dataset.table "

        assert not materialized_view_has_changes(
            deployed_sql,
            """
            CREATE MATERIALIZED VIEW project.dataset.view AS SELECT * FROM
            project.dataset.table
            """,
        )
        assert not materialized_view_has_changes(
            deployed_sql,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS project.dataset.view AS SELECT * FROM
            project.dataset.table
            """,
        )
        assert materialized_view_has_changes(
            deployed_sql,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS project.dataset.view AS SELECT * FROM
            project.dataset.table WHERE TRUE
            """,
        )
        assert not materialized_view_has_changes(
            deployed_sql,
            """
            CREATE OR REPLACE MATERIALIZED VIEW
            project.dataset.view AS SELECT * FROM
            project.dataset.table
            """,
        )

    # Check that if you try to run a backfill for a start date < retention days and retention = False, it exits
    def test_query_backfill_with_scheduling_overrides_outside_retention_limit(
        self, runner
    ):
        with (
            runner.isolated_filesystem(),
            # Mock client to avoid NotFound
            patch("google.cloud.bigquery.Client", autospec=True),
        ):
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write(
                    "SELECT DATE('2021-01-01') as submission_date WHERE submission_date = @submission_date"
                )

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {
                    "dag_name": "bqetl_test",
                    "date_partition_parameter": None,
                    "parameters": [
                        "submission_date:DATE:{{(execution_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d')}}",
                    ],
                },
                "bigquery": {"time_partitioning": {"type": "day"}},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            result = runner.invoke(
                backfill,
                [
                    "telemetry_derived.query_v1",
                    "--project_id=moz-fx-data-shared-prod",
                    f"--start_date={str(date.today() - timedelta(days=10+NBR_DAYS_RETAINED))}",
                    f"--end_date={str(date.today() - timedelta(days=10))}",
                    "--parallelism=0",
                    """--scheduling_overrides={"parameters": ["test:INT64:30"], "date_partition_parameter": "submission_date"}""",
                ],
            )
            assert result.exit_code == 1

    @patch("bigquery_etl.cli.query.ParallelTopologicalSorter")
    @patch("bigquery_etl.cli.query._update_query_schema_with_downstream")
    def test_schema_update(
        self,
        mock_update_query_schema_with_downstream,
        mock_sorter_,
        runner,
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
                "labels": {"test": 123, "foo": "abc", "review_bugs": [1234, 1254]},
            }

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            # the update() uses a map that we need to intercept to simulate a run and test
            # the mocked calls.
            mock_sorter = mock_sorter_.return_value

            def fake_map(f):
                mock_sorter._mapped = f

            def fake_run():
                mock_sorter._mapped()

            mock_sorter.map.side_effect = fake_map
            mock_sorter.run.side_effect = fake_run

            result = runner.invoke(update, ["telemetry_derived.query_v1"])
            mock_sorter.run()
            assert str(result) == "<Result okay>"
            assert mock_update_query_schema_with_downstream.call_count == 1

            result2 = runner.invoke(
                update, ["telemetry_derived.query_v1", "--use_global_schema"]
            )
            mock_sorter.run()
            assert str(result2) == "<Result okay>"
            assert mock_update_query_schema_with_downstream.call_count == 2

    def test_update_query_schema_with_base_schemas(self, runner, capsys):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            os.makedirs("bigquery_etl/schema")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            query_schema_yaml = {
                "fields": [
                    {"name": "column_1", "type": "DATE", "mode": "NULLABLE"},
                    {
                        "name": "dim_1",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "dim_1.",
                    },
                    {"name": "dim_2", "type": "INTEGER", "mode": "NULLABLE"},
                ]
            }
            global_schema_yaml = {
                "fields": [
                    {
                        "name": "dim_1",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "Updated global dim_1 description.",
                    },
                    {
                        "name": "dim_2",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                        "description": "Updated global dim_2 description.",
                    },
                ]
            }

            dataset_schema_yaml = {
                "fields": [
                    {
                        "name": "column_1",
                        "type": "DATE",
                        "mode": "NULLABLE",
                        "description": "Updated dataset column_1 description.",
                    },
                    {
                        "name": "dim_1",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "Updated dataset dim_1 description.",
                    },
                ]
            }
            expected2_schema_yaml = {
                "fields": [
                    {"name": "column_1", "type": "DATE", "mode": "NULLABLE"},
                    {
                        "name": "dim_1",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "Updated global dim_1 description.",
                    },
                    {
                        "name": "dim_2",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                        "description": "Updated global dim_2 description.",
                    },
                ]
            }

            expected4_schema_yaml = {
                "fields": [
                    {
                        "name": "column_1",
                        "type": "DATE",
                        "mode": "NULLABLE",
                        "description": "Updated dataset column_1 description.",
                    },
                    {
                        "name": "dim_1",
                        "type": "STRING",
                        "mode": "NULLABLE",
                        "description": "Updated dataset dim_1 description.",
                    },
                    {
                        "name": "dim_2",
                        "type": "INTEGER",
                        "mode": "NULLABLE",
                        "description": "Updated global dim_2 description.",
                    },
                ]
            }
            query_schema_path = Path(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/schema.yaml"
            )
            global_schema_path = Path("bigquery_etl/schema/global.yaml")
            dataset_schema_path = Path("bigquery_etl/schema/telemetry_derived.yaml")

            with open(query_schema_path, "w") as f:
                f.write(yaml.safe_dump(query_schema_yaml))
            query_schema = Schema.from_schema_file(query_schema_path)

            # Test using global schema when the global schema file doesn't exist.
            _ = _update_query_schema_with_base_schemas(
                query_schema, "telemetry_derived", False, True
            )
            captured = capsys.readouterr()
            assert (
                "WARNING: Option --use_global_schema was not applied due to missing required schema"
                in captured.out
            )

            # Test using global schema and the global schema file exists.
            with open(global_schema_path, "w") as f:
                f.write(yaml.dump(global_schema_yaml))
            result2 = _update_query_schema_with_base_schemas(
                query_schema, "telemetry_derived", False, True
            )
            captured = capsys.readouterr()
            assert expected2_schema_yaml["fields"] == result2.schema["fields"]
            assert (
                "[INFO] The following columns are missing descriptions:"
            ) in captured.out

            # Test not using any of the two base schemas: global, dataset.
            query_schema2 = Schema.from_schema_file(query_schema_path)
            result = _update_query_schema_with_base_schemas(
                query_schema2,
                "telemetry_derived",
                use_dataset_schema=False,
                use_global_schema=False,
            )
            captured = capsys.readouterr()
            assert query_schema2.schema["fields"] == result.schema["fields"]
            assert (
                "[INFO] The following columns are missing descriptions:"
            ) in captured.out

            # Test using both base schemas, when dataset schema is missing.
            query_schema3 = Schema.from_schema_file(query_schema_path)
            result = _update_query_schema_with_base_schemas(
                query_schema3,
                "telemetry_derived",
                use_dataset_schema=True,
                use_global_schema=True,
            )
            captured = capsys.readouterr()
            assert query_schema3.schema["fields"] == result.schema["fields"]
            assert (
                "WARNING: Option --use_dataset_schema was not applied due to missing required"
            ) in captured.out

            # Test using both base schemas: global, dataset, both are present.
            query_schema4 = Schema.from_schema_file(query_schema_path)
            with open(dataset_schema_path, "w") as f:
                f.write(yaml.dump(dataset_schema_yaml))
            result = _update_query_schema_with_base_schemas(
                query_schema4,
                "telemetry_derived",
                use_dataset_schema=True,
                use_global_schema=True,
            )
            captured = capsys.readouterr()
            assert expected4_schema_yaml["fields"] == result.schema["fields"]
            assert (
                "[WARNING] The following column descriptions were overwritten using the base schemas:"
            ) in captured.out

    @patch("bigquery_etl.cli.query.deploy_table")
    @patch("google.cloud.bigquery.Client.get_table")
    @patch("bigquery_etl.cli.query._run_query")
    def test_query_initialize(
        self, mock_run_query, mock_get_table, mock_deploy_table, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write(
                    "SELECT column_1 FROM test_table"
                    "{% raw %}"
                    "{% if is_init() %}"
                    "{% endraw %}"
                    "WHERE sample_id=@sample_id"
                    "{% raw %}"
                    "{% else %}"
                    "{% endraw %}"
                    "{% raw %}"
                    "{% endif %}"
                    "{% endraw %}"
                )

            mock_get_table.return_value = types.SimpleNamespace(num_rows=0)
            result = runner.invoke(initialize, ["*.telemetry_derived.query_v1"])

            assert result.exit_code == 0
            assert mock_run_query.call_count == TOTAL_SAMPLE_ID_COUNT

            sample_ids = range(0, TOTAL_SAMPLE_ID_COUNT)
            expected_sample_id_params = [
                f"--parameter=sample_id:INT64:{sample_id}" for sample_id in sample_ids
            ]

            for call in mock_run_query.call_args_list:
                sample_id_params = [
                    arg for arg in call.args[-1] if "--parameter=sample_id" in arg
                ]
                assert len(sample_id_params) == 1
                assert sample_id_params[0] in expected_sample_id_params

    @patch("bigquery_etl.cli.query.deploy_table")
    @patch("google.cloud.bigquery.Client.get_table")
    @patch("bigquery_etl.cli.query._run_query")
    def test_query_initialize_batch(
        self, mock_run_query, mock_get_table, mock_deploy_table, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write(
                    "SELECT column_1 FROM test_table"
                    "{% raw %}"
                    "{% if is_init() %}"
                    "{% endraw %}"
                    "WHERE sample_id >= @sample_id "
                    "AND sample_id < @sample_id + @sampling_batch_size"
                    "{% raw %}"
                    "{% else %}"
                    "{% endraw %}"
                    "{% raw %}"
                    "{% endif %}"
                    "{% endraw %}"
                )

            mock_get_table.return_value = types.SimpleNamespace(num_rows=0)
            result = runner.invoke(initialize, ["*.telemetry_derived.query_v1"])

            assert result.exit_code == 0
            assert (
                mock_run_query.call_count
                == TOTAL_SAMPLE_ID_COUNT // DEFAULT_SAMPLING_BATCH_SIZE
            )

            sample_ids = range(0, TOTAL_SAMPLE_ID_COUNT, DEFAULT_SAMPLING_BATCH_SIZE)
            expected_sample_id_params = [
                f"--parameter=sample_id:INT64:{sample_id}" for sample_id in sample_ids
            ]

            for call in mock_run_query.call_args_list:
                sample_id_params = [
                    arg for arg in call.args[-1] if "--parameter=sample_id" in arg
                ]
                assert len(sample_id_params) == 1
                assert sample_id_params[0] in expected_sample_id_params
