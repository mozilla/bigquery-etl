import os

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.query import (
    _queries_matching_name_pattern,
    create,
    info,
    schedule,
)


class TestQuery:
    @pytest.fixture
    def runner(self):
        return CliRunner()

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
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod") == ["test"]
            assert os.listdir("sql/moz-fx-data-shared-prod/test") == ["test_query_v1"]
            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

    def test_create_query_with_version(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            result = runner.invoke(create, ["test.test_query_v4"])
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod/test") == ["test_query_v4"]

    def test_create_derived_query_with_view(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test_derived")
            result = runner.invoke(create, ["test_derived.test_query"])
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql/moz-fx-data-shared-prod")
            assert "test" in os.listdir("sql/moz-fx-data-shared-prod")
            assert os.listdir("sql/moz-fx-data-shared-prod/test_derived") == [
                "test_query_v1"
            ]
            assert os.listdir("sql/moz-fx-data-shared-prod/test") == ["test_query"]
            assert os.listdir("sql/moz-fx-data-shared-prod/test/test_query") == [
                "view.sql"
            ]

    def test_create_query_as_derived(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            os.mkdir("sql/moz-fx-data-shared-prod/test_derived")
            os.mkdir("sql/moz-fx-data-shared-prod/test")
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql/moz-fx-data-shared-prod")
            assert "test" in os.listdir("sql/moz-fx-data-shared-prod")
            assert os.listdir("sql/moz-fx-data-shared-prod/test_derived") == [
                "test_query_v1"
            ]
            assert os.listdir("sql/moz-fx-data-shared-prod/test") == ["test_query"]
            assert os.listdir("sql/moz-fx-data-shared-prod/test/test_query") == [
                "view.sql"
            ]

    def test_create_query_with_init(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            result = runner.invoke(create, ["test.test_query", "--init"])
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod/test") == ["test_query_v1"]
            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            assert "init.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

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

    @pytest.mark.java
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

    @pytest.mark.java
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

    def test_queries_matching_name_pattern(self, runner):
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

            assert len(_queries_matching_name_pattern("*", "sql/", None)) == 4
            assert (
                len(
                    _queries_matching_name_pattern(
                        "*.sql", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 0
            )
            assert (
                len(
                    _queries_matching_name_pattern(
                        "test", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 0
            )
            assert (
                len(
                    _queries_matching_name_pattern(
                        "foo_derived", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 0
            )
            assert (
                len(
                    _queries_matching_name_pattern(
                        "foo_derived*", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 1
            )
            assert len(_queries_matching_name_pattern("*query*", "sql/", None)) == 4
            assert (
                len(
                    _queries_matching_name_pattern(
                        "foo_derived.query_v2", "sql/", "moz-fx-data-shared-prod"
                    )
                )
                == 1
            )

            assert (
                len(
                    _queries_matching_name_pattern(
                        "telemetry_derived.query_v1", "sql/", "moz-fx-data-test-project"
                    )
                )
                == 1
            )

            assert (
                len(
                    _queries_matching_name_pattern(
                        "moz-fx-data-test-project.telemetry_derived.query_v1",
                        "sql/",
                        None,
                    )
                )
                == 1
            )

            assert (
                len(
                    _queries_matching_name_pattern(
                        "moz-fx-data-test-project.telemetry_derived.*", "sql/", None
                    )
                )
                == 1
            )
