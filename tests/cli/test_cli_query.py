import os
import pytest
from click.testing import CliRunner
import yaml

from bigquery_etl.cli.query import create, schedule


class TestQuery:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_invalid_path(self, runner):
        with runner.isolated_filesystem():
            with open("foo.txt", "w") as f:
                f.write("")
            result = runner.invoke(create, ["test.query_v1", "--path=foo.txt"])
            assert result.exit_code == 2

    def test_create_invalid_query_name(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(create, ["invalid_query_name"])
            assert result.exit_code == 2

    def test_create_query(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert os.listdir("sql") == ["test"]
            assert os.listdir("sql/test") == ["test_query_v1"]
            assert "query.sql" in os.listdir("sql/test/test_query_v1")
            assert "metadata.yaml" in os.listdir("sql/test/test_query_v1")

    def test_create_query_with_version(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            result = runner.invoke(create, ["test.test_query_v4"])
            assert result.exit_code == 0
            assert os.listdir("sql/test") == ["test_query_v4"]

    def test_create_derived_query_with_view(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("sql/test_derived")
            result = runner.invoke(create, ["test_derived.test_query"])
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql")
            assert "test" in os.listdir("sql")
            assert os.listdir("sql/test_derived") == ["test_query_v1"]
            assert os.listdir("sql/test") == ["test_query"]
            assert os.listdir("sql/test/test_query") == ["view.sql"]

    def test_create_query_as_derived(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("sql/test_derived")
            os.mkdir("sql/test")
            result = runner.invoke(create, ["test.test_query"])
            assert result.exit_code == 0
            assert "test_derived" in os.listdir("sql")
            assert "test" in os.listdir("sql")
            assert os.listdir("sql/test_derived") == ["test_query_v1"]
            assert os.listdir("sql/test") == ["test_query"]
            assert os.listdir("sql/test/test_query") == ["view.sql"]

    def test_create_query_with_init(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            result = runner.invoke(create, ["test.test_query", "--init"])
            assert result.exit_code == 0
            assert os.listdir("sql/test") == ["test_query_v1"]
            assert "query.sql" in os.listdir("sql/test/test_query_v1")
            assert "metadata.yaml" in os.listdir("sql/test/test_query_v1")
            assert "init.sql" in os.listdir("sql/test/test_query_v1")

    def test_schedule_invalid_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(schedule, ["/test/query_v1"])
            assert result.exit_code == 2

    def test_schedule_invalid_query_path(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("sql/test")
            os.mkdir("sql/query_v1")
            result = runner.invoke(schedule, ["/test/query_v1"])
            assert result.exit_code == 2

    def test_schedule_query_non_existing_dag(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("sql/test")
            os.mkdir("sql/test/query_v1")
            open("sql/test/query_v1/query.sql", "a").close()
            result = runner.invoke(schedule, ["sql/test/query_v1", "--dag=foo"])
            assert result.exit_code == 1

    def test_schedule_query(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("dags")
            os.mkdir("sql/telemetry_derived")
            os.mkdir("sql/telemetry_derived/query_v1")
            with open("sql/telemetry_derived/query_v1/query.sql", "w") as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
            }

            with open("sql/telemetry_derived/query_v1/metadata.yaml", "w") as f:
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
                schedule, ["sql/telemetry_derived/query_v1", "--dag=bqetl_test"]
            )

            assert result.exit_code == 0

    def test_reschedule_query(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("sql")
            os.mkdir("dags")
            os.mkdir("sql/telemetry_derived")
            os.mkdir("sql/telemetry_derived/query_v1")
            with open("sql/telemetry_derived/query_v1/query.sql", "w") as f:
                f.write("SELECT 1")

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "scheduling": {"dag_name": "bqetl_test"},
            }

            with open("sql/telemetry_derived/query_v1/metadata.yaml", "w") as f:
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

            result = runner.invoke(schedule, ["sql/telemetry_derived/query_v1"])

            assert result.exit_code == 0
