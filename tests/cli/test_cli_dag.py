import os
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.dag import create, generate, info, remove

TEST_DIR = Path(__file__).parent.parent


class TestDag:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_dag_info_without_tasks(self, runner):
        result = runner.invoke(
            info, ["--dags_config=" + str(TEST_DIR / "data" / "dags.yaml")]
        )
        assert result.exit_code == 0
        assert "bqetl_core" in result.output
        assert "bqetl_events" in result.output

    def test_dag_info_with_tasks(self, runner):
        result = runner.invoke(
            info,
            [
                "--with_tasks",
            ],
        )
        assert result.exit_code == 0

    def test_single_dag_info(self, runner):
        result = runner.invoke(
            info,
            ["bqetl_core", "--dags_config=" + str(TEST_DIR / "data" / "dags.yaml")],
        )
        assert result.exit_code == 0
        assert "bqetl_core" in result.output
        assert "bqetl_events" not in result.output

    def test_dag_create_invalid_name(self, runner):
        with runner.isolated_filesystem():
            dags_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                    "tags": ["impact/tier_3"],
                }
            }

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dags_conf))

            result = runner.invoke(
                create,
                [
                    "invalid_dag_name",
                    "--schedule_interval=daily",
                    "--owner=test@example.org",
                    "--description=test",
                    "--start_date=2020-01-01",
                    "--tag=impact/tier_3",
                ],
            )
            assert result.exit_code == 1

    def test_dag_create(self, runner):
        with runner.isolated_filesystem():
            dags_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                    "tags": ["impact/tier_3"],
                }
            }

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dags_conf))

            result = runner.invoke(
                create,
                [
                    "bqetl_new_dag",
                    "--schedule_interval=daily",
                    "--owner=test@example.org",
                    "--description=test",
                    "--start_date=2020-01-01",
                    "--tag=impact/tier_3",
                ],
            )
            assert result.exit_code == 0

            with open("dags.yaml", "r") as dags_file:
                dags_conf = yaml.safe_load(dags_file.read())
                assert "bqetl_new_dag" in dags_conf
                assert "bqetl_test" in dags_conf
                assert (
                    dags_conf["bqetl_new_dag"]["default_args"]["owner"]
                    == "test@example.org"
                )
                assert dags_conf["bqetl_new_dag"]["description"] == "test"
                assert (
                    dags_conf["bqetl_new_dag"]["default_args"]["start_date"]
                    == "2020-01-01"
                )
                assert dags_conf["bqetl_new_dag"]["schedule_interval"] == "daily"

    def test_dag_remove_non_existing_dag(self, runner):
        with runner.isolated_filesystem():
            dags_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                },
                "bqetl_test_2": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                },
            }

            os.makedirs("sql/moz-fx-data-shared-prod")
            os.mkdir("dags")

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dags_conf))

            result = runner.invoke(
                remove,
                ["non_existing_dag"],
            )
            assert result.exit_code == 1

    def test_dag_remove(self, runner):
        with runner.isolated_filesystem():
            dags_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                },
                "bqetl_test_2": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                },
            }

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dags_conf))

            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql",
                "w",
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

            os.mkdir("dags")
            with open("dags/bqetl_test.py", "w") as f:
                f.write("")

            result = runner.invoke(
                remove,
                ["bqetl_test"],
            )
            assert result.exit_code == 0
            assert os.listdir("dags") == []

            with open("dags.yaml", "r") as dags_file:
                dags_conf = yaml.safe_load(dags_file.read())
                assert "bqetl_test_2" in dags_conf
                assert "bqetl_test" not in dags_conf

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "r",
            ) as f:
                metadata = yaml.safe_load(f.read())
                assert "scheduling" not in metadata

    def test_dag_generate_without_any_tasks(self, runner):
        with runner.isolated_filesystem():
            dags_conf = {
                "bqetl_test": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                },
            }

            os.makedirs("sql/moz-fx-data-shared-prod")
            os.mkdir("dags")

            with open("dags.yaml", "w") as f:
                f.write(yaml.dump(dags_conf))

            result = runner.invoke(
                generate,
                ["bqetl_test"],
            )

            assert result.exit_code == 0
