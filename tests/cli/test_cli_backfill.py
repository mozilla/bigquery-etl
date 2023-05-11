import os
from datetime import date

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.backfill.parse import BackfillStatus, DEFAULT_REASON
from bigquery_etl.cli.backfill import create

DEFAULT_WATCHER = "example@mozilla.com"
VALID_STATUS = BackfillStatus.Drafting.value


class TestBackfill:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_backfill(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                ],
            )
            assert result.exit_code == 0
            assert "backfill.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

    def test_invalid_watcher(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    "--watcher=test.org",
                ],
            )
            assert result.exit_code == 1

    def test_invalid_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(
                create, ["test.test_query_v1", "--start_date=2021-03-01"]
            )
            assert result.exit_code == 2

    def test_invalid_start_date_before_end_date(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--end_date=2021-03-01",
                ],
            )
            assert result.exit_code == 1

    def test_invalid_excluded_date_before_start_date(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--exclude=2021-03-01",
                ],
            )
            assert result.exit_code == 1

    def test_invalid_excluded_date_after_end_date(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--end_date=2021-06-01",
                    "--exclude=2021-07-01",
                ],
            )
            assert result.exit_code == 1

    def test_non_existing_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                ],
            )
            assert result.exit_code == 2

    def test_create_backfill_entry(self, runner):
        with runner.isolated_filesystem():

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                ],
            )

            assert result.exit_code == 0
            assert "backfill.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/backfill.yaml"

            with open(backfill_file, "r") as yaml_stream:
                try:
                    backfills = yaml.safe_load(yaml_stream)

                    for entry_date, backfill in backfills.items():

                        assert backfill["start_date"] == date(2021, 3, 1)
                        assert backfill["end_date"] == date.today()
                        assert backfill["watchers"] == [DEFAULT_WATCHER]
                        assert backfill.get("reason", None) == DEFAULT_REASON
                        assert backfill.get("status", None) == VALID_STATUS

                except yaml.YAMLError as e:
                    raise e

    def test_create_backfill_with_exsting_entries(self, runner):
        pass
        # with runner.isolated_filesystem():
        #
        #     SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
        #     os.makedirs(SQL_DIR)
        #     result_1 = runner.invoke(
        #         create,
        #         ["moz-fx-data-shared-prod.test.test_query_v1",
        #          "--start_date=2021-03-01",
        #          "--end_date=2021-03-10"]
        #     )
        #
        #     assert result_1.exit_code == 0
        #     assert "backfill.yaml" in os.listdir(
        #         "sql/moz-fx-data-shared-prod/test/test_query_v1"
        #     )
        #
        #     result_2 = runner.invoke(
        #         create,
        #         ["moz-fx-data-shared-prod.test.test_query_v1",
        #          "--start_date=2021-04-01",
        #          "--end_date=2021-05-10"]
        #     )
        #
        #     assert result_2.exit_code == 0
        #     assert "backfill.yaml" in os.listdir(
        #         "sql/moz-fx-data-shared-prod/test/test_query_v1"
        #     )
        #
        #     backfill_file = SQL_DIR + "/backfill.yaml"
