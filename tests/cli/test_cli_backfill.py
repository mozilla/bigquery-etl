import os
from datetime import date, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    TODAY,
    Backfill,
    BackfillStatus,
)
from bigquery_etl.cli.backfill import create, validate

DEFAULT_STATUS = BackfillStatus.DRAFTING
VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"
VALID_BACKFILL = Backfill(
    date(2021, 5, 3),
    date(2021, 1, 3),
    date(2021, 5, 3),
    [date(2021, 2, 3)],
    VALID_REASON,
    [VALID_WATCHER],
    DEFAULT_STATUS,
)


class TestBackfill:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_backfill(self, runner):
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
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/" + BACKFILL_FILE

            backfill = Backfill.entries_from_file(backfill_file)[0]

            assert backfill.entry_date == TODAY
            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == TODAY
            assert backfill.watchers == [DEFAULT_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS

    def test_create_backfill_with_invalid_watcher(self, runner):
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

    def test_create_backfill_with_invalid_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(
                create, ["test.test_query_v1", "--start_date=2021-03-01"]
            )
            assert result.exit_code == 2

    def test_create_backfill_with_invalid_start_date(self, runner):
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

    def test_create_backfill_with_invalid_excluded_date_before_start_date(self, runner):
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

    def test_create_backfill_with_excluded_date_after_end_date(self, runner):
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

    def test_create_backfill_with_non_existing_path(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                ],
            )
            assert result.exit_code == 2

    def test_create_backfill_entry_with_params(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    "--end_date=2021-03-10",
                    "--exclude=2021-03-05",
                    "--watcher=test@example.org",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/" + BACKFILL_FILE

            backfill = Backfill.entries_from_file(backfill_file)[0]

            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == date(2021, 3, 10)
            assert backfill.watchers == [VALID_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS

    def test_create_backfill_with_exsting_entry(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_entry_1 = Backfill(
                date(2021, 5, 3),
                date(2021, 1, 3),
                date(2021, 5, 3),
                [date(2021, 2, 3)],
                VALID_REASON,
                [VALID_WATCHER],
                DEFAULT_STATUS,
            )

            backfill_entry_2 = Backfill(
                TODAY,
                date(2023, 3, 1),
                date(2023, 3, 10),
                [],
                DEFAULT_REASON,
                [DEFAULT_WATCHER],
                DEFAULT_STATUS,
            )

            backfill_file = (
                Path("sql/moz-fx-data-shared-prod/test/test_query_v1") / BACKFILL_FILE
            )
            backfill_file.write_text(backfill_entry_1.to_yaml())

            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2023-03-01",
                    "--end_date=2023-03-10",
                ],
            )

            assert result.exit_code == 0

            backfills = Backfill.entries_from_file(backfill_file)

            assert backfills[1] == backfill_entry_1
            assert backfills[0] == backfill_entry_2

    def test_validate_backfill_invalid_table_name(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert (
                "Qualified table name must be named like: <project>.<dataset>.<table>"
                in validate_backfill_result.output
            )

    def test_validate_backfill_non_existing_table_name(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v2",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "does not exist" in validate_backfill_result.output

    def test_validate_backfill(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(VALID_BACKFILL.to_yaml())

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 0

    def test_validate_backfill_invalid_reason(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            VALID_BACKFILL.reason = DEFAULT_REASON
            backfill_file.write_text(VALID_BACKFILL.to_yaml())
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid Reason" in validate_backfill_result.output

    def test_validate_backfill_empty_reason(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            VALID_BACKFILL.reason = ""
            backfill_file.write_text(VALID_BACKFILL.to_yaml())
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid Reason" in validate_backfill_result.output

    def test_validate_backfill_invalid_watcher(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            VALID_BACKFILL.watchers = DEFAULT_WATCHER
            backfill_file.write_text(VALID_BACKFILL.to_yaml())
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid Watcher" in validate_backfill_result.output

    def test_validate_backfill_empty_watcher(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            VALID_BACKFILL.watchers = ""
            backfill_file.write_text(VALID_BACKFILL.to_yaml())
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid Watchers" in validate_backfill_result.output

    def test_validate_backfill_invalid_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  excluded_dates:\n"
                "  - 2021-02-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: INVALIDSTATUS\n"
            )
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "INVALIDSTATUS" in str(validate_backfill_result.exception)

    def test_validate_backfill_duplicate_entry_dates(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE

            backfill_file.write_text(
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  excluded_dates:\n"
                "  - 2021-02-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Drafting\n"
                "\n"
                "2021-05-03:\n"
                "  start_date: 2020-01-03\n"
                "  end_date: 2020-05-03\n"
                "  excluded_dates:\n"
                "  - 2020-02-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Drafting\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Backfill entry already exists" in validate_backfill_result.output

    def test_validate_backfill_invalid_entry_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            VALID_BACKFILL.entry_date = TODAY + timedelta(days=1)
            backfill_file.write_text(VALID_BACKFILL.to_yaml())
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid entry date" in validate_backfill_result.output


# TODO: Fix failing tests
# def test_validate_backfill_invalid_start_date_greater_than_end_date(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.start_date = VALID_BACKFILL.end_date + timedelta(days=1)
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid start date" in validate_backfill_result.output
#
# def test_validate_backfill_invalid_start_date_greater_than_entry_date(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.start_date = VALID_BACKFILL.entry_date + timedelta(days=1)
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid start date" in validate_backfill_result.output
#
# def test_validate_backfill_invalid_start_date_greater_than_today(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.start_date = TODAY + timedelta(days=1)
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid start date" in validate_backfill_result.output
#
# def test_validate_backfill_invalid_end_date_greater_than_entry_date(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.end_date = VALID_BACKFILL.entry_date + timedelta(days=1)
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid end date" in validate_backfill_result.output
#
# def test_validate_backfill_invalid_end_date_greater_than_today(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.end_date = TODAY + timedelta(days=1)
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid end date" in validate_backfill_result.output
#
# def test_validate_backfill_invalid_excluded_dates_less_than_start_date(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.excluded_dates = [VALID_BACKFILL.start_date - timedelta(days=1)]
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid excluded dates" in validate_backfill_result.output
#
# def test_validate_backfill_invalid_excluded_dates_greater_than_end_date(self, runner):
#     with runner.isolated_filesystem():
#         SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
#         os.makedirs(SQL_DIR)
#
#         backfill_file = Path(SQL_DIR) / BACKFILL_FILE
#         VALID_BACKFILL.excluded_dates = [VALID_BACKFILL.end_date + timedelta(days=1)]
#         backfill_file.write_text(VALID_BACKFILL.to_yaml())
#
#         assert BACKFILL_FILE in os.listdir(SQL_DIR)
#
#         validate_backfill_result = runner.invoke(
#             validate,
#             [
#                 "moz-fx-data-shared-prod.test.test_query_v1",
#             ],
#         )
#         assert validate_backfill_result.exit_code == 1
#         assert "Invalid excluded dates" in validate_backfill_result.output


# TODO: add tests
# overlap
# sorted
