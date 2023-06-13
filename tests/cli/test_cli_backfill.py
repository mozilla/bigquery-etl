import os
from datetime import date, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from bigquery_etl.cli.backfill import create, info, validate

DEFAULT_STATUS = BackfillStatus.DRAFTING
VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"
VALID_BACKFILL = Backfill(
    date(2021, 5, 4),
    date(2021, 1, 3),
    date(2021, 5, 3),
    [date(2021, 2, 3)],
    VALID_REASON,
    [VALID_WATCHER],
    DEFAULT_STATUS,
)
BACKFILL_YAML_TEMPLATE = (
    "2021-05-04:\n"
    "  start_date: 2021-01-03\n"
    "  end_date: 2021-05-03\n"
    "  excluded_dates:\n"
    "  - 2021-02-03\n"
    "  reason: test_reason\n"
    "  watchers:\n"
    "  - test@example.org\n"
    "  status: Drafting\n"
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

            assert backfill.entry_date == date.today()
            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == date.today()
            assert backfill.watchers == [DEFAULT_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS

    def test_create_backfill_with_invalid_watcher(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            invalid_watcher = "test.org"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    "--watcher=" + invalid_watcher,
                ],
            )
            assert result.exit_code == 1
            assert "Invalid" in str(result.exception)
            assert "watchers" in str(result.exception)

    def test_create_backfill_with_invalid_path(self, runner):
        with runner.isolated_filesystem():
            invalid_path = "test.test_query_v1"
            result = runner.invoke(create, [invalid_path, "--start_date=2021-03-01"])
            assert result.exit_code == 2
            assert "Invalid" in result.output
            assert "path" in result.output

    def test_create_backfill_with_invalid_start_date_greater_than_end_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            invalid_start_date = "2021-05-01"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=" + invalid_start_date,
                    "--end_date=2021-03-01",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid start date" in str(result.exception)

    def test_create_backfill_with_invalid_excluded_dates_before_start_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            invalid_exclude_date = "2021-03-01"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--exclude=" + invalid_exclude_date,
                ],
            )
            assert result.exit_code == 1
            assert "Invalid excluded dates" in str(result.exception)

    def test_create_backfill_with_excluded_dates_after_end_date(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            invalid_exclude_date = "2021-07-01"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--end_date=2021-06-01",
                    "--exclude=" + invalid_exclude_date,
                ],
            )
            assert result.exit_code == 1
            assert "Invalid excluded dates" in str(result.exception)

    def test_create_backfill_entry_with_all_params(self, runner):
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
                date.today(),
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

            backfills = Backfill.entries_from_file(backfill_file)
            assert backfills[0] == backfill_entry_1

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

    def test_validate_backfill(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(BACKFILL_YAML_TEMPLATE)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 0

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

    def test_validate_backfill_invalid_default_reason(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_REASON, DEFAULT_REASON
            )
            backfill_file.write_text(invalid_backfill)
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
            invalid_reason = ""
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_REASON, invalid_reason
            )
            backfill_file.write_text(invalid_backfill)
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
            invalid_watcher = "test@example"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_WATCHER, invalid_watcher
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid" in validate_backfill_result.output
            assert "watchers" in validate_backfill_result.output

    def test_validate_backfill_empty_watcher(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_watcher = ""
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_WATCHER, invalid_watcher
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid" in validate_backfill_result.output
            assert "watchers" in validate_backfill_result.output

    def test_validate_backfill_watchers_duplicated(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_watchers = "  - test@example.org\n" "  - test@example.org\n"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                "  - " + VALID_WATCHER, invalid_watchers
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Duplicate or default watcher" in validate_backfill_result.output

    def test_validate_backfill_invalid_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_status = "INVALIDSTATUS"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                DEFAULT_STATUS.value, invalid_status
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert invalid_status in str(validate_backfill_result.exception)

    def test_validate_backfill_duplicate_entry_dates(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE

            duplicate_entry_date = "2021-05-05"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                "2021-05-04", duplicate_entry_date
            )
            backfill_file.write_text(invalid_backfill + "\n" + invalid_backfill)

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
            invalid_entry_date = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-05-04", invalid_entry_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "can't be in the future" in validate_backfill_result.output

    def test_validate_backfill_invalid_start_date_greater_than_end_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_start_date = "2021-05-04"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-01-03", invalid_start_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid start date" in validate_backfill_result.output

    #
    def test_validate_backfill_invalid_start_date_greater_than_entry_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_start_date = "2021-05-05"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-01-03", invalid_start_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid start date" in validate_backfill_result.output

    def test_validate_backfill_invalid_end_date_greater_than_entry_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_end_date = "2021-05-05"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-05-03", invalid_end_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid end date" in validate_backfill_result.output

    def test_validate_backfill_invalid_excluded_dates_less_than_start_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_excluded_date = "2021-01-02"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-02-03", invalid_excluded_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid excluded dates" in validate_backfill_result.output

    def test_validate_backfill_invalid_excluded_dates_greater_than_end_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_excluded_date = "2021-05-04"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-02-03", invalid_excluded_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "Invalid excluded dates" in validate_backfill_result.output

    def test_validate_backfill_invalid_excluded_dates_duplicated(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            duplicate_excluded_dates = "  - 2021-02-03\n" "  - 2021-02-03\n"
            backfill_file.write_text(
                (
                    "2021-05-04:\n"
                    "  start_date: 2021-01-03\n"
                    "  end_date: 2021-05-03\n"
                    "  excluded_dates:\n"
                    + duplicate_excluded_dates
                    + "  reason: test_reason\n"
                    "  watchers:\n"
                    "  - test@example.org\n"
                    "  status: Drafting\n"
                )
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 1
            assert "duplicate excluded dates" in validate_backfill_result.output

    def test_validate_backfill_invalid_excluded_dates_not_sorted(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            duplicate_excluded_dates = "  - 2021-02-04\n" "  - 2021-02-03\n"
            backfill_file.write_text(
                "2021-05-04:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  excluded_dates:\n"
                + duplicate_excluded_dates
                + "  reason: test_reason\n"
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
            assert "excluded dates not sorted" in validate_backfill_result.output

    def test_validate_backfill_entries_not_sorted(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2023-05-04:\n"
                "  start_date: 2020-01-03\n"
                "  end_date: 2020-05-03\n"
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
            assert "entries are not sorted" in validate_backfill_result.output

    def test_validate_backfill_overlap_dates(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
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
            assert "overlap dates" in validate_backfill_result.output

    def test_validate_backfill_overlap_dates_not_drafting_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Complete\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            validate_backfill_result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert validate_backfill_result.exit_code == 0

    def test_backfill_info_one_table_all_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Validating\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            os.makedirs(SQL_DIR)
            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"

            result = runner.invoke(
                create,
                [
                    qualified_table_name_2,
                    "--start_date=2021-04-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [qualified_table_name_1],
            )

            assert result.exit_code == 0
            assert qualified_table_name_1 in result.output
            assert BackfillStatus.DRAFTING.value in result.output
            assert BackfillStatus.VALIDATING.value in result.output
            assert "total of 2 backfill(s)" in result.output
            assert qualified_table_name_2 not in result.output
            assert BackfillStatus.COMPLETE.value not in result.output

    def test_backfill_info_one_table_drafting_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Validating\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [qualified_table_name, "--status=drafting"],
            )

            assert result.exit_code == 0
            assert qualified_table_name in result.output
            assert BackfillStatus.DRAFTING.value in result.output
            assert "total of 1 backfill(s)" in result.output
            assert BackfillStatus.VALIDATING.value not in result.output
            assert BackfillStatus.COMPLETE.value not in result.output

    def test_backfill_info_all_tables_all_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Validating\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            os.makedirs(SQL_DIR)
            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"

            result = runner.invoke(
                create,
                [
                    qualified_table_name_2,
                    "--start_date=2021-04-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [],
            )

            assert result.exit_code == 0
            assert qualified_table_name_1 in result.output
            assert qualified_table_name_2 in result.output
            assert BackfillStatus.DRAFTING.value in result.output
            assert BackfillStatus.VALIDATING.value in result.output
            assert "total of 3 backfill(s)" in result.output
            assert BackfillStatus.COMPLETE.value not in result.output

    def test_backfill_info_all_tables_with_validating_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Validating\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Validating\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [
                    "--status=validating",
                ],
            )

            assert result.exit_code == 0
            assert qualified_table_name_1 in result.output
            assert qualified_table_name_2 in result.output
            assert BackfillStatus.VALIDATING.value in result.output
            assert "total of 2 backfill(s)" in result.output
            assert BackfillStatus.DRAFTING.value not in result.output
            assert BackfillStatus.COMPLETE.value not in result.output

    def test_backfill_info_with_invalid_path(self, runner):
        with runner.isolated_filesystem():
            invalid_path = "moz-fx-data-shared-prod.test.test_query_v2"
            result = runner.invoke(info, [invalid_path])

            assert result.exit_code == 2
            assert "Invalid" in result.output
            assert "path" in result.output

    def test_backfill_info_with_invalid_qualified_table_name(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            invalid_qualified_table_name = "mozdata.test_query_v2"
            result = runner.invoke(info, [invalid_qualified_table_name])

            assert result.exit_code == 1
            assert "Qualified table name must be named like:" in result.output

    def test_backfill_info_one_table_invalid_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Validating\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [qualified_table_name, "--status=testing"],
            )

            assert result.exit_code == 2
            assert "Invalid value for '--status'" in result.output
