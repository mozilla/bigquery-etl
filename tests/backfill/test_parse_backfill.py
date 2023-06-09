from datetime import date, timedelta
from pathlib import Path

import pytest

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)

DEFAULT_STATUS = BackfillStatus.DRAFTING

TEST_DIR = Path(__file__).parent.parent

TEST_BACKFILL_1 = Backfill(
    date(2021, 5, 3),
    date(2021, 1, 3),
    date(2021, 5, 3),
    [date(2021, 2, 3)],
    DEFAULT_REASON,
    [DEFAULT_WATCHER],
    DEFAULT_STATUS,
)

TEST_BACKFILL_2 = Backfill(
    date(2023, 5, 3),
    date(2023, 1, 3),
    date(2023, 5, 3),
    [date(2023, 2, 3)],
    DEFAULT_REASON,
    [DEFAULT_WATCHER],
    DEFAULT_STATUS,
)


class TestParseBackfill(object):
    def test_backfill_instantiation(self):
        backfill = TEST_BACKFILL_1

        assert backfill.entry_date == date(2021, 5, 3)
        assert backfill.start_date == date(2021, 1, 3)
        assert backfill.end_date == date(2021, 5, 3)
        assert backfill.excluded_dates == [date(2021, 2, 3)]
        assert backfill.reason == DEFAULT_REASON
        assert backfill.watchers == [DEFAULT_WATCHER]
        assert backfill.status == DEFAULT_STATUS

    def test_invalid_watcher(self):
        with pytest.raises(ValueError) as e:
            invalid_watcher = ["test.org"]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                invalid_watcher,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid" in str(e.value)
        assert "watchers" in str(e.value)

    def test_invalid_watchers(self):
        with pytest.raises(ValueError) as e:
            invalid_watchers = [DEFAULT_WATCHER, "test.org"]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                invalid_watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid" in str(e.value)
        assert "watchers" in str(e.value)

    def test_no_watchers(self):
        with pytest.raises(ValueError) as e:
            invalid_watchers = [""]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                invalid_watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid" in str(e.value)
        assert "watchers" in str(e.value)

    def test_multiple_watchers(self):
        valid_watchers = TEST_BACKFILL_1.watchers + [
            "test2@example.org",
            "test3@example.org",
        ]
        backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            TEST_BACKFILL_1.reason,
            valid_watchers,
            TEST_BACKFILL_1.status,
        )

        assert backfill.watchers == TEST_BACKFILL_1.watchers + [
            "test2@example.org",
            "test3@example.org",
        ]

    def test_all_status(self):
        valid_status = [status.value for status in BackfillStatus]
        for i, status in enumerate(BackfillStatus):
            backfill = Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                status,
            )

            assert backfill.status.value == valid_status[i]

    def test_invalid_entry_date_greater_than_today(self):
        with pytest.raises(ValueError) as e:
            invalid_entry_date = date.today() + timedelta(days=1)
            Backfill(
                invalid_entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "can't be in the future" in str(e.value)

    def test_invalid_start_date_greater_than_entry_date(self):
        with pytest.raises(ValueError) as e:
            invalid_start_date = TEST_BACKFILL_1.entry_date + timedelta(days=1)
            Backfill(
                TEST_BACKFILL_1.entry_date,
                invalid_start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid start date" in str(e.value)

    def test_invalid_start_date_greater_than_end_date(self):
        with pytest.raises(ValueError) as e:
            invalid_start_date = TEST_BACKFILL_1.end_date + timedelta(days=1)
            Backfill(
                TEST_BACKFILL_1.entry_date,
                invalid_start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid start date" in str(e.value)

    def test_invalid_end_date_greater_than_entry_date(self):
        with pytest.raises(ValueError) as e:
            invalid_end_date = TEST_BACKFILL_1.entry_date + timedelta(days=1)
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                invalid_end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid end date" in str(e.value)

    def test_invalid_excluded_dates_greater_than_end_date(self):
        with pytest.raises(ValueError) as e:
            invalid_excluded_dates = [TEST_BACKFILL_1.end_date + timedelta(days=1)]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                invalid_excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid excluded dates" in str(e.value)

    def test_invalid_excluded_dates_greater_than_end_date_multiple(self):
        with pytest.raises(ValueError) as e:
            invalid_excluded_dates = [
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.end_date + timedelta(days=1),
            ]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                invalid_excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid excluded dates" in str(e.value)

    def test_invalid_excluded_dates_less_than_start_date(self):
        with pytest.raises(ValueError) as e:
            invalid_excluded_dates = [TEST_BACKFILL_1.start_date - timedelta(days=1)]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                invalid_excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid excluded dates" in str(e.value)

    def test_invalid_excluded_dates_less_than_start_date_multiple(self):
        with pytest.raises(ValueError) as e:
            invalid_excluded_dates = [
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.start_date - timedelta(days=1),
            ]
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                invalid_excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

        assert "Invalid excluded dates" in str(e.value)

    def test_invalid_status(self):
        with pytest.raises(AttributeError):
            invalid_status = "invalid_status"
            Backfill(
                TEST_BACKFILL_1.entry_date,
                TEST_BACKFILL_1.start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                invalid_status,
            )

    def test_non_existing_file(self):
        backfill_file = TEST_DIR / "nonexisting_dir" / BACKFILL_FILE
        with pytest.raises(FileNotFoundError):
            Backfill.entries_from_file(backfill_file)

    def test_of_backfill_file_no_backfill(self):
        backfill_file = TEST_DIR / "test" / BACKFILL_FILE
        with pytest.raises(FileNotFoundError):
            Backfill.entries_from_file(backfill_file)

    def test_of_backfill_file_one(self):
        backfill_file = TEST_DIR / "backfill" / BACKFILL_FILE
        backfills = Backfill.entries_from_file(backfill_file)
        backfill = backfills[0]

        assert backfill.entry_date == date(2021, 5, 3)
        assert backfill.start_date == date(2021, 1, 3)
        assert backfill.end_date == date(2021, 5, 3)
        assert backfill.excluded_dates == [date(2021, 2, 3)]
        assert backfill.reason == DEFAULT_REASON
        assert backfill.watchers == [DEFAULT_WATCHER]
        assert backfill.status == DEFAULT_STATUS

    def test_entries_from_file_multiple(self):
        backfill_file = TEST_DIR / "backfill" / "test_dir_multiple" / BACKFILL_FILE
        backfills = Backfill.entries_from_file(backfill_file)

        backfill_1 = TEST_BACKFILL_1
        backfill_2 = TEST_BACKFILL_2

        assert backfills[0] == backfill_2
        assert backfills[1] == backfill_1

    def test_invalid_file(self):
        backfill_file = TEST_DIR / "test" / "invalid_file_name.yaml"
        with pytest.raises(ValueError) as e:
            Backfill.entries_from_file(backfill_file)

        assert "Invalid file" in str(e.value)

    def test_of_non_existing_table(self):
        backfill_file = TEST_DIR / "non_exist_folder" / BACKFILL_FILE
        with pytest.raises(FileNotFoundError):
            Backfill.entries_from_file(backfill_file)

    def test_is_backfill_file(self):
        assert Backfill.is_backfill_file("foo/bar/invalid.json") is False
        assert Backfill.is_backfill_file("foo/bar/invalid.yaml") is False
        assert Backfill.is_backfill_file(BACKFILL_FILE)
        assert Backfill.is_backfill_file("some/path/to/" + BACKFILL_FILE)

    def test_to_yaml(self):
        expected = (
            "2021-05-03:\n"
            "  start_date: 2021-01-03\n"
            "  end_date: 2021-05-03\n"
            "  excluded_dates:\n"
            "  - 2021-02-03\n"
            "  reason: Please provide a reason for the backfill and links to any related "
            "bugzilla\n"
            "    or jira tickets\n"
            "  watchers:\n"
            "  - nobody@mozilla.com\n"
            "  status: Drafting\n"
        )

        results = TEST_BACKFILL_1.to_yaml()
        assert results == expected

    def test_backfill_str(self):
        backfill = TEST_BACKFILL_1
        actual_backfill_str = str(backfill)
        expected_backfill_str = """
            entry_date = 2021-05-03
            start_date = 2021-01-03
            end_date = 2021-05-03
            excluded_dates = [2021-02-03]
            reason = Please provide a reason for the backfill and links to any related bugzilla or jira tickets
            watcher(s) = [nobody@mozilla.com]
            status = Drafting
            """

        assert actual_backfill_str == expected_backfill_str

    def test_to_yaml_no_excluded_dates(self):
        expected = (
            "2021-05-03:\n"
            "  start_date: 2021-01-03\n"
            "  end_date: 2021-05-03\n"
            "  reason: Please provide a reason for the backfill and links to any related "
            "bugzilla\n"
            "    or jira tickets\n"
            "  watchers:\n"
            "  - nobody@mozilla.com\n"
            "  status: Drafting\n"
        )

        TEST_BACKFILL_1.excluded_dates = []
        results = TEST_BACKFILL_1.to_yaml()

        assert results == expected
