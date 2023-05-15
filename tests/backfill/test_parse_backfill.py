from datetime import date, timedelta
from pathlib import Path

import pytest

from bigquery_etl.backfill.parse import Backfill, BackfillStatus

TEST_DIR = Path(__file__).parent.parent

VALID_BACKFILL_1 = Backfill(
    date(2021, 5, 3),
    date(2021, 1, 3),
    date(2021, 5, 3),
    [date(2021, 2, 3)],
    "test_reason",
    ["test@example.org"],
    BackfillStatus.DRAFTING,
)

VALID_BACKFILL_2 = Backfill(
    date(2023, 5, 3),
    date(2023, 1, 3),
    date(2023, 5, 3),
    [date(2023, 2, 3)],
    "test_reason",
    ["test@example.org"],
    BackfillStatus.DRAFTING,
)


class TestParseBackfill(object):
    def test_backfill_instantiation(self):
        backfill = VALID_BACKFILL_1

        assert backfill.entry_date == date(2021, 5, 3)
        assert backfill.start_date == date(2021, 1, 3)
        assert backfill.end_date == date(2021, 5, 3)
        assert backfill.excluded_dates == [date(2021, 2, 3)]
        assert backfill.reason == "test_reason"
        assert backfill.watchers == ["test@example.org"]
        assert backfill.status == BackfillStatus.DRAFTING

    def test_invalid_watchers(self):
        with pytest.raises(ValueError):
            invalid_watchers = ["test.org"]
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                invalid_watchers,
                VALID_BACKFILL_1.status,
            )

    def test_no_watchers(self):
        with pytest.raises(ValueError):
            invalid_watchers = []
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                invalid_watchers,
                VALID_BACKFILL_1.status,
            )

    def test_multiple_watchers(self):
        valid_watchers = VALID_BACKFILL_1.watchers + [
            "test2@example.org",
            "test3@example.org",
        ]
        backfill = Backfill(
            VALID_BACKFILL_1.entry_date,
            VALID_BACKFILL_1.start_date,
            VALID_BACKFILL_1.end_date,
            VALID_BACKFILL_1.excluded_dates,
            VALID_BACKFILL_1.reason,
            valid_watchers,
            VALID_BACKFILL_1.status,
        )

        assert backfill.watchers == VALID_BACKFILL_1.watchers + [
            "test2@example.org",
            "test3@example.org",
        ]

    def test_all_status(self):
        valid_status = ["Drafting", "Validating", "Complete"]
        for i, status in enumerate(BackfillStatus):
            backfill = Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                status,
            )

            assert backfill.status.value == valid_status[i]

    def test_invalid_entry_date_greater_than_today(self):
        with pytest.raises(ValueError):
            invalid_entry_date = date.today() + timedelta(days=1)
            Backfill(
                invalid_entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_start_date_greater_than_today(self):
        with pytest.raises(ValueError):
            invalid_start_date = date.today() + timedelta(days=1)
            Backfill(
                VALID_BACKFILL_1.entry_date,
                invalid_start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_start_date_greater_than_end_date(self):
        with pytest.raises(ValueError):
            invalid_start_date = date(2023, 5, 4)
            Backfill(
                VALID_BACKFILL_1.entry_date,
                invalid_start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_end_date_greater_than_today(self):
        with pytest.raises(ValueError):
            invalid_end_date = date.today() + timedelta(days=1)
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                invalid_end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    # TODO: Add tests for excluded_dates

    def test_invalid_excluded_date_greater_than_end_date_one(self):
        with pytest.raises(ValueError):
            invalid_excluded_date = [VALID_BACKFILL_1.end_date + timedelta(days=1)]
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                invalid_excluded_date,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_excluded_date_greater_than_end_date_multiple(self):
        with pytest.raises(ValueError):
            invalid_excluded_date = [
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.end_date + timedelta(days=1),
            ]
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                invalid_excluded_date,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_excluded_date_less_than_start_date_one(self):
        with pytest.raises(ValueError):
            invalid_excluded_date = [VALID_BACKFILL_1.start_date - timedelta(days=1)]
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                invalid_excluded_date,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_excluded_date_less_than_start_date_multiple(self):
        with pytest.raises(ValueError):
            invalid_excluded_date = [
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.start_date - timedelta(days=1),
            ]
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                invalid_excluded_date,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_no_reason(self):
        with pytest.raises(ValueError):
            invalid_reason = ""
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                invalid_reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

    def test_invalid_status(self):
        with pytest.raises(AttributeError):
            invalid_status = "invalid_status"
            Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                invalid_status,
            )

    def test_non_existing_file(self):
        backfill_file = TEST_DIR / "nonexisting_dir" / "backfill.yaml"
        with pytest.raises(FileNotFoundError):
            Backfill.entries_from_file(backfill_file)

    def test_of_backfill_file_no_backfill(self):
        backfill_file = TEST_DIR / "test" / "backfill.yaml"
        with pytest.raises(FileNotFoundError):
            Backfill.entries_from_file(backfill_file)

    def test_of_backfill_file_one(self):
        backfill_file = TEST_DIR / "backfill" / "backfill.yaml"

        backfills = Backfill.entries_from_file(backfill_file)
        backfill = backfills[0]

        assert backfill.entry_date == date(2021, 5, 3)
        assert backfill.start_date == date(2021, 1, 3)
        assert backfill.end_date == date(2021, 5, 3)
        assert backfill.excluded_dates == [date(2021, 2, 3)]
        assert backfill.reason == "test_reason"
        assert backfill.watchers == ["test@example.org"]
        assert backfill.status == BackfillStatus.DRAFTING

    def test_entries_from_file_multiple(self):
        backfill_file = TEST_DIR / "backfill" / "test_dir" / "backfill.yaml"
        backfills = Backfill.entries_from_file(backfill_file)

        backfill_1 = VALID_BACKFILL_1
        backfill_2 = VALID_BACKFILL_2

        assert backfills[0] == backfill_2
        assert backfills[1] == backfill_1

    def test_invalid_file(self):
        backfill_file = TEST_DIR / "test" / "invalid_file_name.yaml"

        with pytest.raises(ValueError):
            Backfill.entries_from_file(backfill_file)

    def test_of_non_existing_table(self):
        with pytest.raises(FileNotFoundError):
            backfill_file = TEST_DIR / "non_exist_folder" / "backfill.yaml"

            Backfill.entries_from_file(backfill_file)

    def test_is_backfill_file(self):
        assert Backfill.is_backfill_file("foo/bar/invalid.json") is False
        assert Backfill.is_backfill_file("foo/bar/invalid.yaml") is False
        assert Backfill.is_backfill_file("backfill.yaml")
        assert Backfill.is_backfill_file("some/path/to/backfill.yaml")

    def test_to_yaml(self):
        expected = (
            "2021-05-03:\n"
            "  start_date: 2021-01-03\n"
            "  end_date: 2021-05-03\n"
            "  excluded_dates:\n"
            "  - 2021-02-03\n"
            "  reason: test_reason\n"
            "  watchers:\n"
            "  - test@example.org\n"
            "  status: Drafting\n"
        )

        results = VALID_BACKFILL_1.to_yaml()

        print(results)

        assert results == expected

    def test_to_yaml_no_excluded_dates(self):
        expected = (
            "2021-05-03:\n"
            "  start_date: 2021-01-03\n"
            "  end_date: 2021-05-03\n"
            "  reason: test_reason\n"
            "  watchers:\n"
            "  - test@example.org\n"
            "  status: Drafting\n"
        )

        VALID_BACKFILL_1.excluded_dates = []

        results = VALID_BACKFILL_1.to_yaml()

        assert results == expected
