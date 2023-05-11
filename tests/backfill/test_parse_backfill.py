from datetime import date, timedelta
from pathlib import Path

import cattrs
import pytest

from bigquery_etl.backfill.parse import Backfill, BackfillStatus

TEST_DIR = Path(__file__).parent.parent

VALID_BACKFILL = Backfill(
    date(2021, 5, 3),
    date(2021, 1, 3),
    date(2021, 5, 3),
    [date(2021, 2, 3)],
    "test_reason",
    ["test@example.org"],
    "Drafting",
)


class TestParseBackfill(object):
    def test_backfill_instantiation(self):
        backfill = VALID_BACKFILL

        assert backfill.entry_date == date(2021, 5, 3)
        assert backfill.start_date == date(2021, 1, 3)
        assert backfill.end_date == date(2021, 5, 3)
        assert backfill.excluded_dates == [date(2021, 2, 3)]
        assert backfill.reason == "test_reason"
        assert backfill.watchers == ["test@example.org"]
        assert backfill.status == BackfillStatus.Drafting.value

    def test_invalid_watchers(self):
        with pytest.raises(ValueError):
            Backfill(
                VALID_BACKFILL.entry_date,
                VALID_BACKFILL.start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                ["test.org"],
                VALID_BACKFILL.status,
            )

    def test_no_watchers(self):
        with pytest.raises(ValueError):
            Backfill(
                VALID_BACKFILL.entry_date,
                VALID_BACKFILL.start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                [],
                VALID_BACKFILL.status,
            )

    def test_multiple_watchers(self):
        backfill = Backfill(
            VALID_BACKFILL.entry_date,
            VALID_BACKFILL.start_date,
            VALID_BACKFILL.end_date,
            VALID_BACKFILL.excluded_dates,
            VALID_BACKFILL.reason,
            VALID_BACKFILL.watchers + ["test2@example.org", "test3@example.org"],
            VALID_BACKFILL.status,
        )

        assert backfill.watchers == VALID_BACKFILL.watchers + [
            "test2@example.org",
            "test3@example.org",
        ]

    def test_all_status(self):
        valid_status = ["Drafting", "Validating", "Complete"]
        for i, status in enumerate(BackfillStatus):
            backfill = Backfill(
                VALID_BACKFILL.entry_date,
                VALID_BACKFILL.start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                VALID_BACKFILL.watchers,
                status.value,
            )

            assert backfill.status == valid_status[i]

    def test_invalid_entry_date_greater_than_today(self):
        with pytest.raises(ValueError):
            invalid_entry_date = date.today() + timedelta(days=1)
            Backfill(
                invalid_entry_date,
                VALID_BACKFILL.start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                VALID_BACKFILL.watchers,
                VALID_BACKFILL.status,
            )

    def test_invalid_start_date_greater_than_today(self):
        with pytest.raises(ValueError):
            invalid_start_date = date.today() + timedelta(days=1)
            Backfill(
                VALID_BACKFILL.entry_date,
                invalid_start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                VALID_BACKFILL.watchers,
                VALID_BACKFILL.status,
            )

    def test_invalid_start_date_greater_than_end_date(self):
        with pytest.raises(ValueError):
            invalid_start_date = date(2023, 5, 4)
            Backfill(
                VALID_BACKFILL.entry_date,
                invalid_start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                VALID_BACKFILL.watchers,
                VALID_BACKFILL.status,
            )

    def test_invalid_end_date_greater_than_today(self):
        with pytest.raises(ValueError):
            invalid_end_date = date.today() + timedelta(days=1)
            Backfill(
                VALID_BACKFILL.entry_date,
                VALID_BACKFILL.start_date,
                invalid_end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                VALID_BACKFILL.watchers,
                VALID_BACKFILL.status,
            )

    # TODO: Add tests for excluded_dates

    # def test_invalid_excluded_date_greater_than_end_date_one(self):

    # def test_invalid_excluded_date_greater_than_end_date_multiple(self):

    # def test_invalid_excluded_date_less_than_start_date_one(self):

    # def test_invalid_excluded_date_less_than_start_date_multiple(self):

    def test_no_reason(self):
        with pytest.raises(ValueError):
            invalid_reason = ""
            Backfill(
                VALID_BACKFILL.entry_date,
                VALID_BACKFILL.start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                invalid_reason,
                VALID_BACKFILL.watchers,
                VALID_BACKFILL.status,
            )

    def test_invalid_status(self):
        with pytest.raises(ValueError):
            invalid_status = "invalid_status"
            Backfill(
                VALID_BACKFILL.entry_date,
                VALID_BACKFILL.start_date,
                VALID_BACKFILL.end_date,
                VALID_BACKFILL.excluded_dates,
                VALID_BACKFILL.reason,
                VALID_BACKFILL.watchers,
                invalid_status,
            )

    def test_from_backfill_file_one(self):
        backfill_file = TEST_DIR / "backfill" / "backfill.yaml"
        backfills = Backfill.from_backfill_file(backfill_file)
        backfill = backfills[VALID_BACKFILL.entry_date]

        assert backfill.entry_date == VALID_BACKFILL.entry_date
        assert backfill.start_date == VALID_BACKFILL.start_date
        assert backfill.end_date == VALID_BACKFILL.end_date
        assert backfill.excluded_dates == VALID_BACKFILL.excluded_dates
        assert backfill.reason == VALID_BACKFILL.reason
        assert backfill.watchers == VALID_BACKFILL.watchers
        assert backfill.status == VALID_BACKFILL.status

    def test_non_existing_file(self):
        backfill_file = TEST_DIR / "nonexisting_dir" / "backfill.yaml"
        with pytest.raises(FileNotFoundError):
            Backfill.from_backfill_file(backfill_file)

    def test_of_backfill_file_no_backfill(self):
        backfill_file = TEST_DIR / "test" / "backfill.yaml"
        with pytest.raises(FileNotFoundError):
            Backfill.from_backfill_file(backfill_file)

    def test_of_backfill_file_one(self):
        backfill_file = TEST_DIR / "backfill" / "backfill.yaml"

        backfills = Backfill.from_backfill_file(backfill_file)
        backfill = backfills[date(2021, 5, 3)]

        assert backfill.entry_date == date(2021, 5, 3)
        assert backfill.start_date == date(2021, 1, 3)
        assert backfill.end_date == date(2021, 5, 3)
        assert backfill.excluded_dates == [date(2021, 2, 3)]
        assert backfill.reason == "test_reason"
        assert backfill.watchers == ["test@example.org"]
        assert backfill.status == "Drafting"

    def test_from_backfill_file_multiple(self):
        backfill_file = TEST_DIR / "backfill" / "test_dir" / "backfill.yaml"
        backfills = Backfill.from_backfill_file(backfill_file)

        backfill_1 = VALID_BACKFILL

        backfill_2 = Backfill(
            date(2023, 5, 3),
            date(2021, 3, 1),
            date(2023, 5, 3),
            [date(2021, 3, 6), date(2021, 3, 9)],
            "test_reason",
            ["example@mozilla.com", "test@example.org"],
            "Validating",
        )

        assert backfills[date(2021, 5, 3)] == backfill_1
        assert backfills[date(2023, 5, 3)] == backfill_2

    def test_invalid_file(self):
        backfill_file = TEST_DIR / "test" / "invalid_file_name.yaml"

        with pytest.raises(ValueError):
            Backfill.from_backfill_file(backfill_file)

    def test_of_non_existing_table(self):
        with pytest.raises(FileNotFoundError):
            backfill_file = TEST_DIR / "non_exist_folder" / "backfill.yaml"

            Backfill.from_backfill_file(backfill_file)

    def test_is_backfill_file(self):
        assert Backfill.is_backfill_file("foo/bar/invalid.json") is False
        assert Backfill.is_backfill_file("foo/bar/invalid.yaml") is False
        assert Backfill.is_backfill_file("backfill.yaml")
        assert Backfill.is_backfill_file("some/path/to/backfill.yaml")

    def test_clean(self):

        converter = cattrs.BaseConverter()
        entry = converter.unstructure(VALID_BACKFILL)
        backfill = Backfill.clean(entry)

        assert "entry_date" not in backfill
        assert backfill["start_date"] == date(2021, 1, 3)
        assert backfill["end_date"] == date(2021, 5, 3)
        assert backfill["excluded_dates"] == [date(2021, 2, 3)]
        assert backfill["reason"] == "test_reason"
        assert backfill["watchers"] == ["test@example.org"]
        assert backfill["status"] == "Drafting"
