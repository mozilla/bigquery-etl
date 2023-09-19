from datetime import date
from pathlib import Path

import pytest

from bigquery_etl.backfill.parse import BACKFILL_FILE, DEFAULT_REASON, Backfill
from bigquery_etl.backfill.validate import (
    validate_duplicate_entry_dates,
    validate_entries,
    validate_entries_are_sorted,
    validate_excluded_dates,
    validate_file,
    validate_overlap_dates,
    validate_reason,
)
from tests.backfill.test_parse_backfill import TEST_BACKFILL_1, TEST_BACKFILL_2

TEST_DIR = Path(__file__).parent.parent

VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"


class TestValidateBackfill(object):
    def test_duplicate_entry_dates_pass(self):
        validate_duplicate_entry_dates(TEST_BACKFILL_1, TEST_BACKFILL_2)

    def test_duplicate_entry_dates_fail(self):
        with pytest.raises(ValueError) as e:
            validate_duplicate_entry_dates(TEST_BACKFILL_1, TEST_BACKFILL_1)

        assert "Duplicate backfill" in str(e.value)

    def test_overlap_dates_pass(self):
        validate_overlap_dates(TEST_BACKFILL_1, TEST_BACKFILL_2)

    def test_overlap_dates_fail(self):
        with pytest.raises(ValueError) as e:
            validate_overlap_dates(TEST_BACKFILL_1, TEST_BACKFILL_1)

        assert "overlap dates" in str(e.value)

    def test_excluded_dates_duplicates(self):
        invalid_excluded_dates = [date(2021, 2, 3), date(2021, 2, 3)]
        invalid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            invalid_excluded_dates,
            TEST_BACKFILL_1.reason,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        with pytest.raises(ValueError) as e:
            validate_excluded_dates(invalid_backfill)

        assert "duplicate excluded dates" in str(e.value)

    def test_excluded_dates_not_sorted(self):
        invalid_excluded_dates = [date(2021, 2, 4), date(2021, 2, 3)]
        invalid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            invalid_excluded_dates,
            TEST_BACKFILL_1.reason,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        with pytest.raises(ValueError) as e:
            validate_excluded_dates(invalid_backfill)

        assert "excluded dates not sorted" in str(e.value)

    def test_valid_reason_pass(self):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        validate_reason(valid_backfill)

    def test_reason_default_fail(self):
        invalid_reason = DEFAULT_REASON
        invalid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            invalid_reason,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        with pytest.raises(ValueError) as e:
            validate_reason(invalid_backfill)

        assert "Invalid Reason" in str(e.value)

    def test_reason_empty_fail(self):
        invalid_reason = ""
        invalid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            invalid_reason,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )
        with pytest.raises(ValueError) as e:
            validate_reason(invalid_backfill)

        assert "Invalid Reason" in str(e.value)

    def test_entries_sorted(self):
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        validate_entries_are_sorted(backfills)

    def test_entries_not_sorted(self):
        backfills = [TEST_BACKFILL_1, TEST_BACKFILL_2]
        with pytest.raises(ValueError) as e:
            validate_entries_are_sorted(backfills)

        assert "Backfill entries are not sorted" in str(e.value)

    def test_validate_entries_pass(self):
        TEST_BACKFILL_1.watchers = [VALID_WATCHER]
        TEST_BACKFILL_1.reason = VALID_REASON
        TEST_BACKFILL_2.watchers = [VALID_WATCHER]
        TEST_BACKFILL_2.reason = VALID_REASON
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        validate_entries(backfills)

    def test_validate_file(self):
        backfill_file = TEST_DIR / "backfill" / "test_dir_valid" / BACKFILL_FILE
        validate_file(backfill_file)
