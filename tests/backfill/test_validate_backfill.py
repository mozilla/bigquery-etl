from datetime import timedelta

import pytest

from bigquery_etl.backfill.parse import DEFAULT_REASON, Backfill, BackfillStatus
from bigquery_etl.backfill.validate import (
    validate_entries,
    validate_entries_are_sorted,
    validate_overlap_dates,
    validate_reason,
)
from tests.backfill.test_parse_backfill import TEST_BACKFILL_1, TEST_BACKFILL_2

VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"


# TODO: Add assert statements for errors
class TestValidateBackfill(object):
    def test_overlap_dates_pass(self):
        entry_1 = TEST_BACKFILL_1
        entry_2 = TEST_BACKFILL_2
        validate_overlap_dates(entry_1, entry_2)

    def test_overlap_dates_fail(self):
        with pytest.raises(ValueError):
            entry_1 = TEST_BACKFILL_1
            entry_2 = TEST_BACKFILL_1
            validate_overlap_dates(entry_1, entry_2)

    def test_reason_default_fail(self):
        with pytest.raises(ValueError):
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

            validate_reason(invalid_backfill)

    def test_reason_empty_fail(self):
        with pytest.raises(ValueError):
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

            validate_reason(invalid_backfill)

    def test_entries_sorted(self):
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        validate_entries_are_sorted(backfills)

    def test_entries_not_sorted(self):
        with pytest.raises(ValueError):
            backfills = [TEST_BACKFILL_1, TEST_BACKFILL_2]
            validate_entries_are_sorted(backfills)

    def test_validate_entries_pass(self):
        TEST_BACKFILL_1.watchers = ["nobody@mozilla.com"]
        TEST_BACKFILL_1.reason = ["no_reason"]
        TEST_BACKFILL_2.watchers = ["nobody@mozilla.com"]
        TEST_BACKFILL_2.reason = ["no_reason"]
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        validate_entries(backfills)

    def test_validate_entries_reason_default_fail(self):
        with pytest.raises(ValueError):
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

            backfills = [TEST_BACKFILL_2, invalid_backfill]
            validate_entries(backfills)

    def test_validate_entries_reason_empty_fail(self):
        with pytest.raises(ValueError):
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

            backfills = [TEST_BACKFILL_2, invalid_backfill]
            validate_entries(backfills)

    def test_validate_entries_overlap_dates_fail(self):
        with pytest.raises(ValueError):
            invalid_start_date = TEST_BACKFILL_2.start_date
            invalid_backfill = Backfill(
                TEST_BACKFILL_1.entry_date,
                invalid_start_date,
                TEST_BACKFILL_1.end_date,
                TEST_BACKFILL_1.excluded_dates,
                TEST_BACKFILL_1.reason,
                TEST_BACKFILL_1.watchers,
                TEST_BACKFILL_1.status,
            )

            backfills = [TEST_BACKFILL_2, invalid_backfill]
            validate_entries(backfills)

    def test_validate_entries_overlap_dates_status_pass(self):
        valid_status = BackfillStatus.VALIDATING
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date + timedelta(days=1),
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            TEST_BACKFILL_1.reason,
            TEST_BACKFILL_1.watchers,
            valid_status,
        )

        backfills = [valid_backfill, TEST_BACKFILL_1]
        validate_entries(backfills)

    def test_validate_entries_sorted_fail(self):
        with pytest.raises(ValueError):
            backfills = [TEST_BACKFILL_1, TEST_BACKFILL_2]
            validate_entries(backfills)

    def test_validate_entries_duplicate_entry_dates(self):
        with pytest.raises(ValueError) as e:
            TEST_BACKFILL_1.reason = VALID_REASON
            TEST_BACKFILL_1.watchers = [VALID_WATCHER]
            backfills = [TEST_BACKFILL_1, TEST_BACKFILL_1]
            validate_entries(backfills)

        assert "Duplicate backfill" in str(e.value)
