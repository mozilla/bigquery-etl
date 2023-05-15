from datetime import timedelta

import pytest

from bigquery_etl.backfill.parse import DEFAULT_REASON, Backfill, BackfillStatus
from bigquery_etl.backfill.validate import (
    validate_all_entries,
    validate_entries_are_sorted,
    validate_overlap_dates,
    validate_reason,
)
from tests.backfill.test_parse_backfill import VALID_BACKFILL_1, VALID_BACKFILL_2


class TestValidateBackfill(object):
    def test_overlap_dates_pass(self):
        entry_1 = VALID_BACKFILL_1
        entry_2 = VALID_BACKFILL_2
        validate_overlap_dates(entry_1, entry_2)

    def test_overlap_dates_fail(self):
        with pytest.raises(ValueError):
            entry_1 = VALID_BACKFILL_1
            entry_2 = VALID_BACKFILL_1
            validate_overlap_dates(entry_1, entry_2)

    def test_reason_default_fail(self):
        with pytest.raises(ValueError):
            invalid_reason = DEFAULT_REASON
            invalid_backfill = Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                invalid_reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

            validate_reason(invalid_backfill)

    def test_reason_empty_fail(self):
        with pytest.raises(ValueError):
            invalid_reason = ""
            invalid_backfill = Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                invalid_reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

            validate_reason(invalid_backfill)

    def test_entries_sorted(self):
        backfills = [VALID_BACKFILL_2, VALID_BACKFILL_1]
        validate_entries_are_sorted(backfills)

    def test_entries_not_sorted(self):
        with pytest.raises(ValueError):
            backfills = [VALID_BACKFILL_1, VALID_BACKFILL_2]
            validate_entries_are_sorted(backfills)

    def test_validate_all_entries_pass(self):
        backfills = [VALID_BACKFILL_2, VALID_BACKFILL_1]
        validate_all_entries(backfills)

    def test_validate_all_entries_reason_default_fail(self):
        with pytest.raises(ValueError):
            invalid_reason = DEFAULT_REASON
            invalid_backfill = Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                invalid_reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

            backfills = [VALID_BACKFILL_2, invalid_backfill]
            validate_all_entries(backfills)

    def test_validate_all_entries_reason_empty_fail(self):
        with pytest.raises(ValueError):
            invalid_reason = ""
            invalid_backfill = Backfill(
                VALID_BACKFILL_1.entry_date,
                VALID_BACKFILL_1.start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                invalid_reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

            backfills = [VALID_BACKFILL_2, invalid_backfill]
            validate_all_entries(backfills)

    def test_validate_all_entries_overlap_dates_fail(self):
        with pytest.raises(ValueError):
            invalid_start_date = VALID_BACKFILL_2.start_date
            invalid_backfill = Backfill(
                VALID_BACKFILL_1.entry_date,
                invalid_start_date,
                VALID_BACKFILL_1.end_date,
                VALID_BACKFILL_1.excluded_dates,
                VALID_BACKFILL_1.reason,
                VALID_BACKFILL_1.watchers,
                VALID_BACKFILL_1.status,
            )

            backfills = [VALID_BACKFILL_2, invalid_backfill]
            validate_all_entries(backfills)

    def test_validate_all_entries_overlap_dates_status_pass(self):
        valid_status = BackfillStatus.VALIDATING
        valid_backfill = Backfill(
            VALID_BACKFILL_1.entry_date + timedelta(days=1),
            VALID_BACKFILL_1.start_date,
            VALID_BACKFILL_1.end_date,
            VALID_BACKFILL_1.excluded_dates,
            VALID_BACKFILL_1.reason,
            VALID_BACKFILL_1.watchers,
            valid_status,
        )

        backfills = [valid_backfill, VALID_BACKFILL_1]
        validate_all_entries(backfills)

    def test_validate_all_entries_sorted_fail(self):
        with pytest.raises(ValueError):
            backfills = [VALID_BACKFILL_1, VALID_BACKFILL_2]
            validate_all_entries(backfills)

    def test_validate_all_entries_duplicate_entry_dates(self):
        with pytest.raises(ValueError):
            backfills = [VALID_BACKFILL_1, VALID_BACKFILL_1]
            validate_all_entries(backfills)
