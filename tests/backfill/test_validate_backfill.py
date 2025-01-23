from pathlib import Path

import pytest

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from bigquery_etl.backfill.validate import (
    validate_default_reason,
    validate_default_watchers,
    validate_duplicate_entry_dates,
    validate_entries,
    validate_entries_are_sorted,
    validate_file,
)
from tests.backfill.test_parse_backfill import TEST_BACKFILL_1, TEST_BACKFILL_2

TEST_DIR = Path(__file__).parent.parent

VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"


class TestValidateBackfill(object):

    def test_entries_duplicate_entry_dates_should_fail(self):
        backfills = [TEST_BACKFILL_1, TEST_BACKFILL_1]
        with pytest.raises(ValueError) as e:
            validate_duplicate_entry_dates(TEST_BACKFILL_1, backfills)

        assert "Duplicate backfill with entry date" in str(e.value)

    def test_valid_reason_should_pass(self):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        validate_default_reason(valid_backfill)

    def test_validate_default_reason_should_fail(self):
        invalid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            DEFAULT_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        with pytest.raises(ValueError) as e:
            validate_default_reason(invalid_backfill)

        assert "Default reason" in str(e.value)

    def test_validate_default_watcher_should_fail(self):
        TEST_BACKFILL_1.watchers = [DEFAULT_WATCHER]

        with pytest.raises(ValueError) as e:
            validate_default_watchers(TEST_BACKFILL_1)

        assert "Default watcher" in str(e.value)

    def test_entries_sorted(self):
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        validate_entries_are_sorted(backfills)

    def test_entries_not_sorted(self):
        backfills = [TEST_BACKFILL_1, TEST_BACKFILL_2]
        with pytest.raises(ValueError) as e:
            validate_entries_are_sorted(backfills)

        assert "Backfill entries are not sorted" in str(e.value)

    def test_validate_entries_duplicate_entry_with_initiate_status_should_fail(self):
        TEST_BACKFILL_1.watchers = [VALID_WATCHER]
        TEST_BACKFILL_1.reason = VALID_REASON
        TEST_BACKFILL_2.watchers = [VALID_WATCHER]
        TEST_BACKFILL_2.reason = VALID_REASON
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        with pytest.raises(ValueError) as e:
            validate_entries(backfills)
        assert (
            "Backfill entries cannot contain more than one entry with Initiate status"
            in str(e.value)
        )

    def test_validate_entries(self):
        TEST_BACKFILL_1.watchers = [VALID_WATCHER]
        TEST_BACKFILL_1.reason = VALID_REASON
        TEST_BACKFILL_2.watchers = [VALID_WATCHER]
        TEST_BACKFILL_2.reason = VALID_REASON
        TEST_BACKFILL_2.status = BackfillStatus.COMPLETE.value
        backfills = [TEST_BACKFILL_2, TEST_BACKFILL_1]
        validate_entries(backfills)

    def test_validate_file(self):
        backfill_file = TEST_DIR / "backfill" / "test_dir_valid" / BACKFILL_FILE
        validate_file(backfill_file)
