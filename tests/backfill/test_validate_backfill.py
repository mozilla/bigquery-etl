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
    validate_shredder_mitigation,
)
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata
from bigquery_etl.metadata.validate_metadata import SHREDDER_MITIGATION_LABEL
from tests.backfill.test_parse_backfill import TEST_BACKFILL_1, TEST_BACKFILL_2

TEST_DIR = Path(__file__).parent.parent

VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"
TEST_BACKFILL_FILE = TEST_DIR / "backfill" / "test_dir_valid" / BACKFILL_FILE
TEST_BACKFILL_FILE_1 = TEST_DIR / "backfill" / "test_dir_valid_1" / BACKFILL_FILE


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
            validate_entries(backfills, TEST_BACKFILL_FILE)
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
        validate_entries(backfills, TEST_BACKFILL_FILE)

    def test_validate_file(self):
        validate_file(TEST_BACKFILL_FILE)

    def test_validate_backfill_initiate_status_with_shredder_mitigation_true_and_metadata_label_false_should_fail(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
            shredder_mitigation=True,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == False

        with pytest.raises(ValueError) as e:
            validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE)

        assert (
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} should match."
            in str(e.value)
        )

    def test_validate_backfill_initiate_shredder_mitigation_false_and_metadata_label_true_should_fail(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
            shredder_mitigation=False,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE_1).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == True

        with pytest.raises(ValueError) as e:
            validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

        assert (
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} should match."
            in str(e.value)
        )

    def test_validate_backfill_initiate_without_shredder_mitigation_and_metadata_label_true_should_fail(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE_1).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == True

        with pytest.raises(ValueError) as e:
            validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

        assert (
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} should match."
            in str(e.value)
        )

    def test_validate_backfill_intiate_status_with_shredder_mitigation_true_and_metadata_label_true_should_pass(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
            shredder_mitigation=True,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE_1).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == True

        validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

    def test_validate_backfill_intiate_status_with_shredder_mitigation_false_and_metadata_label_false_should_pass(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
            shredder_mitigation=False,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == False

        validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE)

    def test_validate_backfill_complete_with_shredder_mitigation_true_and_metadata_label_true_should_pass(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.COMPLETE,
            shredder_mitigation=True,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE_1).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == True

        validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

    def test_validate_backfill_complete_with_shredder_mitigation_false_and_metadata_label_true_should_pass(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.COMPLETE,
            shredder_mitigation=False,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE_1).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == True

        validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

    def test_validate_backfill_complete_without_shredder_mitigation_and_metadata_label_true_should_pass(
        self,
    ):
        valid_backfill = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.COMPLETE,
        )

        metadata_file = Path(
            str(TEST_BACKFILL_FILE_1).replace(BACKFILL_FILE, METADATA_FILE)
        )
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels
        assert has_shredder_mitigation_label == True

        validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)
