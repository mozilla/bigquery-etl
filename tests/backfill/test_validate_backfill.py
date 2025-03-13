from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from bigquery_etl.backfill.utils import MAX_BACKFILL_ENTRY_AGE_DAYS
from bigquery_etl.backfill.validate import (
    validate_default_reason,
    validate_default_watchers,
    validate_depends_on_past_end_date,
    validate_duplicate_entry_dates,
    validate_entries,
    validate_entries_are_sorted,
    validate_file,
    validate_old_entry_date,
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
TEST_BACKFILL_FILE_DEPENDS_ON_PAST = (
    TEST_DIR / "backfill" / "test_dir_depends_on_past" / BACKFILL_FILE
)


class TestValidateBackfill(object):

    @pytest.fixture(autouse=True)
    def mock_date(self):
        """Use fixed date.today() for tests."""
        with patch("bigquery_etl.backfill.validate.datetime.date") as d:
            d.today.return_value = date(2021, 5, 4)
            d.side_effect = lambda *args, **kw: date(*args, **kw)
            yield

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
        assert has_shredder_mitigation_label is False

        with pytest.raises(ValueError) as e:
            validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE)

        assert (
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} entry {TEST_BACKFILL_1.entry_date} should match."
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
        assert has_shredder_mitigation_label is True

        with pytest.raises(ValueError) as e:
            validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

        assert (
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} entry {TEST_BACKFILL_1.entry_date} should match."
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
        assert has_shredder_mitigation_label is True

        with pytest.raises(ValueError) as e:
            validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

        assert (
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} entry {TEST_BACKFILL_1.entry_date} should match."
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
        assert has_shredder_mitigation_label is True

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
        assert has_shredder_mitigation_label is False

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
        assert has_shredder_mitigation_label is True

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
        assert has_shredder_mitigation_label is True

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
        assert has_shredder_mitigation_label is True

        validate_shredder_mitigation(valid_backfill, TEST_BACKFILL_FILE_1)

    def test_validate_depends_on_past_end_date_override(self):
        """override_depends_on_past_end_date should allow end date to be before entry date."""
        backfill_entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            date(2021, 5, 1),
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
            override_depends_on_past_end_date=True,
        )

        metadata_file = TEST_BACKFILL_FILE_DEPENDS_ON_PAST.parent / METADATA_FILE
        metadata = Metadata.from_file(metadata_file)
        assert metadata.scheduling.get(
            "depends_on_past"
        ), "depends_on_past should be true for this test"

        validate_depends_on_past_end_date(
            backfill_entry, TEST_BACKFILL_FILE_DEPENDS_ON_PAST
        )

    def test_validate_depends_on_past_end_date_without_override(self):
        """override_depends_on_past_end_date=false or unset should fail if end date to be before entry date."""
        backfill_entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            date(2021, 5, 1),
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
        )

        metadata_file = TEST_BACKFILL_FILE_DEPENDS_ON_PAST.parent / METADATA_FILE
        metadata = Metadata.from_file(metadata_file)
        assert metadata.scheduling.get(
            "depends_on_past"
        ), "depends_on_past should be true for this test"

        with pytest.raises(ValueError):
            validate_depends_on_past_end_date(
                backfill_entry, TEST_BACKFILL_FILE_DEPENDS_ON_PAST
            )

        backfill_entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            date(2021, 5, 1),
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            TEST_BACKFILL_1.status,
            override_depends_on_past_end_date=False,
        )

        with pytest.raises(ValueError):
            validate_depends_on_past_end_date(
                backfill_entry, TEST_BACKFILL_FILE_DEPENDS_ON_PAST
            )

    @patch("bigquery_etl.backfill.validate.datetime.date")
    def test_validate_old_entry_date_initiate(self, mock_date):
        """Error should be raised if an initiate entry is older than the max allowed."""
        mock_date.today.return_value = date(2021, 6, 4)
        backfill_entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
        )

        with pytest.raises(ValueError) as e:
            validate_old_entry_date(backfill_entry)

        assert (
            f"Backfill entries will not run if they are older than {MAX_BACKFILL_ENTRY_AGE_DAYS} days old"
            in str(e.value)
        )

    @patch("bigquery_etl.backfill.validate.datetime.date")
    def test_validate_old_entry_date_complete(self, mock_date):
        """No error should be raised for complete entries that are older than the max allowed."""
        mock_date.today.return_value = date(2021, 6, 4)
        backfill_entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.COMPLETE,
        )

        validate_old_entry_date(backfill_entry)
