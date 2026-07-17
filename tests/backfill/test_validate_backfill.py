from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from bigquery_etl.backfill.utils import (
    MAX_BACKFILL_ENTRY_AGE_DAYS,
    NBR_DAYS_RETAINED,
    get_effective_retention_days,
)
from bigquery_etl.backfill.validate import (
    validate_custom_query_path,
    validate_default_reason,
    validate_default_watchers,
    validate_depends_on_past_end_date,
    validate_duplicate_entry_dates,
    validate_entries,
    validate_entries_are_sorted,
    validate_file,
    validate_old_entry_date,
    validate_query_script_options,
    validate_retention_range,
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
TEST_BACKFILL_FILE_WITH_EXPIRATION = (
    TEST_DIR / "backfill" / "test_dir_with_expiration" / BACKFILL_FILE
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

    def test_get_effective_retention_days_no_expiration(self):
        """Without expiration_days, effective retention is NBR_DAYS_RETAINED."""
        metadata = Metadata.from_file(TEST_BACKFILL_FILE.parent / METADATA_FILE)
        assert get_effective_retention_days(metadata) == NBR_DAYS_RETAINED

    def test_get_effective_retention_days_with_expiration(self):
        """With expiration_days < NBR_DAYS_RETAINED, effective retention uses expiration_days."""
        metadata = Metadata.from_file(
            TEST_BACKFILL_FILE_WITH_EXPIRATION.parent / METADATA_FILE
        )
        assert get_effective_retention_days(metadata) == 180

    def test_validate_retention_range_uses_expiration_days(self):
        """Retention check should use expiration_days when it's smaller than default."""
        # start_date is 200 days before mock today (2021-05-04),
        # which is within NBR_DAYS_RETAINED (775) but exceeds expiration_days (180)
        backfill_entry = Backfill(
            entry_date=TEST_BACKFILL_1.entry_date,
            start_date=date(2020, 10, 15),
            end_date=TEST_BACKFILL_1.end_date,
            excluded_dates=[],
            reason=VALID_REASON,
            watchers=TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
        )

        # Without backfill_file (default retention), this should pass
        validate_retention_range(backfill_entry, TEST_BACKFILL_FILE)

        # With backfill_file that has expiration_days=180, this should fail
        with pytest.raises(ValueError) as e:
            validate_retention_range(backfill_entry, TEST_BACKFILL_FILE_WITH_EXPIRATION)

        assert "more than 180 days prior to entry date" in str(e.value)

    def test_validate_retention_range_exceeds_limit(self):
        """Error should be raised if start_date exceeds retention limit for initiate entries."""
        backfill_entry = Backfill(
            entry_date=TEST_BACKFILL_1.entry_date,
            start_date=date(2019, 1, 1),
            end_date=TEST_BACKFILL_1.end_date,
            excluded_dates=[],
            reason=VALID_REASON,
            watchers=TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
        )

        with pytest.raises(ValueError) as e:
            validate_retention_range(backfill_entry, TEST_BACKFILL_FILE)

        assert f"more than {NBR_DAYS_RETAINED} days prior to entry date" in str(e.value)

    def test_validate_retention_range_override(self):
        """No error should be raised if override_retention_limit is True."""
        backfill_entry = Backfill(
            entry_date=TEST_BACKFILL_1.entry_date,
            start_date=date(2019, 1, 1),
            end_date=TEST_BACKFILL_1.end_date,
            excluded_dates=[],
            reason=VALID_REASON,
            watchers=TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
            override_retention_limit=True,
        )

        validate_retention_range(backfill_entry, TEST_BACKFILL_FILE)

    def test_validate_retention_range_complete_status(self):
        """No error should be raised for complete entries even if start_date exceeds limit."""
        backfill_entry = Backfill(
            entry_date=TEST_BACKFILL_1.entry_date,
            start_date=date(2019, 1, 1),
            end_date=TEST_BACKFILL_1.end_date,
            excluded_dates=[],
            reason=VALID_REASON,
            watchers=TEST_BACKFILL_1.watchers,
            status=BackfillStatus.COMPLETE,
        )

        validate_retention_range(backfill_entry, TEST_BACKFILL_FILE)

    def test_validate_retention_range_within_limit(self):
        """No error should be raised if start_date is within retention limit."""
        backfill_entry = Backfill(
            entry_date=TEST_BACKFILL_1.entry_date,
            start_date=TEST_BACKFILL_1.start_date,
            end_date=TEST_BACKFILL_1.end_date,
            excluded_dates=TEST_BACKFILL_1.excluded_dates,
            reason=VALID_REASON,
            watchers=TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
        )

        validate_retention_range(backfill_entry, TEST_BACKFILL_FILE)

    def test_validate_query_script_missing_entrypoint(self):
        """Error should be raised if query_script_entrypoint is missing for a query script."""
        entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
            custom_query_path="sql/proj/dataset/table/query.py",
            query_script_date_arg="date",
        )
        with pytest.raises(ValueError):
            validate_query_script_options(entry, TEST_BACKFILL_FILE)

    def test_validate_query_script_missing_date_arg(self):
        """Error should be raised if query_script_entrypoint is missing for a query script."""
        entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
            custom_query_path="sql/proj/dataset/table/query.py",
            query_script_entrypoint="main",
        )
        with pytest.raises(ValueError):
            validate_query_script_options(entry, TEST_BACKFILL_FILE)

    def test_validate_query_script_valid(self):
        """No error should be raised if all required arguments are given."""
        entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            TEST_BACKFILL_1.watchers,
            status=BackfillStatus.INITIATE,
            custom_query_path="sql/proj/dataset/table/query.py",
            query_script_entrypoint="main",
            query_script_date_arg="date",
        )
        validate_query_script_options(entry, TEST_BACKFILL_FILE)

    def _custom_query_entry(self, custom_query_path, status=BackfillStatus.INITIATE):
        return Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            [VALID_WATCHER],
            status=status,
            custom_query_path=custom_query_path,
        )

    def test_validate_custom_query_path_valid(self, tmp_path):
        """A custom SQL query that dry runs successfully should pass validation."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1 WHERE @submission_date IS NOT NULL")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            dry_run_cls.return_value.is_valid.return_value = True
            validate_custom_query_path(entry, backfill_file)

        # the partition parameter should be bound for the dry run
        _, kwargs = dry_run_cls.call_args
        assert kwargs["query_parameters"] == {"submission_date": "DATE"}

    def _write_metadata(self, tmp_path, scheduling):
        """Write a minimal metadata.yaml with the given scheduling block."""
        (tmp_path / METADATA_FILE).write_text(
            yaml.dump(
                {
                    "friendly_name": "test",
                    "description": "test",
                    "owners": ["nobody@mozilla.com"],
                    "scheduling": scheduling,
                }
            )
        )

    def _write_schema(self, tmp_path, fields):
        """Write a schema.yaml with the given fields."""
        (tmp_path / "schema.yaml").write_text(yaml.dump({"fields": fields}))

    def test_validate_custom_query_path_binds_custom_partition_parameter(
        self, tmp_path
    ):
        """The dry run should bind the table's date_partition_parameter, not submission_date."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1 WHERE @first_seen_date IS NOT NULL")
        backfill_file = tmp_path / BACKFILL_FILE
        self._write_metadata(tmp_path, {"date_partition_parameter": "first_seen_date"})
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            dry_run_cls.return_value.is_valid.return_value = True
            validate_custom_query_path(entry, backfill_file)

        _, kwargs = dry_run_cls.call_args
        assert kwargs["query_parameters"] == {"first_seen_date": "DATE"}

    def test_validate_custom_query_path_binds_scheduling_parameters(self, tmp_path):
        """The dry run should bind scheduling parameters alongside the partition parameter."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT @submission_date, @n")
        backfill_file = tmp_path / BACKFILL_FILE
        self._write_metadata(tmp_path, {"parameters": ["n:INT64:1"]})
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            dry_run_cls.return_value.is_valid.return_value = True
            validate_custom_query_path(entry, backfill_file)

        _, kwargs = dry_run_cls.call_args
        assert kwargs["query_parameters"] == {"n": "INT64", "submission_date": "DATE"}

    def test_validate_custom_query_path_override_null_partition_binds_submission_date(
        self, tmp_path
    ):
        """With override_depends_on_past_null_partition, submission_date is bound even when the metadata partition parameter is null."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1 WHERE @submission_date IS NOT NULL")
        backfill_file = tmp_path / BACKFILL_FILE
        self._write_metadata(
            tmp_path,
            {
                "date_partition_parameter": None,
                "parameters": ["submission_date:DATE:{{ds}}"],
            },
        )
        entry = Backfill(
            TEST_BACKFILL_1.entry_date,
            TEST_BACKFILL_1.start_date,
            TEST_BACKFILL_1.end_date,
            TEST_BACKFILL_1.excluded_dates,
            VALID_REASON,
            [VALID_WATCHER],
            status=BackfillStatus.INITIATE,
            custom_query_path=str(query_file),
            override_depends_on_past_null_partition=True,
        )

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            dry_run_cls.return_value.is_valid.return_value = True
            validate_custom_query_path(entry, backfill_file)

        _, kwargs = dry_run_cls.call_args
        assert kwargs["query_parameters"] == {"submission_date": "DATE"}

    def test_validate_custom_query_path_schema_compatible_should_pass(self, tmp_path):
        """A custom query whose output fits the schema.yaml should pass."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1")
        backfill_file = tmp_path / BACKFILL_FILE
        # schema.yaml declares two columns; the query returns a compatible subset
        self._write_schema(
            tmp_path,
            [
                {"name": "submission_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
            ],
        )
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            instance = dry_run_cls.return_value
            instance.is_valid.return_value = True
            instance.get_schema.return_value = {
                "fields": [
                    {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
                ]
            }
            validate_custom_query_path(entry, backfill_file)

    def test_validate_custom_query_path_schema_incompatible_should_fail(self, tmp_path):
        """A custom query producing a column not in the schema.yaml should raise."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1")
        backfill_file = tmp_path / BACKFILL_FILE
        self._write_schema(
            tmp_path,
            [{"name": "submission_date", "type": "DATE", "mode": "NULLABLE"}],
        )
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            instance = dry_run_cls.return_value
            instance.is_valid.return_value = True
            instance.get_schema.return_value = {
                "fields": [
                    {"name": "unexpected_col", "type": "STRING", "mode": "NULLABLE"},
                ]
            }
            with pytest.raises(ValueError) as e:
                validate_custom_query_path(entry, backfill_file)

        assert "schema is incompatible" in str(e.value)

    def test_validate_custom_query_path_no_schema_file_skips_schema_check(
        self, tmp_path
    ):
        """With no schema.yaml, the dry run still validates but the schema check is skipped."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            instance = dry_run_cls.return_value
            instance.is_valid.return_value = True
            validate_custom_query_path(entry, backfill_file)

        # schema check short-circuits before reading the dry run schema
        instance.get_schema.assert_not_called()

    def test_validate_custom_query_path_invalid_should_fail(self, tmp_path):
        """A custom SQL query that fails the dry run should raise."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT bad syntax")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            instance = dry_run_cls.return_value
            instance.is_valid.return_value = False
            instance.errors.return_value = [{"message": "Syntax error"}]
            with pytest.raises(ValueError) as e:
                validate_custom_query_path(entry, backfill_file)

        assert "Custom query dry run failed" in str(e.value)

    def test_validate_custom_query_path_python_valid_syntax(self, tmp_path):
        """A .py custom query with valid syntax should pass the syntax check without dry running."""
        query_file = tmp_path / "backfill_custom.py"
        query_file.write_text("def main():\n    return 1\n")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            validate_custom_query_path(entry, backfill_file)

        # .py is syntax-checked offline, never dry run
        dry_run_cls.assert_not_called()

    def test_validate_custom_query_path_python_invalid_syntax_should_fail(
        self, tmp_path
    ):
        """A .py custom query with a syntax error should raise a ValueError."""
        query_file = tmp_path / "backfill_custom.py"
        query_file.write_text("def main(:\n    return 1\n")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            with pytest.raises(ValueError) as e:
                validate_custom_query_path(entry, backfill_file)

        assert "syntax error" in str(e.value).lower()
        dry_run_cls.assert_not_called()

    def test_validate_custom_query_path_python_not_executed(self, tmp_path):
        """A .py custom query's syntax check should not execute its module-level code."""
        query_file = tmp_path / "backfill_custom.py"
        # would raise at import time if executed, but is syntactically valid
        query_file.write_text("raise RuntimeError('should not run')\n")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(query_file))

        with patch("bigquery_etl.dryrun.DryRun"):
            validate_custom_query_path(entry, backfill_file)

    def test_validate_custom_query_path_skips_complete_status(self, tmp_path):
        """A Complete-status entry should be skipped without dry running."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1")
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(
            str(query_file), status=BackfillStatus.COMPLETE
        )

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            validate_custom_query_path(entry, backfill_file)

        dry_run_cls.assert_not_called()

    def test_validate_custom_query_path_skips_when_none(self, tmp_path):
        """An entry without a custom_query_path should be a no-op."""
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(None)

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            validate_custom_query_path(entry, backfill_file)

        dry_run_cls.assert_not_called()

    def test_validate_custom_query_path_missing_file_should_fail(self, tmp_path):
        """A custom_query_path that doesn't resolve to a file should raise a ValueError."""
        backfill_file = tmp_path / BACKFILL_FILE
        entry = self._custom_query_entry(str(tmp_path / "does_not_exist.sql"))

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            with pytest.raises(ValueError) as e:
                validate_custom_query_path(entry, backfill_file)

        assert "custom_query_path not found" in str(e.value)
        dry_run_cls.assert_not_called()

    def test_validate_custom_query_path_resolves_sibling(self, tmp_path):
        """A custom_query_path that only exists next to backfill.yaml should still resolve."""
        query_file = tmp_path / "backfill_custom.sql"
        query_file.write_text("SELECT 1")
        backfill_file = tmp_path / BACKFILL_FILE
        # stored path points somewhere that doesn't exist; only the sibling does
        entry = self._custom_query_entry("sql/proj/dataset/table/backfill_custom.sql")

        with patch("bigquery_etl.dryrun.DryRun") as dry_run_cls:
            dry_run_cls.return_value.is_valid.return_value = True
            validate_custom_query_path(entry, backfill_file)

        args, kwargs = dry_run_cls.call_args
        assert kwargs["sqlfile"] == str(query_file)
