"""Validate backfill entries."""
from ..backfill.parse import DEFAULT_REASON, Backfill, BackfillStatus


def valdiate_duplicate_entry_dates(entry_1: Backfill, entry_2: Backfill):
    """Validate duplicate entry dates between two backfill entries."""
    if entry_1.entry_date == entry_2.end_date:
        raise ValueError(f"Duplicate backfill with entry date: {entry_1.entry_date}.")


def validate_overlap_dates(entry_1: Backfill, entry_2: Backfill):
    """Check overlap dates between two backfill entries."""
    if max(entry_1.start_date, entry_2.start_date) <= min(
        entry_1.end_date, entry_2.end_date
    ):
        raise ValueError(
            f"Existing backfill entry with overlap dates from: {entry_1.entry_date}."
        )


def validate_reason(backfill):
    """Check if backfill reason is the same as placeholder."""
    if backfill.reason in [DEFAULT_REASON, ""]:
        raise ValueError(f"Invalid Reason: {backfill.reason}.")


def validate_entries_are_sorted(backfills):
    """Validate list of backfill entries are sorted."""
    entry_dates = [backfill.entry_date for backfill in backfills]
    if not entry_dates == sorted(entry_dates, reverse=True):
        raise ValueError("Backfill entries are not sorted")


def validate_all_entries(backfills):
    """Validate list of backfill entries."""
    for i, backfill_entry_1 in enumerate(backfills):
        validate_reason(backfill_entry_1)
        for j, backfill_entry_2 in enumerate(backfills):
            if (
                i != j
                and backfill_entry_1.status == BackfillStatus.DRAFTING
                and backfill_entry_2.status == BackfillStatus.DRAFTING
            ):
                valdiate_duplicate_entry_dates(backfill_entry_1, backfill_entry_2)
                validate_overlap_dates(backfill_entry_1, backfill_entry_2)

    validate_entries_are_sorted(backfills)
