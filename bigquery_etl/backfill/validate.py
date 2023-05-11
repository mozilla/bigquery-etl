"""Validate backfill entries."""
from ..backfill.parse import DEFAULT_REASON, Backfill, BackfillStatus

def validate_one(backfill, backfills):
    """Check new backfill entry against existing backfills."""
    for entry in backfills:
        if entry.status == BackfillStatus.DRAFTING:
            validate_overlap_dates(backfill, entry)


def validate_overlap_dates(entry_1: Backfill, entry_2: Backfill):
    """Check overlap dates between two backfill entries."""
    if max(entry_1.start_date, entry_2.start_date) <= min(
        entry_1.end_date, entry_2.end_date
    ):
        raise ValueError(
            f"Existing backfill entry with overlap dates from: {entry_1.entry_date}."
        )


def validate_reason(backfill):
    """Check is backfill reason is the same as placeholder."""
    if backfill.reason == DEFAULT_REASON:
        raise ValueError(f"Invalid Reason: {backfill.reason}.")


def validate_entries_are_sorted(backfills):
    """Validate list of backfill entries."""
    if not list(backfills.keys()) == sorted(backfills, reverse=True):
        raise ValueError("Backfill entries are not sorted")
