"""Validate backfill entries."""
from ..backfill.parse_backfill import BackfillStatus


def validate(backfill, backfills):
    """Check new backfill entry against existing backfills."""
    for entry_date, entry in backfills.items():
        if entry.status == BackfillStatus.Drafting.value:
            validate_overlap_dates(backfill, entry)


def validate_overlap_dates(backfill, entry):
    """Check overlap dates between two backfill entries."""
    if max(backfill.start_date, entry.start_date) <= min(
        backfill.end_date, entry.end_date
    ):
        raise ValueError(
            f"Existing backfill entry with overlap dates from: {entry.entry_date}."
        )


def validate_reason(backfill):
    """Check is backfill reason is the same as placeholder."""
    if (
        backfill.reason
        == "Please provide a reason for the backfill and links to any related bugzilla or jira tickets"
    ):
        raise ValueError(f"Invalid Reason: {backfill.reason}.")


def validate_entries(backfills):
    """Validate list of backfill entries."""
    if not list(backfills.keys()) == sorted(backfills, reverse=True):
        raise ValueError("Backfill entries are not sorted")
