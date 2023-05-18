"""Validate backfill entries."""
from pathlib import Path

from ..backfill.parse import DEFAULT_REASON, DEFAULT_WATCHER, Backfill, BackfillStatus
from ..query_scheduling.utils import is_email_or_github_identity


def validate_duplicate_entry_dates(entry_1: Backfill, entry_2: Backfill) -> None:
    """Check if backfill entries have the same entry dates."""
    if entry_1.entry_date == entry_2.entry_date:
        raise ValueError(f"Duplicate backfill with entry date: {entry_1.entry_date}.")


def validate_overlap_dates(entry_1: Backfill, entry_2: Backfill) -> None:
    """Check overlap dates between two backfill entries."""
    if max(entry_1.start_date, entry_2.start_date) <= min(
        entry_1.end_date, entry_2.end_date
    ):
        raise ValueError(
            f"Existing backfill entry with overlap dates from: {entry_1.entry_date}."
        )


def validate_reason(entry: Backfill) -> None:
    """Check if backfill reason is the same as default or empty."""
    if entry.reason in [DEFAULT_REASON, ""]:
        raise ValueError(f"Invalid Reason: {entry.reason}.")


def validate_watchers(entry: Backfill) -> None:
    """Check if backfill watcher is the same as default or empty."""
    if not entry.watchers or not all(
        map(
            lambda e: is_email_or_github_identity(e) and e != DEFAULT_WATCHER,
            entry.watchers,
        )
    ):
        raise ValueError(f"Invalid Watchers: {entry.watchers}.")


def validate_entries_are_sorted(backfills: list) -> None:
    """Check if list of backfill entries are sorted."""
    entry_dates = [backfill.entry_date for backfill in backfills]
    if not entry_dates == sorted(entry_dates, reverse=True):
        raise ValueError("Backfill entries are not sorted")


def validate_file(file: Path) -> None:
    """Validate a given backfill.yaml file."""
    backfills = Backfill.entries_from_file(file)
    validate_entries(backfills)


def validate_entries(backfills: list) -> None:
    """Validate a list of backfill entries."""
    for i, backfill_entry_1 in enumerate(backfills):
        validate_watchers(backfill_entry_1)
        validate_reason(backfill_entry_1)
        for j, backfill_entry_2 in enumerate(backfills):
            if (
                i != j
                and backfill_entry_1.status == BackfillStatus.DRAFTING
                and backfill_entry_2.status == BackfillStatus.DRAFTING
            ):
                validate_duplicate_entry_dates(backfill_entry_1, backfill_entry_2)
                validate_overlap_dates(backfill_entry_1, backfill_entry_2)

    validate_entries_are_sorted(backfills)
