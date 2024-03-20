"""Validate backfill entries."""

from pathlib import Path
from typing import List

from ..backfill.parse import DEFAULT_REASON, DEFAULT_WATCHER, Backfill, BackfillStatus


def validate_excluded_dates(entry: Backfill) -> None:
    """Check if backfill excluded dates are sorted and have no duplicates."""
    if not entry.excluded_dates == sorted(entry.excluded_dates):
        raise ValueError(
            f"Existing backfill entry with excluded dates not sorted: {entry.entry_date}."
        )
    if not len(entry.excluded_dates) == len(set(entry.excluded_dates)):
        raise ValueError(
            f"Existing backfill entry with duplicate excluded dates: {entry.entry_date}."
        )


def validate_default_reason(entry: Backfill) -> None:
    """Check if backfill reason is the same as default."""
    if entry.reason == DEFAULT_REASON:
        raise ValueError(f"Default reason found: {entry.reason}.")


def validate_default_watchers(entry: Backfill) -> None:
    """Check if backfill watcher is the same as default."""
    if DEFAULT_WATCHER in entry.watchers:
        raise ValueError(f"Default watcher found: ({entry.watchers}).")


def validate_entries_are_sorted(backfills: List[Backfill]) -> None:
    """Check if list of backfill entries are sorted by entry dates."""
    entry_dates = [backfill.entry_date for backfill in backfills]
    if not entry_dates == sorted(entry_dates, reverse=True):
        raise ValueError("Backfill entries are not sorted by entry dates")


def validate_file(file: Path) -> None:
    """Validate all entries from a given backfill.yaml file."""
    backfills = Backfill.entries_from_file(file)
    validate_entries(backfills)


def validate_duplicate_entry_with_initiate_status(
    backfill_entry: Backfill, backfills: list
):
    """Check if list of backfill entries have more than one entry with Initiate Status."""
    if backfill_entry.status == BackfillStatus.INITIATE:
        for backfill_entry_1 in backfills:
            if backfill_entry_1.status == BackfillStatus.INITIATE:
                raise ValueError(
                    "Backfill entries cannot contain more than one entry with Initiate status"
                )


def validate_entries(backfills: list) -> None:
    """Validate a list of backfill entries."""
    for i, backfill_entry in enumerate(backfills):
        validate_default_watchers(backfill_entry)
        validate_default_reason(backfill_entry)
        validate_excluded_dates(backfill_entry)
        validate_duplicate_entry_with_initiate_status(
            backfill_entry, backfills[i + 1 :]
        )
    validate_entries_are_sorted(backfills)
