"""Validate backfill entries."""
from pathlib import Path

from ..backfill.parse import DEFAULT_REASON, DEFAULT_WATCHER, Backfill, BackfillStatus


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
            f"Existing backfill entry with overlap dates from: {entry_2.entry_date}."
        )


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


def validate_reason(entry: Backfill) -> None:
    """Check if backfill reason is the same as default or empty."""
    if not entry.reason or entry.reason == DEFAULT_REASON:
        raise ValueError(f"Invalid Reason: {entry.reason}.")


def validate_watchers(entry: Backfill) -> None:
    """Check if backfill watcher is the same as default or duplicated."""
    watcher_dict = set()
    for watcher in entry.watchers:
        if watcher in watcher_dict or watcher == DEFAULT_WATCHER:
            raise ValueError(f"Invalid Watcher: {watcher}.")
        watcher_dict.add(watcher)


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
        validate_excluded_dates(backfill_entry_1)

        for j, backfill_entry_2 in enumerate(backfills[i + 1 :], start=i + 1):
            if (
                backfill_entry_1.status == BackfillStatus.DRAFTING
                and backfill_entry_2.status == BackfillStatus.DRAFTING
            ):
                validate_duplicate_entry_dates(backfill_entry_1, backfill_entry_2)
                validate_overlap_dates(backfill_entry_1, backfill_entry_2)

    validate_entries_are_sorted(backfills)
