"""Validate backfill entries."""

from pathlib import Path
from typing import List

from ..backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from ..metadata.validate_metadata import SHREDDER_MITIGATION_LABEL


def validate_duplicate_entry_dates(
    backfill_entry: Backfill, backfills: list[Backfill]
) -> None:
    """Check if backfill entries have the same entry dates."""
    for b in backfills:
        if backfill_entry.entry_date == b.entry_date:
            raise ValueError(
                f"Duplicate backfill with entry date: {backfill_entry.entry_date}."
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
    entry_dates = [b.entry_date for b in backfills]
    if not entry_dates == sorted(entry_dates, reverse=True):
        raise ValueError("Backfill entries are not sorted by entry dates")


def validate_shredder_mitigation(entry: Backfill, backfill_file: Path) -> None:

    metadata_file = Path(str(backfill_file).replace(BACKFILL_FILE, METADATA_FILE))
    metadata = Metadata.from_file(metadata_file)
    has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels

    if has_shredder_mitigation_label != entry.shredder_mitigation:
        raise ValueError(
            f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} should match."
        )


def validate_file(file: Path) -> None:
    """Validate all entries from a given backfill.yaml file."""
    backfills = Backfill.entries_from_file(file)
    validate_entries(backfills, file)


def validate_duplicate_entry_with_initiate_status(
    backfill_entry: Backfill, backfills: list
) -> None:
    """Check if list of backfill entries have more than one entry with Initiate Status."""
    if backfill_entry.status == BackfillStatus.INITIATE:
        for b in backfills:
            if b.status == BackfillStatus.INITIATE:
                raise ValueError(
                    "Backfill entries cannot contain more than one entry with Initiate status"
                )


def validate_entries(backfills: list, file: Path) -> None:
    """Validate a list of backfill entries."""
    for i, backfill_entry in enumerate(backfills):
        validate_default_watchers(backfill_entry)
        validate_default_reason(backfill_entry)
        validate_duplicate_entry_dates(backfill_entry, backfills[i + 1 :])
        validate_excluded_dates(backfill_entry)
        validate_shredder_mitigation(backfill_entry, file)
        validate_duplicate_entry_with_initiate_status(
            backfill_entry, backfills[i + 1 :]
        )
    validate_entries_are_sorted(backfills)
