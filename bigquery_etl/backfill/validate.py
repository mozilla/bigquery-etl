"""Validate backfill entries."""

import datetime
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
from .utils import (
    MAX_BACKFILL_ENTRY_AGE_DAYS,
    NBR_DAYS_RETAINED,
    get_effective_retention_days,
)


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
    """Check if shredder mitigation in backfill entry and metadata label matches."""
    if entry.status == BackfillStatus.INITIATE:
        metadata_file = Path(str(backfill_file).replace(BACKFILL_FILE, METADATA_FILE))
        metadata = Metadata.from_file(metadata_file)
        has_shredder_mitigation_label = SHREDDER_MITIGATION_LABEL in metadata.labels

        if has_shredder_mitigation_label != entry.shredder_mitigation:
            raise ValueError(
                f"{SHREDDER_MITIGATION_LABEL} label in {METADATA_FILE} and {BACKFILL_FILE} entry {entry.entry_date} should match."
            )


def validate_depends_on_past_end_date(backfill_entry: Backfill, backfill_file: Path):
    """Check if the table depends on past and has an end_date before the entry date.

    An end date in the past may result in data inconsistencies with depends_on_past tables.
    """
    if backfill_entry.override_depends_on_past_end_date:
        return

    table_metadata = Metadata.from_file(backfill_file.parent / METADATA_FILE)

    if not table_metadata.scheduling.get("depends_on_past", False):
        return

    if backfill_entry.end_date < backfill_entry.entry_date:
        raise ValueError(
            "End date must be on or after the backfill entry date for a depends_on_past table. "
            "Use --override-depends-on-past-end-date flag with `bqetl backfill create` to override this check."
        )


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


def validate_old_entry_date(backfill_entry: Backfill) -> None:
    """Check if entry is in Initiate but is too old to run."""
    if (
        backfill_entry.status == BackfillStatus.INITIATE
        and backfill_entry.entry_date
        < datetime.date.today() - datetime.timedelta(days=MAX_BACKFILL_ENTRY_AGE_DAYS)
    ):
        raise ValueError(
            "Backfill entries will not run if they are older than "
            f"{MAX_BACKFILL_ENTRY_AGE_DAYS} days old"
        )


def validate_retention_range(backfill_entry: Backfill, backfill_file: Path) -> None:
    """Check if start date exceeds retention limit.

    When backfill_file is provided, also considers the table's partition
    expiration_days and uses the smaller of that and the default retention limit.
    """
    if (
        backfill_entry.status != BackfillStatus.INITIATE
        or backfill_entry.override_retention_limit
    ):
        return

    retention_days = NBR_DAYS_RETAINED
    metadata_file = backfill_file.parent / METADATA_FILE
    if metadata_file.exists():
        metadata = Metadata.from_file(metadata_file)
        retention_days = get_effective_retention_days(metadata)

    if backfill_entry.start_date < backfill_entry.entry_date - datetime.timedelta(
        days=retention_days - 1
    ):
        raise ValueError(
            f"Cannot backfill more than {retention_days} days prior to entry date "
            "due to retention policies. "
            "Add `override_retention_limit: true` to backfill to override this check."
        )


def validate_reinitialize_sampling_batch_size_value(batch_size) -> None:
    """Check that a reinitialize sampling batch size is within the valid range.

    A None value is allowed (the default batch size is used). Raises ValueError
    for any out-of-range value.
    """
    if batch_size is None:
        return
    if not 1 <= batch_size <= 100:
        raise ValueError(
            f"reinitialize_sampling_batch_size must be between 1 and 100, got {batch_size}."
        )


def validate_reinitialize_sampling_batch_size(backfill_entry: Backfill) -> None:
    """Check that the entry's reinitialize sampling batch size is within range."""
    validate_reinitialize_sampling_batch_size_value(
        backfill_entry.reinitialize_sampling_batch_size
    )


def validate_override_depends_on_past_null_partition(backfill_entry: Backfill) -> None:
    """Check that override_depends_on_past_null_partition is used correctly.

    The override bypasses the depends_on_past + null date_partition_parameter guard. That
    is only safe for a custom query that processes each partition independently; the
    table's own scheduled query relies on the depends-on-past replay and must not be run
    per-partition.

    It is also mutually exclusive with reinitialize_table: reinitialize rebuilds the whole
    table via its is_init() query, which is the heavy path the override exists to avoid.
    Setting both is contradictory and, because override is checked first in
    validate_depends_on_past, reinitialize_table would be silently ignored.
    """
    if (
        backfill_entry.override_depends_on_past_null_partition
        and not backfill_entry.custom_query_path
    ):
        raise ValueError(
            "override_depends_on_past_null_partition is only allowed on entries with a custom_query_path."
        )

    if (
        backfill_entry.override_depends_on_past_null_partition
        and backfill_entry.reinitialize_table
    ):
        raise ValueError(
            "override_depends_on_past_null_partition and reinitialize_table are mutually exclusive; "
            "set only one."
        )


def _resolve_custom_query_path(custom_query_path: str, backfill_file: Path) -> Path:
    """Resolve a backfill entry's custom_query_path to an existing file.

    custom_query_path is stored repo-root-relative. If that path doesn't exist
    (e.g. validate was run from a different working directory), fall back to
    resolving it as a file sitting next to the backfill.yaml.
    """
    path = Path(custom_query_path)
    if path.exists():
        return path
    sibling = backfill_file.parent / path.name
    if sibling.exists():
        return sibling
    raise ValueError(f"custom_query_path not found: {custom_query_path}")


def _custom_query_parameters(backfill_entry: Backfill, backfill_file: Path) -> dict:
    """Resolve the query parameters to bind when dry running a custom query.

    Mirrors how `bqetl backfill` binds parameters at run time: the table's
    date_partition_parameter (from the sibling metadata.yaml, defaulting to
    submission_date) plus any scheduling `parameters`. The
    override_depends_on_past_null_partition path forces submission_date as the
    partition parameter even when metadata leaves it null, so honor that here too.
    """
    parameters: dict = {}

    metadata_file = backfill_file.parent / METADATA_FILE
    scheduling = {}
    if metadata_file.exists():
        scheduling = Metadata.from_file(metadata_file).scheduling or {}

    # scheduling parameters are "name:type:value"; bind name -> type
    for parameter in scheduling.get("parameters", []):
        name, parameter_type, _ = parameter.strip().split(":", 2)
        parameters[name] = parameter_type or "STRING"

    if backfill_entry.override_depends_on_past_null_partition:
        # the override binds submission_date per-partition regardless of metadata
        date_partition_parameter = "submission_date"
    else:
        date_partition_parameter = scheduling.get(
            "date_partition_parameter", "submission_date"
        )

    if date_partition_parameter and date_partition_parameter not in parameters:
        parameters[date_partition_parameter] = "DATE"

    return parameters


def _validate_custom_py_syntax(query_path: Path, backfill_entry: Backfill) -> None:
    """Syntax-check a custom .py query without executing it."""
    source = query_path.read_text()
    try:
        compile(source, str(query_path), "exec")
    except SyntaxError as e:
        raise ValueError(
            f"Custom query has a syntax error in {backfill_entry.custom_query_path} "
            f"(entry {backfill_entry.entry_date}): {e}"
        ) from e


def validate_custom_query_path(
    backfill_entry: Backfill,
    backfill_file: Path,
    dry_run=True,
    use_cloud_function: bool = True,
    billing_project: str = None,
) -> None:
    """Validate a backfill entry's custom query before it runs.

    Only validates entries that would still be processed (Initiate status). SQL custom
    queries are dry run against BigQuery and will validate the schema against schema.yaml.
    Python script custom queries (.py) are syntax checked.
    SQL dry runs are only run when dry_run=True.
    """
    # delay import to avoid pulling BigQuery deps into offline validation paths
    from ..dryrun import DryRun

    if backfill_entry.status != BackfillStatus.INITIATE:
        return

    if not backfill_entry.custom_query_path:
        return

    query_path = _resolve_custom_query_path(
        backfill_entry.custom_query_path, backfill_file
    )

    if backfill_entry.custom_query_path.endswith(".py"):
        _validate_custom_py_syntax(query_path, backfill_entry)
        return
    elif not dry_run:
        return

    dry_run = DryRun(
        sqlfile=str(query_path),
        use_cloud_function=use_cloud_function,
        billing_project=billing_project,
        query_parameters=_custom_query_parameters(backfill_entry, backfill_file),
    )

    if not dry_run.is_valid():
        errors = dry_run.errors()
        raise ValueError(
            f"Custom query dry run failed for {backfill_entry.custom_query_path} "
            f"(entry {backfill_entry.entry_date}): {errors}"
        )

    _validate_custom_query_schema(dry_run, backfill_entry, backfill_file)


def _validate_custom_query_schema(
    dry_run, backfill_entry: Backfill, backfill_file: Path
) -> None:
    """Check the custom query's output schema fits the table's schema.yaml."""
    # delay import to prevent circular imports in 'bigquery_etl.schema'
    from ..schema import SCHEMA_FILE, Schema

    schema_path = backfill_file.parent / SCHEMA_FILE
    if not schema_path.is_file():
        return

    query_schema_json = dry_run.get_schema()
    if not query_schema_json.get("fields"):
        # no schema came back from the dry run (e.g. DDL/DML); nothing to compare
        return

    query_schema = Schema.from_json(query_schema_json)
    defined_schema = Schema.from_schema_file(schema_path)

    if not query_schema.compatible(defined_schema):
        raise ValueError(
            f"Custom query schema is incompatible with {schema_path} for "
            f"{backfill_entry.custom_query_path} (entry {backfill_entry.entry_date})."
        )


def validate_query_script_options(
    backfill_entry: Backfill, backfill_file: Path
) -> None:
    """Check if required script options are provided when the query file is a .py script."""
    if (
        backfill_entry.custom_query_path is not None
        and backfill_entry.custom_query_path.endswith(".py")
    ) or (backfill_file.parent / "query.py").exists():
        if (
            backfill_entry.query_script_entrypoint is None
            or backfill_entry.query_script_date_arg is None
        ):
            raise ValueError(
                "Backfill for query scripts require --query-script-entrypoint "
                "and --query-script-date-arg arguments"
            )


def validate_file(file: Path) -> None:
    """Validate all entries from a given backfill.yaml file."""
    backfills = Backfill.entries_from_file(file)
    validate_entries(backfills, file)


def validate_entries(backfills: List[Backfill], backfill_file: Path) -> None:
    """Validate a list of backfill entries."""
    for i, backfill_entry in enumerate(backfills):
        validate_default_watchers(backfill_entry)
        validate_default_reason(backfill_entry)
        validate_duplicate_entry_dates(backfill_entry, backfills[i + 1 :])
        validate_excluded_dates(backfill_entry)
        validate_shredder_mitigation(backfill_entry, backfill_file)
        validate_duplicate_entry_with_initiate_status(
            backfill_entry, backfills[i + 1 :]
        )
        validate_depends_on_past_end_date(backfill_entry, backfill_file)
        validate_old_entry_date(backfill_entry)
        validate_retention_range(backfill_entry, backfill_file)
        validate_query_script_options(backfill_entry, backfill_file)
        validate_reinitialize_sampling_batch_size(backfill_entry)
        validate_override_depends_on_past_null_partition(backfill_entry)
    validate_entries_are_sorted(backfills)


class BackfillConfigurationError(Exception):
    """Backfill configuration error."""
