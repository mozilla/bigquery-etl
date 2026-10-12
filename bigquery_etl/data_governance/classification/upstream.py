"""Fill the classifier's input tables by running the upstream metadata jobs.

Drives the three scripts the data governance metadata pipeline already has: the
column profiler, the DataHub lineage resolver and the probe fetcher. Each is
pointed at the classification_* copies of its tables instead of the shared ones it
writes on its own schedule.

Profiling goes one dataset at a time into a scratch table, whose rows are then
appended into the consolidated table, because the profiler overwrites its whole
date partition on every run. That truncation is also why the copies exist:
profiling into the shared tables would erase what the scheduled run wrote there.
Lineage and probes rebuild their partition wholesale, so they are invoked
directly.
"""

import datetime
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from google.cloud import bigquery

from .config import (
    LINEAGE_TABLE,
    PROBES_TABLE,
    PROFILES_TABLE,
    ClassificationConfig,
    validate_bq_identifier,
)
from .inputs import client_for
from .runner import TargetKey

# Locations of upstream scripts in sql/
UPSTREAM_PROJECT = "moz-fx-data-shared-prod"
UPSTREAM_DATASET = "data_governance_metadata_derived"
PROFILER_SCRIPT = "column_profiles_v1"
LINEAGE_SCRIPT = "lineage_mapping_v1"
PROBES_SCRIPT = "probe_definitions_v1"

DEFAULT_SQL_DIR = Path("sql")

SCRATCH_EXPIRY_DAYS = 1

# Clears one target's rows from the run's partition, so the copy that follows
# replaces them. Scoped by table when the target names one, otherwise profiling a
# single table would delete the rest of its dataset's rows from that partition.
_DELETE_QUERY = """
DELETE FROM `{profiles_table}`
WHERE profiled_at = @date
  AND source_project = @project
  AND source_dataset = @dataset{table_predicate}
"""


@dataclass
class ProfileSummary:
    """What a profiling pass did."""

    targets: int = 0
    rows: int = 0
    failed: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Destination:
    """The profiles table a pass appends to, opened once for the whole pass."""

    client: bigquery.Client
    table: str
    # The deployed schema, partitioning and clustering, which every scratch table
    # is created from. Read once because it cannot change while the pass runs.
    spec: bigquery.Table


def _destination_for(config: ClassificationConfig) -> _Destination:
    """Open the profiles table the pass appends to."""
    client = client_for(config)
    return _Destination(
        client=client,
        table=config.profiles_table,
        spec=client.get_table(config.profiles_table),
    )


def _target_name(target: TargetKey) -> str:
    """Name a target for a log line or an error message."""
    project, dataset, table = target
    return f"{project}.{dataset}" + (f".{table}" if table is not None else "")


def _script_path(script: str, sql_dir: Path) -> Path:
    """Resolve one upstream query.py, raising before any work if it is missing."""
    path = sql_dir / UPSTREAM_PROJECT / UPSTREAM_DATASET / script / "query.py"
    if not path.is_file():
        raise FileNotFoundError(f"Upstream script not found at {path.resolve()}")
    return path


def _run_script(script: Path, args: list[str]) -> None:
    """Run an upstream query.py to completion, raising if it fails."""
    command = [sys.executable, str(script), *args]
    logging.info("Running %s", " ".join(command))
    # A child process, because the profiler can be OOM-killed: that costs one
    # dataset rather than the whole pass. Output is left uncaptured so progress
    # reaches the task log as it happens.
    subprocess.run(command, check=True)


def _optional_args(max_workers: int | None, dry_run: bool) -> list[str]:
    """Return the flags every script shares, each passed only when asked for.

    max_workers is left out when unset, so each script keeps its own default
    rather than this module restating three numbers it has no opinion about.
    """
    args = []
    if max_workers is not None:
        args += ["--max-workers", str(max_workers)]
    if dry_run:
        args.append("--dry-run")
    return args


def _upstream_args(
    config: ClassificationConfig,
    date: datetime.date,
    *,
    source_table: str,
    destination_table: str,
    max_workers: int | None,
    dry_run: bool,
) -> list[str]:
    """Build the argv shared by the lineage and probe scripts."""
    return [
        "--date",
        date.isoformat(),
        "--source-project",
        config.project,
        "--source-dataset",
        config.dataset,
        "--source-table",
        source_table,
        "--destination-project",
        config.project,
        "--destination-dataset",
        config.dataset,
        "--destination-table",
        destination_table,
    ] + _optional_args(max_workers, dry_run)


def _profiler_args(
    target: TargetKey,
    date: datetime.date,
    scratch: str,
    *,
    max_workers: int | None,
    dry_run: bool,
) -> list[str]:
    """Build the profiler's argv for one target, writing into the scratch table."""
    project, dataset, table = target
    scratch_project, scratch_dataset, scratch_name = scratch.split(".")
    args = [
        "--date",
        date.isoformat(),
        "--source-project",
        project,
        # Exactly one dataset, which is also what makes --tables legal.
        "--source-datasets",
        dataset,
        "--destination-project",
        scratch_project,
        "--destination-dataset",
        scratch_dataset,
        "--destination-table",
        scratch_name,
    ]
    if table is not None:
        args += ["--tables", table]
    return args + _optional_args(max_workers, dry_run)


def _validate_temp_dataset(temp_dataset: str) -> str:
    """Reject a temp_dataset that is not project.dataset, before any target runs.

    Both halves reach a table reference as text, so they are checked the same way
    the config's own project and dataset are.
    """
    parts = temp_dataset.split(".")
    if len(parts) != 2:
        raise ValueError(f"temp_dataset must be project.dataset, got {temp_dataset!r}")
    validate_bq_identifier(parts[0], "temp_dataset project")
    validate_bq_identifier(parts[1], "temp_dataset dataset")
    return temp_dataset


def _scratch_table(temp_dataset: str, dataset: str, date: datetime.date) -> str:
    """Fully qualified name of the scratch table a dataset's profiles land in."""
    return f"{temp_dataset}.classification_profiles_{dataset}_{date:%Y%m%d}"


def _create_scratch_table(destination: _Destination, scratch: str) -> None:
    """Create the scratch table with the destination's own spec, expiring soon."""
    # The spec is copied from the destination rather than restated here, which is
    # what guarantees the copy job out of it is accepted and leaves nothing to
    # drift.
    table = bigquery.Table(scratch, schema=destination.spec.schema)
    table.time_partitioning = destination.spec.time_partitioning
    table.clustering_fields = destination.spec.clustering_fields
    # A safety net for a scratch table orphaned by a killed process.
    table.expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        days=SCRATCH_EXPIRY_DAYS
    )
    destination.client.delete_table(scratch, not_found_ok=True)
    destination.client.create_table(table)


def _delete_target_rows(
    destination: _Destination, target: TargetKey, date: datetime.date
) -> None:
    """Delete one target's rows from the run's partition of the profiles table."""
    project, dataset, table = target
    parameters = [
        bigquery.ScalarQueryParameter("date", "DATE", date),
        bigquery.ScalarQueryParameter("project", "STRING", project),
        bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
    ]
    if table is not None:
        parameters.append(bigquery.ScalarQueryParameter("table", "STRING", table))
    query = _DELETE_QUERY.format(
        profiles_table=destination.table,
        table_predicate="\n  AND source_table = @table" if table is not None else "",
    )
    destination.client.query(
        query, job_config=bigquery.QueryJobConfig(query_parameters=parameters)
    ).result()


def _profile_one(
    destination: _Destination | None,
    target: TargetKey,
    date: datetime.date,
    *,
    script: Path,
    temp_dataset: str,
    max_workers: int | None,
) -> int:
    """Profile one dataset into a scratch table and append it, returning rows appended.

    A dry run has no destination: the profiler is asked what it would do, and
    nothing here touches BigQuery. It still gets the destination flags, which it
    names in its own log line.
    """
    _project, dataset, _table = target
    scratch = _scratch_table(temp_dataset, dataset, date)
    args = _profiler_args(
        target, date, scratch, max_workers=max_workers, dry_run=destination is None
    )
    if destination is None:
        _run_script(script, args)
        return 0

    _create_scratch_table(destination, scratch)
    try:
        _run_script(script, args)
        rows = destination.client.get_table(scratch).num_rows or 0
        if rows:
            _delete_target_rows(destination, target, date)
            # A copy job scans no bytes, and the rows route themselves into the
            # destination's profiled_at partition.
            destination.client.copy_table(
                scratch,
                destination.table,
                job_config=bigquery.CopyJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                ),
            ).result()
            logging.info(
                "Appended %s rows for %s to %s",
                rows,
                _target_name(target),
                destination.table,
            )
        else:
            # Upstream's own semantic for a run that profiled nothing is to leave
            # the partition unchanged. A dataset whose tables are all empty, or
            # all skipped benignly, must not delete rows an earlier run wrote.
            logging.info(
                "Nothing profiled for %s, leaving %s unchanged",
                _target_name(target),
                destination.table,
            )
    except Exception:
        # The scratch table's contents are the only evidence of what the profiler
        # produced, and its expiry reclaims it.
        logging.info("Leaving the scratch table %s in place", scratch)
        raise
    try:
        destination.client.delete_table(scratch, not_found_ok=True)
    except Exception as e:
        # The rows are in the destination by this point, so a failed cleanup is
        # not a failed profile: reporting one would cost a re-scan of the whole
        # dataset. The scratch table's expiry reclaims it.
        logging.warning("Could not delete the scratch table %s: %s", scratch, e)
    return rows


def profile(
    config: ClassificationConfig,
    targets: list[TargetKey],
    date: datetime.date,
    *,
    sql_dir: Path = DEFAULT_SQL_DIR,
    temp_dataset: str | None = None,
    max_workers: int | None = None,
    dry_run: bool = False,
) -> ProfileSummary:
    """Profile the given targets into the profiles table and report what it did.

    Each target is a (project, dataset, table or None) triple. Every target lands
    in the date's partition, because lineage reads exactly one profiled_at
    partition and must be given the same date.

    A failing target is logged and recorded, then the pass moves to the next one.
    Raises at the end if any target failed, so the task goes red while keeping
    the rows it did write.
    """
    # Everything that is the same for every target is settled before the first
    # one runs, so a bad argument or a missing script fails once rather than once
    # per target.
    temp_dataset = _validate_temp_dataset(temp_dataset or f"{config.project}.tmp")
    script = _script_path(PROFILER_SCRIPT, sql_dir)
    destination = None if dry_run else _destination_for(config)
    summary = ProfileSummary()
    logging.info(
        "Profiling %s targets for %s into %s",
        len(targets),
        date.isoformat(),
        config.profiles_table,
    )

    # One target at a time: the appends are writes against one destination
    # partition, and two at once hit BigQuery's serialization retry and then
    # fail.
    for target in targets:
        try:
            rows = _profile_one(
                destination,
                target,
                date,
                script=script,
                temp_dataset=temp_dataset,
                max_workers=max_workers,
            )
        except Exception:
            logging.exception("Profiling %s failed", _target_name(target))
            summary.failed.append(_target_name(target))
            continue
        summary.targets += 1
        summary.rows += rows

    logging.info(
        "Profiled %s targets, %s rows appended, %s targets failed",
        summary.targets,
        summary.rows,
        len(summary.failed),
    )
    if summary.failed:
        raise RuntimeError(f"Profiling failed for {', '.join(summary.failed)}")
    return summary


def _require_datahub_token() -> None:
    """Fail before spawning if the DataHub token the lineage and probe jobs need is unset."""
    # The lineage script exits on its own without a token, but the probe script
    # only warns and then skips every legacy-telemetry probe. Those probes carry
    # the data_sensitivity the prompt uses, so a missing token would quietly
    # classify legacy ping tables worse. Requiring it is this wrapper's policy.
    if not os.environ.get("DATAHUB_GMS_TOKEN", ""):
        raise RuntimeError(
            "DATAHUB_GMS_TOKEN is not set, so the lineage and probe jobs cannot "
            "read DataHub. It is configured as an Airflow secret."
        )


def resolve_lineage(
    config: ClassificationConfig,
    date: datetime.date,
    *,
    sql_dir: Path = DEFAULT_SQL_DIR,
    max_workers: int | None = None,
    dry_run: bool = False,
) -> None:
    """Resolve each profiled table's ping from DataHub into the lineage table.

    Reads the profiles table's partition for the date, so it needs the same date
    the profiling used. Rebuilds its whole partition, so re-running it is safe.
    """
    _require_datahub_token()
    _run_script(
        _script_path(LINEAGE_SCRIPT, sql_dir),
        _upstream_args(
            config,
            date,
            source_table=PROFILES_TABLE,
            destination_table=LINEAGE_TABLE,
            max_workers=max_workers,
            dry_run=dry_run,
        ),
    )


def fetch_probes(
    config: ClassificationConfig,
    date: datetime.date,
    *,
    sql_dir: Path = DEFAULT_SQL_DIR,
    max_workers: int | None = None,
    dry_run: bool = False,
) -> None:
    """Fetch the probe definitions of every ping in the lineage table.

    Reads the lineage table's partition for the date, so it needs the same date
    the lineage resolution used. Rebuilds its whole partition, so re-running it
    is safe.
    """
    _require_datahub_token()
    _run_script(
        _script_path(PROBES_SCRIPT, sql_dir),
        _upstream_args(
            config,
            date,
            source_table=LINEAGE_TABLE,
            destination_table=PROBES_TABLE,
            max_workers=max_workers,
            dry_run=dry_run,
        ),
    )
