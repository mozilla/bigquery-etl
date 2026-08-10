"""Classify the columns of a set of datasets and write the results.

For each dataset it is given: read the columns to classify, drop the ones an
earlier run already did with this model, suppress and sanitize the sample
values, ask the model about each remaining column, and write the answer with the
evidence it was based on.

The destination table holds one current row per column. Rows are deleted by
column key before new ones are appended, so re-classifying a column replaces its
row.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from .config import ClassificationConfig
from .inputs import (
    ColumnKey,
    PingKey,
    TableKey,
    client_for,
    load_already_classified,
    load_columns,
    load_ping_mapping,
    load_probes_by_ping,
)
from .models import VertexModel, model_for
from .prompt import (
    MAX_SAMPLE_VALUES,
    build_classification_prompt,
    find_matching_probes,
    find_metric_probes,
)
from .sanitize import (
    DROP,
    MASK,
    Sanitizer,
    sanitize_columns,
    sanitizer_for,
    suppress_high_cardinality_samples,
)
from .taxonomy import load_taxonomy, taxonomy_prompt_block, taxonomy_version

# Rows are written in batches rather than once per table, so an aborted run keeps
# the columns it already classified. The next run sees them as done and starts
# where this one stopped.
WRITE_BATCH_SIZE = 200

MAX_CONSECUTIVE_FAILURES = 10

# Extra passes over the columns that failed, run at the end of the table.
MAX_ANSWER_RETRIES = 2

# How many failed column names the final error message lists.
MAX_NAMED_FAILURES = 5

TargetKey = tuple[str, str, str | None]  # (project, dataset, table or None)

WorkItem = tuple[dict[str, Any], list[dict[str, Any]]]  # (column, its probes)

# Clears these columns' existing rows so the append that follows replaces them.
# Not filtered by model on purpose, so switching models replaces a column's row
# instead of adding a second one.
_DELETE_QUERY = """
DELETE FROM `{destination_table}`
WHERE source_project = @project
  AND source_dataset = @dataset
  AND source_table = @table
  AND column_name IN UNNEST(@columns)
"""


class ModelUnavailableError(RuntimeError):
    """The model failed often enough in a row to treat as unavailable.

    Carries the abandoned pass's failed items, which never reach its caller by
    return.
    """

    def __init__(self, message: str, failed: list[WorkItem]):
        """Hold the message and the items the abandoned pass had failed on."""
        super().__init__(message)
        self.failed = failed


@dataclass
class RunSummary:
    """Counts describing what one run did."""

    tables: int = 0
    classified: int = 0
    skipped: int = 0
    dropped_metrics: int = 0
    failed_column_names: list[str] = field(default_factory=list)
    failed_targets: list[str] = field(default_factory=list)

    @property
    def failed_columns(self) -> int:
        """How many columns are still unclassified."""
        return len(self.failed_column_names)


@dataclass
class RunContext:
    """What every step of a run needs: its clients, the taxonomy, the counts."""

    config: ClassificationConfig
    client: bigquery.Client
    model: VertexModel
    sanitizer: Sanitizer | None
    taxonomy_json: str
    labels: frozenset[str]
    taxonomy_version: str
    refresh: bool
    summary: RunSummary
    consecutive_failures: int = 0


def _sanitized_samples(column: dict[str, Any], sanitized: bool) -> list[str]:
    """Return the sample values the model was shown and the row may keep.

    An unsanitized run keeps none: the destination promises DLP-sanitized values
    only, and it is unpartitioned and never expires.

    Otherwise this follows the same rule as the prompt's profile block, cap
    included. A column can carry up to 50 stored values while only
    MAX_SAMPLE_VALUES are rendered, so recording all of them would claim the
    model saw values it never saw.
    """
    if not sanitized:
        return []
    if column.get("is_glean_metric"):
        return []
    values = column.get("values") or []
    if values:
        return [v for v, _f in values[:MAX_SAMPLE_VALUES] if v is not None]
    example = column.get("example_value")
    return [example] if example is not None else []


def _answer_fields(
    result: Any, labels: frozenset[str], column_name: str
) -> dict[str, Any]:
    """Validate and normalize the model's parsed answer into the row's fields.

    Raises ValueError if the answer is not an object, if it carries no
    primary_label, or if a field carries a type its destination column cannot
    take. Writes are batched, so one malformed answer would otherwise fail the
    load job for the good rows beside it.

    A label the taxonomy does not contain is kept as the model gave it and
    flagged with needs_review. The reasoning is still what a reviewer needs, and
    dropping the row would only have the column classified the same way again on
    the next run.
    """
    if not isinstance(result, dict):
        raise ValueError(f"model answer is not a JSON object: {type(result).__name__}")

    fields: dict[str, Any] = {}
    for name in ("primary_label", "confidence", "reasoning"):
        value = result.get(name)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{name} is not a string: {value!r}")
        fields[name] = value

    # No label is a malformed answer rather than a verdict: the taxonomy has a
    # system branch for non-personal data, so the model always has somewhere to
    # land, and a NULL-label row would be skipped by every later run.
    if not fields["primary_label"]:
        raise ValueError("model answer carries no primary_label")

    secondary = result.get("secondary_labels") or []
    if not isinstance(secondary, list) or not all(
        isinstance(label, str) for label in secondary
    ):
        raise ValueError(f"secondary_labels is not a list of strings: {secondary!r}")
    fields["secondary_labels"] = secondary

    needs_review = result.get("needs_review")
    if needs_review is not None and not isinstance(needs_review, bool):
        raise ValueError(f"needs_review is not a boolean: {needs_review!r}")

    returned = [fields["primary_label"]] + secondary
    unknown = [label for label in returned if label not in labels]
    if unknown:
        logging.warning(
            "Label(s) outside the taxonomy for %s, flagging for review: %s",
            column_name,
            ", ".join(unknown),
        )
        needs_review = True
    # Otherwise the model's own value, so None ("did not say") stays distinct
    # from False ("said no").
    fields["needs_review"] = needs_review
    return fields


def _record(
    ctx: RunContext,
    key: TableKey,
    column: dict[str, Any],
    probes: list[dict[str, Any]],
    answer: dict[str, Any],
) -> dict[str, Any]:
    """Build the destination row for one classified column.

    evidence is written even when every statistic is None, which is the case for
    a Glean metric column. input_hash is not populated by any run yet.
    """
    project, dataset, table = key
    probe = probes[0] if probes else None
    return {
        # Stamped per record, not once per run: a run can take hours, and run_id
        # is what groups its rows.
        "classified_at": datetime.now(timezone.utc).isoformat(),
        "run_id": ctx.config.run_id,
        "source_project": project,
        "source_dataset": dataset,
        "source_table": table,
        "column_name": column["column_name"],
        "data_type": column.get("data_type"),
        **answer,
        "matched_probe": probe["probe_name"] if probe else None,
        "data_sensitivity": probe.get("data_sensitivity") or [] if probe else [],
        "model": ctx.config.model,
        "taxonomy_version": ctx.taxonomy_version,
        "evidence": {
            "null_rate": column.get("null_rate"),
            "distinct_count": column.get("distinct_count"),
            "is_high_cardinality": column.get("is_high_cardinality"),
            "column_tier": column.get("column_tier"),
            "existing_description": column.get("bq_description"),
            "sanitized_samples": _sanitized_samples(column, ctx.sanitizer is not None),
        },
    }


def _write(ctx: RunContext, key: TableKey, records: list[dict[str, Any]]) -> None:
    """Replace the batch's rows in the destination table: delete, then append."""
    if not records:
        return
    project, dataset, table = key
    columns = [record["column_name"] for record in records]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("project", "STRING", project),
            bigquery.ScalarQueryParameter("dataset", "STRING", dataset),
            bigquery.ScalarQueryParameter("table", "STRING", table),
            bigquery.ArrayQueryParameter("columns", "STRING", columns),
        ]
    )
    query = _DELETE_QUERY.format(destination_table=ctx.config.destination_table)
    ctx.client.query(query, job_config=job_config).result()

    ctx.client.load_table_from_json(
        records,
        ctx.config.destination_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        ),
    ).result()
    logging.info("  wrote %s rows for %s.%s", len(records), dataset, table)


def _probes_for(
    column: dict[str, Any], ping_probes: list[dict[str, Any]]
) -> list[dict[str, Any]] | None:
    """Return the probes to show for a column, or None not to classify it.

    An ordinary column is classified from its own statistics, with or without a
    matching probe. A Glean metric column (metrics.<type>.<name>) has no
    statistics, since the profiler never descends into the metrics STRUCT, and
    usually no description either. Without a probe the model would see only a
    name and a type, so its answer would be a guess that every later run then
    skips as already classified.
    """
    column_name = column["column_name"]
    if not column.get("is_glean_metric"):
        return find_matching_probes(column_name, ping_probes)
    probes = find_metric_probes(column_name, ping_probes)
    if not probes:
        logging.debug("Dropping metric %s: no exact probe match", column_name)
        return None
    return probes


def _record_failures(summary: RunSummary, key: TableKey, items: list[WorkItem]) -> None:
    """Count columns that are still unclassified, named for the final report."""
    _project, dataset, table = key
    summary.failed_column_names.extend(
        f"{dataset}.{table}.{column['column_name']}" for column, _probes in items
    )


def _classify_items(
    ctx: RunContext, key: TableKey, work_items: list[WorkItem]
) -> list[WorkItem]:
    """Classify one pass over the work items and return the ones that failed.

    Does not count failures: a column that fails here may succeed on a later
    pass, so the caller counts what is still failing once the passes are done.
    """
    _project, dataset, table = key
    records: list[dict[str, Any]] = []
    failed: list[WorkItem] = []
    try:
        for column, probes in work_items:
            column_name = column["column_name"]
            # One line per column is verbose, but it is the only way to tell a
            # slow run from a stuck one.
            logging.info("  %s -> %s probe candidates", column_name, len(probes))
            # A column the prompt cannot render costs that column rather than the
            # whole dataset, and says nothing about the model, so it is caught
            # apart from the call and does not count towards the breaker.
            try:
                prompt = build_classification_prompt(
                    column=column,
                    table=f"{dataset}.{table}",
                    probes=probes,
                    taxonomy_json=ctx.taxonomy_json,
                )
            except Exception as exc:
                logging.error("Could not build a prompt for %s: %s", column_name, exc)
                failed.append((column, probes))
                continue
            try:
                answer = _answer_fields(
                    ctx.model.generate_json(prompt), ctx.labels, column_name
                )
            except Exception as exc:
                logging.error("Classification failed for %s: %s", column_name, exc)
                failed.append((column, probes))
                ctx.consecutive_failures += 1
                # The column is retried at the end of the table, so one failure
                # costs nothing. This counter is for a model that is down: bad
                # credentials would otherwise walk a whole table against a dead
                # endpoint and finish green with no rows.
                if ctx.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    raise ModelUnavailableError(
                        f"{ctx.config.model} failed {ctx.consecutive_failures} "
                        "times in a row, abandoning the run",
                        failed,
                    ) from exc
                continue
            ctx.consecutive_failures = 0
            records.append(_record(ctx, key, column, probes, answer))
            ctx.summary.classified += 1
            if len(records) >= WRITE_BATCH_SIZE:
                _write(ctx, key, records)
                records.clear()
    except BaseException:
        # Keep the columns classified since the last flush, rather than
        # discarding the work the breaker fired to protect. Best effort: a write
        # failure here must not replace the exception on its way out, which on
        # the breaker path is what stops the run.
        try:
            _write(ctx, key, records)
        except Exception:
            logging.exception(
                "Could not flush %s rows for %s.%s", len(records), dataset, table
            )
        raise
    _write(ctx, key, records)
    return failed


def _classify_table(
    ctx: RunContext,
    key: TableKey,
    columns: list[dict[str, Any]],
    already: set[ColumnKey],
    ping: str | None,
    ping_probes: list[dict[str, Any]],
) -> None:
    """Classify one table's pending columns and write them in batches."""
    project, dataset, table = key

    work_items: list[WorkItem] = []
    dropped = 0
    for column in columns:
        if (project, dataset, table, column["column_name"]) in already:
            ctx.summary.skipped += 1
            continue
        probes = _probes_for(column, ping_probes)
        if probes is None:
            dropped += 1
        else:
            work_items.append((column, probes))
    ctx.summary.dropped_metrics += dropped

    pending = len(work_items) + dropped
    if not pending:
        logging.info(
            "Skipping %s.%s - all %s columns already classified",
            dataset,
            table,
            len(columns),
        )
        return
    logging.info(
        "--- %s.%s | ping: %s | %s probes | %s columns | %s metrics dropped ---",
        dataset,
        table,
        ping or "none",
        len(ping_probes),
        pending,
        dropped,
    )

    # Suppression runs first, so DLP is never asked about a value that is about
    # to be dropped. Both mutate the column dicts in place.
    work_columns = [column for column, _probes in work_items]
    suppressed = suppress_high_cardinality_samples(work_columns)
    if suppressed:
        logging.info("  suppressed samples for %s high-cardinality columns", suppressed)
    if ctx.sanitizer is not None:
        changes = sanitize_columns(ctx.sanitizer, work_columns)
        if changes:
            # The change records hold no sample values, so they are safe to log.
            logging.info(
                "  sanitized samples: %s masked, %s dropped across %s columns",
                sum(1 for c in changes if c["action"] == MASK),
                sum(1 for c in changes if c["action"] == DROP),
                len({c["column"] for c in changes}),
            )
            for change in changes:
                logging.debug("  sanitized %s", change)

    # Sweeping at the end of the table rather than retrying a column on the spot:
    # a second call milliseconds later meets the same conditions, and the rest of
    # the table is enough spacing without a sleep. Failures are counted once the
    # passes are done, so a column that failed twice and then succeeded is not
    # counted at all.
    failed = work_items
    try:
        for attempt in range(1 + MAX_ANSWER_RETRIES):
            if attempt:
                logging.info(
                    "  retrying %s failed columns (pass %s)", len(failed), attempt + 1
                )
            failed = _classify_items(ctx, key, failed)
            if not failed:
                return
    except ModelUnavailableError as exc:
        _record_failures(ctx.summary, key, exc.failed)
        raise
    _record_failures(ctx.summary, key, failed)


def _classify_target(
    ctx: RunContext,
    project: str,
    dataset: str,
    table: str | None,
    ping_mapping: dict[TableKey, dict[str, Any]],
    probes_by_ping: dict[PingKey, list[dict[str, Any]]],
) -> None:
    """Classify one target: a whole dataset, or a single table in it."""
    columns_by_table = load_columns(ctx.client, ctx.config, project, dataset, table)
    # Skipped entirely under refresh, rather than called and ignored, which
    # saves a query.
    already = (
        set()
        if ctx.refresh
        else load_already_classified(ctx.client, ctx.config, project, dataset, table)
    )
    # Sorted so two runs log their tables in the same order and can be diffed.
    for key, columns in sorted(columns_by_table.items()):
        # A table whose lineage found no ping is present with both fields None,
        # so check the ping before looking up its probes.
        ping = ping_mapping.get(key) or {}
        source_ping = ping.get("source_ping")
        ping_probes = (
            probes_by_ping.get((ping["ping_platform"], source_ping), [])
            if source_ping
            else []
        )
        ctx.summary.tables += 1
        _classify_table(ctx, key, columns, already, source_ping, ping_probes)


def _log_summary(ctx: RunContext) -> None:
    """Log what the run did and what it spent."""
    summary = ctx.summary
    logging.info(
        "Run %s: %s tables, %s columns classified, %s skipped, "
        "%s metrics dropped, %s columns failed",
        ctx.config.run_id,
        summary.tables,
        summary.classified,
        summary.skipped,
        summary.dropped_metrics,
        summary.failed_columns,
    )
    totals = ctx.model.totals
    logging.info(
        "TOKENS model=%s calls=%s input_tokens=%s output_tokens=%s",
        ctx.config.model,
        totals.calls,
        totals.input_tokens,
        totals.output_tokens,
    )


def _failure_message(summary: RunSummary) -> str:
    """Describe what the run did not finish, naming the first few columns."""
    problems = []
    if summary.failed_targets:
        problems.append(
            f"{len(summary.failed_targets)} targets failed "
            f"({', '.join(summary.failed_targets)})"
        )
    if summary.failed_columns:
        named = summary.failed_column_names[:MAX_NAMED_FAILURES]
        rest = len(summary.failed_column_names) - len(named)
        listed = ", ".join(named) + (f", and {rest} more" if rest else "")
        problems.append(f"{summary.failed_columns} columns unclassified ({listed})")
    return "; ".join(problems)


def run(
    config: ClassificationConfig,
    targets: list[TargetKey],
    *,
    refresh: bool = False,
) -> RunSummary:
    """Classify the given targets and return what the run did.

    Each target is a (project, dataset, table or None) triple, already parsed.
    With refresh, columns an earlier run classified are done again rather than
    skipped.

    A failing target is logged and recorded, then the run moves to the next one:
    a read fault on one dataset should not cost the others their model calls.
    Raises at the end if any target failed or any column is still unclassified,
    so the task goes red while keeping every row it did write.
    """
    taxonomy = load_taxonomy()
    ctx = RunContext(
        config=config,
        client=client_for(config),
        model=model_for(config),
        sanitizer=sanitizer_for(config),
        taxonomy_json=taxonomy_prompt_block(taxonomy),
        labels=frozenset(entry["label"] for entry in taxonomy),
        taxonomy_version=taxonomy_version(),
        refresh=refresh,
        summary=RunSummary(),
    )
    summary = ctx.summary
    logging.info(
        "Classifying %s targets with %s against taxonomy %s (%s labels)",
        len(targets),
        config.model,
        ctx.taxonomy_version,
        len(ctx.labels),
    )

    ping_mapping = load_ping_mapping(ctx.client, config)
    probes_by_ping = load_probes_by_ping(ctx.client, config)

    try:
        for project, dataset, table in targets:
            try:
                _classify_target(
                    ctx, project, dataset, table, ping_mapping, probes_by_ping
                )
            except ModelUnavailableError:
                # The next target would fail the same way.
                raise
            except Exception:
                logging.exception("Target %s.%s failed", project, dataset)
                summary.failed_targets.append(f"{project}.{dataset}")
    finally:
        # In a finally so that an abandoned run still reports what it did and
        # spent, which is when that is most worth knowing.
        _log_summary(ctx)

    if summary.failed_targets or summary.failed_columns:
        raise RuntimeError(_failure_message(summary))
    return summary
