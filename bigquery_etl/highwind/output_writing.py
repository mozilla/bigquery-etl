"""SECTION 5: OUTPUT WRITING.

Two destinations, for two different readers. BigQuery keeps the corpus queryable across experiments,
which is what makes cross-experiment context and meta-analysis possible and lets a result be
inspected without opening a blob. GCS is the seam Experimenter reads, matching where the enrollment
funnel already writes from this same Airflow.
"""

import datetime
import json
import pathlib
from dataclasses import dataclass

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Production targets, the defaults an `Outputs` takes when nothing overrides them.
RESULTS_TABLE = "moz-fx-data-experiments.monitoring.highwind_statistics_v1"
SUFFICIENT_STATS_TABLE = (
    "moz-fx-data-experiments.monitoring.highwind_sufficient_stats_v1"
)
BLOB_PREFIX = "gs://mozanalysis/highwind"
PIPELINE_VERSION = "poc-1"


@dataclass(frozen=True)
class Outputs:
    """Where one run's three outputs go.

    An argument rather than module state, so a writer's behaviour is a function of what it was
    called with. A local run overrides the tables and sends blobs to a directory instead of GCS.
    """

    results_table: str = RESULTS_TABLE
    sufficient_stats_table: str = SUFFICIENT_STATS_TABLE
    blob_prefix: str = BLOB_PREFIX
    # Set for a local run: blobs go to this directory instead of to GCS.
    local_blob_dir: str | None = None


def write_blob(storage, experiment, as_of, results, outputs):
    """Write the per-experiment JSON Experimenter ingests.

    Written per experiment, unlike the tables below, because the blob IS per experiment and its name
    is the key: rewriting `<slug>.json` replaces the previous run's answer, so this leg is idempotent
    without any extra machinery.

    Carries `generated_at` and `pipeline_version` in the header and as object metadata, so the
    ingest can tell a new run from an old one without downloading the blob.
    """
    # When this ran, not the date it analysed: two runs of the same date, a retry or a backfill,
    # have to be distinguishable, which is the whole purpose of the field.
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload = {
        "metrics_meta": {
            "slug": experiment.slug,
            "as_of_date": as_of.isoformat(),
            "generated_at": generated_at,
            "pipeline_version": PIPELINE_VERSION,
            "reference_branch": experiment.reference_branch,
            "branches": list(experiment.branches),
        },
        "statistics": results,
        "errors": [result for result in results if result["state"] == "error"],
    }
    if outputs.local_blob_dir:
        path = pathlib.Path(outputs.local_blob_dir) / f"{experiment.slug}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))
        return str(path)
    blob = blob_for(storage, experiment.slug, outputs.blob_prefix)
    blob.metadata = {
        "as_of_date": as_of.isoformat(),
        "generated_at": generated_at,
        "pipeline_version": PIPELINE_VERSION,
    }
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    return f"{outputs.blob_prefix}/{experiment.slug}.json"


def write_tables(client, as_of, results_by_slug, cells_by_slug, outputs):
    """Both BigQuery tables for the whole run, in one write each.

    Once per run rather than once per experiment, and that is what makes a rerun safe. Every
    experiment's rows land in the same daily partition, so a per-experiment write can only append,
    and appending means a retried or backfilled run duplicates every row it already wrote. Airflow
    retries as a matter of course, so this would not be a rare case. Writing the run's rows together
    lets the load replace the partition instead, which is idempotent by construction.

    It also turns two load jobs per experiment into two for the whole run.
    """
    results_rows = [
        dict(
            result,
            slug=slug,
            as_of_date=as_of.isoformat(),
            pipeline_version=PIPELINE_VERSION,
        )
        for slug, results in results_by_slug.items()
        for result in results
    ]
    stats_rows = [
        dict(
            stats,
            slug=slug,
            as_of_date=as_of.isoformat(),
            metric=metric,
            window=window,
            branch=branch,
        )
        for slug, cells in cells_by_slug.items()
        for (metric, window, branch), stats in cells.items()
    ]
    replace_partition(client, outputs.results_table, results_rows, as_of)
    replace_partition(client, outputs.sufficient_stats_table, stats_rows, as_of)
    return len(results_rows), len(stats_rows)


def replace_partition(client, table, rows, as_of):
    """Write `rows` as the entire contents of this run date's partition.

    Replacing the partition rather than appending to it is what makes the job safe to rerun: the
    second run of a date produces the same table as the first, where appending would double it. A
    run with no rows leaves the partition untouched rather than emptying it, so a failed rerun
    cannot destroy a good result.

    Deployed, the table already exists with the schema declared in its schema.yaml, so the load
    writes a partition of it and inherits that schema. `autodetect` is switched OFF explicitly for
    that path: the client turns it on by itself for a WRITE_TRUNCATE with no schema, and inferring
    a schema per run is how a column changes type between days. That is a live risk here rather
    than a theoretical one, because a cell in the `error` or `not_started` state omits `point`,
    `lower`, `upper` and `theta` entirely, so which columns appear at all varies with the day's mix
    of states.

    The create branch exists for a local run against a table nothing has deployed, where inference
    is the only option and the partitioning has to be declared here instead.
    """
    if not rows:
        return
    try:
        client.get_table(table)
        target, config = f"{table}${as_of.strftime('%Y%m%d')}", bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
        )
    except NotFound:
        target, config = table, bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(field="as_of_date"),
        )
    client.load_table_from_json(rows, target, job_config=config).result()


def blob_for(storage, slug, blob_prefix):
    """Resolve one experiment's blob from a gs:// prefix, with or without a path component."""
    bucket_name, _, prefix = blob_prefix.removeprefix("gs://").partition("/")
    key = f"{prefix}/{slug}.json" if prefix else f"{slug}.json"
    return storage.bucket(bucket_name).blob(key)


def state_counts(results):
    """How many cells landed in each state, which is the operational-health signal per run."""
    counts = {}
    for result in results:
        counts[result["state"]] = counts.get(result["state"], 0) + 1
    return counts
