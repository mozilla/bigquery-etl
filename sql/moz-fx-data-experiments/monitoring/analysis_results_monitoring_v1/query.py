#!/usr/bin/env python3

"""Build the daily jetstream analysis success/failure rollup.

For a given date, records one row per (experiment, analysis_period,
window_index, analysis_basis, segment, metric) cell that jetstream either
successfully computed (found in that experiment's `statistics_*` tables) or
failed to compute (found in `monitoring.jetstream_analysis_errors`).
`window_index` is NULL when the failure that produced a row didn't identify
a specific window (most failures don't -- see FAILED_SQL).

Table names under `mozanalysis` don't map 1:1 to a single experiment slug
without normalizing hyphens to underscores (`jetstream.bq_normalize_name`),
so the set of tables written on the target date is discovered via
`INFORMATION_SCHEMA.JOBS_BY_PROJECT`, matched back to experiment slugs
primarily via each table's description (falling back to matching the table
name when a description is missing), and unioned into a single query
alongside the failure view.

`--date` is jetstream's analysis date, matching its own `--date={{ds}}`
argument. Both it and this task actually run a day later, so table/log
timestamps are filtered on `date + 1`, not `date` itself.

Scheduled to run after telemetry-airflow's `jetstream` DAG completes for the
same analysis date (see metadata.yaml's `depends_on`).

Backfillable, with one caveat: JOBS_BY_PROJECT is historical, so discovery
is accurate for old dates, but if a table's content changes after the
target date (e.g. a `rerun`/`rerun-config-changed` recomputing an
experiment's history), a backfill reads today's content, not the target
date's -- BigQuery has no way to recover the old rows past its few-day
time-travel window. Tables written once and never touched again (the
common case) aren't affected.
"""

from argparse import ArgumentParser
from datetime import UTC, date, datetime

from google.cloud import bigquery

PROJECT = "moz-fx-data-experiments"
DATASET = "monitoring"
STATS_DATASET = "mozanalysis"
DESTINATION_TABLE = "analysis_results_monitoring_v1"

# Safety margin below BigQuery's 1,000-referenced-resource-per-query cap,
# which build_final_sql approaches as one reference per discovered table.
# Peak observed over 180 days is 314.
MAX_DISCOVERED_TABLES = 900

# JOBS_BY_PROJECT retains 180 days. Past that, table discovery silently
# returns zero rows -- indistinguishable from "nothing was written that
# day" -- so a backfill this old must fail loudly instead of reporting a
# false 0% success rate.
MAX_BACKFILL_AGE_DAYS = 180

# error_category values (see jetstream_analysis_errors/view.sql) that represent
# an actual failure to compute something. Excludes `skipped` (the experiment
# was intentionally not analyzed), `valid_degradation`/`library_warning`
# (benign; the metric was still computed), `informational`, and `uncategorized`
# (ambiguous by construction -- see that view's comments).
FAILURE_CATEGORIES = ["resources", "data", "configuration", "jetstream_failure"]

# valid jetstream.AnalysisPeriod values, as they appear in table name suffixes.
PERIOD_TOKENS = [
    "preenrollment_week",
    "preenrollment_days28",
    "days28",
    "week",
    "overall",
    "day",
]

# jetstream's DAG runs daily at 04:00 UTC, so a run for `@date` produces
# rows between 04:00 UTC on `@date + 1` and 04:00 UTC on `@date + 2`.
WINDOW_START = (
    "TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(@date, INTERVAL 1 DAY)), INTERVAL 4 HOUR)"
)
WINDOW_END = (
    "TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(@date, INTERVAL 2 DAY)), INTERVAL 4 HOUR)"
)

DISCOVER_TABLES_SQL = f"""
WITH discovered_tables AS (
  -- JOBS_BY_PROJECT is a historical, per-job record (180-day retention),
  -- unlike INFORMATION_SCHEMA.TABLES/TABLE_STORAGE, which only ever reflect
  -- a table's current state -- querying those for a past date silently
  -- misses any table rewritten since.
  SELECT DISTINCT destination_table.table_id AS table_name
  FROM `{PROJECT}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  WHERE destination_table.project_id = '{PROJECT}'
    AND destination_table.dataset_id = '{STATS_DATASET}'
    AND STARTS_WITH(destination_table.table_id, 'statistics_')
    AND job_type = 'LOAD'
    AND error_result IS NULL
    AND creation_time >= {WINDOW_START}
    AND creation_time < {WINDOW_END}
),
-- jetstream sets each table's description to the exact experiment slug.
table_experiments AS (
  -- NULLIF: an empty (not missing) description must also fall through to
  -- the name-matching fallback below, not resolve to experiment = ''.
  SELECT table_name, NULLIF(TRIM(option_value, '"'), '') AS experiment
  FROM `{PROJECT}.{STATS_DATASET}.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE option_name = 'description'
),
-- A table can lack a description (https://github.com/mozilla/jetstream/issues/3299);
-- those fall back to matching the table name against known experiment
-- slugs, resolving ambiguous prefix matches (e.g. "foo" vs "foo-bar") by
-- preferring the longest slug.
normalized_experiments AS (
  SELECT
    normandy_slug AS experiment,
    REGEXP_REPLACE(normandy_slug, r'[^a-zA-Z0-9_]', '_') AS normalized_slug
  FROM `{PROJECT}.{DATASET}.experimenter_experiments_v1`
  WHERE normandy_slug IS NOT NULL
),
fallback_matched AS (
  SELECT
    d.table_name,
    e.experiment,
    ROW_NUMBER() OVER (
      PARTITION BY d.table_name ORDER BY LENGTH(e.normalized_slug) DESC
    ) AS rn
  FROM discovered_tables d
  LEFT JOIN table_experiments t USING (table_name)
  JOIN normalized_experiments e
    ON STARTS_WITH(d.table_name, CONCAT('statistics_', e.normalized_slug, '_'))
  WHERE t.experiment IS NULL
),
table_experiments_resolved AS (
  SELECT table_name, experiment, 'description' AS matched_via FROM table_experiments
  UNION ALL
  SELECT table_name, experiment, 'fallback' AS matched_via FROM fallback_matched WHERE rn = 1
),
with_period_window AS (
  SELECT
    d.table_name,
    e.experiment,
    e.matched_via,
    SUBSTR(
      d.table_name,
      LENGTH('statistics_' || REGEXP_REPLACE(e.experiment, r'[^a-zA-Z0-9_]', '_') || '_') + 1
    ) AS period_window
  FROM discovered_tables d
  JOIN table_experiments_resolved e USING (table_name)
)
SELECT
  table_name,
  experiment,
  matched_via,
  REGEXP_REPLACE(period_window, r'_\\d+$', '') AS analysis_period,
  CAST(REGEXP_EXTRACT(period_window, r'_(\\d+)$') AS INT64) AS window_index
FROM with_period_window
WHERE REGEXP_CONTAINS(period_window, r'^({"|".join(PERIOD_TOKENS)})_\\d+$')
"""

# `analysis_period` on failed rows is sometimes bare (e.g. "day", logged by
# statistics.py -- window_index is then NULL, same "unattributed" semantics
# as a NULL analysis_basis/segment/metric) and sometimes period+window-index
# (e.g. "day_7", logged by calculate_metrics/calculate_metric_for_ds).
FAILED_SQL = f"""
SELECT
  * EXCEPT (timestamp)
FROM (
  SELECT
    @date AS analysis_date,
    experiment,
    REGEXP_REPLACE(analysis_period, r'_\\d+$', '') AS analysis_period,
    CAST(REGEXP_EXTRACT(analysis_period, r'_(\\d+)$') AS INT64) AS window_index,
    analysis_basis,
    segment,
    metric,
    'failed' AS status,
    error_category,
    timestamp
  FROM `{PROJECT}.{DATASET}.jetstream_analysis_errors`
  WHERE source = 'jetstream'
    AND experiment IS NOT NULL
    AND timestamp >= {WINDOW_START}
    AND timestamp < {WINDOW_END}
    AND error_category IN ({", ".join(f"'{c}'" for c in FAILURE_CATEGORIES)})
)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY experiment, analysis_period, window_index, analysis_basis, segment, metric
  ORDER BY timestamp DESC
) = 1
"""


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


CURRENTLY_EXISTS_SQL = f"""
SELECT table_name
FROM `{PROJECT}.{STATS_DATASET}.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE' AND table_name IN UNNEST(@table_names)
"""

RAW_DISCOVERED_COUNT_SQL = f"""
SELECT COUNT(DISTINCT destination_table.table_id) AS n
FROM `{PROJECT}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE destination_table.project_id = '{PROJECT}'
  AND destination_table.dataset_id = '{STATS_DATASET}'
  AND STARTS_WITH(destination_table.table_id, 'statistics_')
  AND job_type = 'LOAD'
  AND error_result IS NULL
  AND creation_time >= {WINDOW_START}
  AND creation_time < {WINDOW_END}
"""


def discover_statistics_tables(
    client: bigquery.Client, date: str
) -> list[bigquery.Row]:
    """Find the statistics_* tables written on `date`, matched to experiments.

    A table found via JOBS_BY_PROJECT may have since been deleted (it's a
    historical record, so this is expected for old-enough backfills, not a
    bug), which would otherwise crash the whole day's query with "Not found"
    when build_succeeded_sql tries to read it. Tables missing today are
    dropped and reported rather than silently skipped.
    """
    job = client.query(
        DISCOVER_TABLES_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", date)]
        ),
    )
    tables = list(job.result())

    raw_count = next(
        iter(
            client.query(
                RAW_DISCOVERED_COUNT_SQL,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("date", "DATE", date)
                    ]
                ),
            ).result()
        )
    ).n
    unmatched = raw_count - len({row.table_name for row in tables})
    if unmatched:
        print(
            f"{unmatched} table(s) matched neither a description nor a "
            "known experiment slug -- excluded from this day's rollup"
        )
    fallback_count = sum(1 for row in tables if row.matched_via == "fallback")
    if fallback_count:
        print(f"{fallback_count} table(s) had no description; matched by name instead")

    if not tables:
        return tables

    exists_job = client.query(
        CURRENTLY_EXISTS_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "table_names", "STRING", [row.table_name for row in tables]
                )
            ]
        ),
    )
    existing_names = {row.table_name for row in exists_job.result()}

    missing = [row.table_name for row in tables if row.table_name not in existing_names]
    if missing:
        print(f"Skipping {len(missing)} table(s) no longer present: {missing}")

    return [row for row in tables if row.table_name in existing_names]


def build_succeeded_sql(tables: list[bigquery.Row]) -> str:
    """Build a UNION ALL over the discovered tables' (metric, segment, basis).

    Each table is exactly one (experiment, analysis_period, window_index) --
    that's what its name encodes -- so window_index is a literal per subquery.
    """
    if not tables:
        return """
        SELECT
          CAST(NULL AS STRING) AS experiment,
          CAST(NULL AS STRING) AS analysis_period,
          CAST(NULL AS INT64) AS window_index,
          CAST(NULL AS STRING) AS analysis_basis,
          CAST(NULL AS STRING) AS segment,
          CAST(NULL AS STRING) AS metric
        WHERE FALSE
        """
    return "\nUNION ALL\n".join(f"""
        SELECT DISTINCT
          '{_escape(row.experiment)}' AS experiment,
          '{_escape(row.analysis_period)}' AS analysis_period,
          {row.window_index if row.window_index is not None else "CAST(NULL AS INT64)"} AS window_index,
          analysis_basis,
          segment,
          metric
        FROM `{PROJECT}.{STATS_DATASET}.{row.table_name}`
        """ for row in tables)


def build_final_sql(tables: list[bigquery.Row]) -> str:
    """Combine failed and succeeded into one table, one row per cell.

    A cell can have both a failure logged and output in the statistics table
    (e.g. one statistic/reference-branch errored, another didn't) -- such
    cells are labelled "partial" rather than picking a side.
    """
    succeeded_sql = build_succeeded_sql(tables)
    return f"""
    WITH failed AS (
      {FAILED_SQL}
    ),
    succeeded AS (
      SELECT
        @date AS analysis_date,
        experiment,
        analysis_period,
        window_index,
        analysis_basis,
        segment,
        metric,
        'succeeded' AS status
      FROM (
        {succeeded_sql}
      )
    )
    SELECT
      COALESCE(f.analysis_date, s.analysis_date) AS analysis_date,
      COALESCE(f.experiment, s.experiment) AS experiment,
      COALESCE(f.analysis_period, s.analysis_period) AS analysis_period,
      COALESCE(f.window_index, s.window_index) AS window_index,
      COALESCE(f.analysis_basis, s.analysis_basis) AS analysis_basis,
      COALESCE(f.segment, s.segment) AS segment,
      COALESCE(f.metric, s.metric) AS metric,
      CASE
        WHEN f.status IS NOT NULL AND s.status IS NOT NULL THEN 'partial'
        WHEN f.status IS NOT NULL THEN 'failed'
        ELSE 'succeeded'
      END AS status,
      f.error_category
    FROM failed f
    FULL OUTER JOIN succeeded s
      ON f.experiment = s.experiment
      AND f.analysis_period = s.analysis_period
      AND f.window_index = s.window_index
      AND f.analysis_basis = s.analysis_basis
      AND f.segment = s.segment
      AND f.metric = s.metric
    """


def main():
    """Run."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--destination_dataset", default=DATASET)
    parser.add_argument("--destination_table", default=DESTINATION_TABLE)
    parser.add_argument(
        "--date", dest="date", required=True, help="Date to roll up (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    today = datetime.now(tz=UTC).date()
    backfill_age_days = (today - date.fromisoformat(args.date)).days
    if backfill_age_days > MAX_BACKFILL_AGE_DAYS:
        raise RuntimeError(
            f"--date={args.date} is {backfill_age_days} days old, past "
            f"JOBS_BY_PROJECT's {MAX_BACKFILL_AGE_DAYS}-day retention -- "
            "table discovery would silently find nothing."
        )

    client = bigquery.Client(project=args.project)

    tables = discover_statistics_tables(client, args.date)
    print(f"Discovered {len(tables)} statistics tables")
    if len(tables) > MAX_DISCOVERED_TABLES:
        raise RuntimeError(
            f"{len(tables)} tables discovered for {args.date}, over the "
            f"{MAX_DISCOVERED_TABLES} safety margin -- query would likely hit "
            "BigQuery's 1,000-referenced-resource limit. Suggested fix: "
            "implement chunking."
        )
    final_sql = build_final_sql(tables)

    destination = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
        f"${args.date.replace('-', '')}"
    )
    job = client.query(
        final_sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", args.date)],
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="analysis_date"
            ),
        ),
    )
    result = job.result()
    print(f"Wrote {result.total_rows} rows to {destination}")


if __name__ == "__main__":
    main()
