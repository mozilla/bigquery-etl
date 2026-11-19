#!/usr/bin/env python3

"""Build the daily jetstream analysis success/failure rollup.

One row per (experiment, analysis_period, window_index, analysis_basis,
segment, metric) cell for a given date. Cells come from what jetstream
produced that day -- rows in its `statistics_*` tables and in
`monitoring.jetstream_analysis_errors` -- plus the cells each experiment's
`metric_config` (`experiment_metric_configs_v1`) says those observed windows
should contain. Expected cells are anchored on observed windows, not a
schedule, since reruns and manual runs make "what should run today"
unknowable. Status is succeeded, partial, failed, skipped or missing (see
schema.yaml and build_final_sql). Log rows that don't identify a cell (e.g.
NULL segment) are spread onto the expected cells they cover; ones covering
none are kept as their own rows (see COVERS/EXACT).

Tables are discovered via `INFORMATION_SCHEMA.JOBS_BY_PROJECT` and matched
to slugs by table description (falling back to name), since `mozanalysis`
table names don't map 1:1 to slugs (`jetstream.bq_normalize_name`), then
unioned into a single query with the log view.

`--date` is jetstream's analysis date (its `--date={{ds}}`). Both jobs run
a day later, so table/log timestamps are filtered on `date + 1`. Reruns can
land after this task runs but inside that window, so scheduled runs pass
`--rebuild-previous-day` (skipped if that partition was built after its
window closed, e.g. in backfills). Runs after telemetry-airflow's `jetstream`
DAG (metadata.yaml `depends_on`).

Backfillable, with two caveats. Discovery is accurate for old dates, but a
table recomputed since (e.g. `rerun`) is read as it is today -- BigQuery's
time travel only reaches back a few days; untouched tables (the common case)
are fine. And `experiment_metric_configs_v1` has no history: it holds each
experiment's config as of its latest `first_updated`, so a backfill of a
date before a rerun (which moves that timestamp) or a resolver change
expects the newer metric set, surfacing as extra `missing` rows.
"""

from argparse import ArgumentParser
from datetime import UTC, date, datetime, time, timedelta

from google.cloud import bigquery

PROJECT = "moz-fx-data-experiments"
DATASET = "monitoring"
STATS_DATASET = "mozanalysis"
DESTINATION_TABLE = "analysis_results_monitoring_v1"
METRIC_CONFIGS_TABLE = "experiment_metric_configs_v1"

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

# error_category values that explain why a cell has no output without it being
# a failure: jetstream intentionally declined to compute it.
SKIP_CATEGORIES = ["insufficient_clients"]

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
JETSTREAM_RUN_HOUR_UTC = 4
WINDOW_START = (
    "TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(@date, INTERVAL 1 DAY)), "
    f"INTERVAL {JETSTREAM_RUN_HOUR_UTC} HOUR)"
)
WINDOW_END = (
    "TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(@date, INTERVAL 2 DAY)), "
    f"INTERVAL {JETSTREAM_RUN_HOUR_UTC} HOUR)"
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

# `analysis_period` on logged rows is bare ("day": statistics.py's cell-level
# sites, so window_index is NULL and the row applies to every window of that
# period observed that day) or period+window ("day_7": calculate_metrics/
# calculate_metric_for_ds, which omit segment). A failure outranks a skip
# logged for the same key.
LOGGED_SQL = f"""
SELECT
  *
FROM (
  SELECT
    experiment,
    REGEXP_REPLACE(analysis_period, r'_\\d+$', '') AS analysis_period,
    CAST(REGEXP_EXTRACT(analysis_period, r'_(\\d+)$') AS INT64) AS window_index,
    analysis_basis,
    segment,
    metric,
    IF(error_category IN ({", ".join(f"'{c}'" for c in SKIP_CATEGORIES)}), 'skipped', 'failed') AS status,
    error_category,
    exception_type,
    exception,
    timestamp
  FROM `{PROJECT}.{DATASET}.jetstream_analysis_errors`
  WHERE source = 'jetstream'
    AND experiment IS NOT NULL
    AND timestamp >= {WINDOW_START}
    AND timestamp < {WINDOW_END}
    AND error_category IN ({", ".join(f"'{c}'" for c in FAILURE_CATEGORIES + SKIP_CATEGORIES)})
)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY experiment, analysis_period, window_index, analysis_basis, segment, metric
  ORDER BY status = 'failed' DESC, timestamp DESC
) = 1
"""

# A logged row covers a cell when each of its non-NULL dimensions matches.
COVERS = """
  l.experiment = c.experiment
  AND (l.analysis_period IS NULL OR l.analysis_period = c.analysis_period)
  AND (l.window_index IS NULL OR l.window_index = c.window_index)
  AND (l.analysis_basis IS NULL OR l.analysis_basis = c.analysis_basis)
  AND (l.segment IS NULL OR l.segment = c.segment)
  AND (l.metric IS NULL OR l.metric = c.metric)
"""

# whether a logged row identifies a cell, up to its window: no jetstream log
# site records metric, segment, basis and window together, so requiring
# window_index here would make this unreachable
EXACT = """(
  l.analysis_period IS NOT NULL
  AND l.analysis_basis IS NOT NULL
  AND l.segment IS NOT NULL
  AND l.metric IS NOT NULL
)"""


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

    JOBS_BY_PROJECT is historical, so a table it lists may since have been
    deleted (expected for old backfills); reading it would fail the whole
    day's query with "Not found", so such tables are dropped and reported.
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
    `clients` is the basis/segment's identity count summed over branches, which
    jetstream writes (as 0) even when it then computes no statistics; NULL if
    the identity rows themselves are absent.
    """
    if not tables:
        return """
        SELECT
          CAST(NULL AS STRING) AS experiment,
          CAST(NULL AS STRING) AS analysis_period,
          CAST(NULL AS INT64) AS window_index,
          CAST(NULL AS STRING) AS analysis_basis,
          CAST(NULL AS STRING) AS segment,
          CAST(NULL AS STRING) AS metric,
          CAST(NULL AS FLOAT64) AS clients
        FROM (SELECT 1)
        WHERE FALSE
        """
    return "\nUNION ALL\n".join(f"""
        SELECT DISTINCT
          '{_escape(row.experiment)}' AS experiment,
          '{_escape(row.analysis_period)}' AS analysis_period,
          {row.window_index if row.window_index is not None else "CAST(NULL AS INT64)"} AS window_index,
          analysis_basis,
          segment,
          metric,
          SUM(IF(metric = 'identity' AND statistic = 'count', point, NULL))
            OVER (PARTITION BY analysis_basis, segment) AS clients
        FROM `{PROJECT}.{STATS_DATASET}.{row.table_name}`
        """ for row in tables)


def build_final_sql(tables: list[bigquery.Row]) -> str:
    """Resolve expected and observed cells into one row per cell.

    A cell with output and an exact-grain failure is "partial" (e.g. one
    statistic errored, another didn't); a broader log row never downgrades a
    cell with output. Precedence for cells without output: failed, then
    skipped (logged, or in a basis/segment with zero clients), then missing.
    """
    succeeded_sql = build_succeeded_sql(tables)
    return f"""
    WITH logged AS (
      {LOGGED_SQL}
    ),
    succeeded AS (
      SELECT DISTINCT *, TRUE AS has_output
      FROM (
        {succeeded_sql}
      )
    ),
    configs AS (
      SELECT normandy_slug AS experiment, metric_config
      FROM `{PROJECT}.{DATASET}.{METRIC_CONFIGS_TABLE}`
      WHERE metric_config IS NOT NULL AND metric_config.resolution_error IS NULL
    ),
    observed_windows AS (
      SELECT DISTINCT experiment, analysis_period, window_index FROM succeeded
      UNION DISTINCT
      SELECT DISTINCT experiment, analysis_period, window_index
      FROM logged
      WHERE analysis_period IS NOT NULL AND window_index IS NOT NULL
    ),
    expected AS (
      SELECT DISTINCT
        w.experiment,
        w.analysis_period,
        w.window_index,
        analysis_basis,
        segment,
        m.name AS metric,
        TRUE AS is_expected
      FROM observed_windows w
      JOIN configs c USING (experiment)
      CROSS JOIN UNNEST(c.metric_config.metrics) m
      CROSS JOIN UNNEST(m.analysis_bases) analysis_basis
      CROSS JOIN UNNEST(
        ARRAY_CONCAT(['all'], ARRAY(SELECT s.name FROM UNNEST(c.metric_config.segments) s))
      ) segment
      WHERE EXISTS (
        SELECT 1 FROM UNNEST(m.statistics) st, UNNEST(st.analysis_periods) p
        WHERE p = w.analysis_period
      )
    ),
    cells AS (
      SELECT *
      FROM expected
      FULL OUTER JOIN succeeded
        USING (experiment, analysis_period, window_index, analysis_basis, segment, metric)
    ),
    -- the single log row that best explains each cell: failures first, then
    -- exact-grain rows, then the latest
    resolved AS (
      SELECT
        c.*,
        l.status AS log_status,
        l.error_category,
        l.exception_type,
        l.exception,
        {EXACT} AS exact
      FROM cells c
      LEFT JOIN logged l
        ON {COVERS}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.experiment, c.analysis_period, c.window_index, c.analysis_basis, c.segment, c.metric
        ORDER BY l.status = 'failed' DESC, {EXACT} DESC, l.timestamp DESC
      ) = 1
    ),
    -- basis/segment groups jetstream ran with no clients: it writes a 0 identity
    -- count and silently computes nothing else, so their cells are skipped
    unpopulated AS (
      SELECT DISTINCT experiment, analysis_period, window_index, analysis_basis, segment
      FROM succeeded
      WHERE clients = 0
    ),
    cell_rows AS (
      SELECT
        * EXCEPT (error_category, exception_type, exception),
        CASE
          WHEN status = 'succeeded' THEN NULL
          -- zero clients is the degenerate case of the logged skip
          WHEN status = 'skipped' THEN COALESCE(error_category, 'insufficient_clients')
          ELSE error_category
        END AS error_category,
        IF(status = 'succeeded', NULL, exception_type) AS exception_type,
        IF(status = 'succeeded', NULL, exception) AS exception
      FROM (
        SELECT
          r.*,
          CASE
            WHEN r.has_output AND r.exact AND r.log_status = 'failed' THEN 'partial'
            WHEN r.has_output THEN 'succeeded'
            WHEN r.log_status IS NOT NULL THEN r.log_status
            WHEN u.experiment IS NOT NULL THEN 'skipped'
            ELSE 'missing'
          END AS status
        FROM resolved r
        LEFT JOIN unpopulated u
          ON u.experiment = r.experiment
          AND u.analysis_period = r.analysis_period
          AND u.window_index = r.window_index
          AND u.analysis_basis = r.analysis_basis
          AND u.segment = r.segment
      )
    ),
    -- logged rows no cell represents: they cover none (e.g. a failure before any
    -- window ran), or are non-exact and cover only cells with output
    unmatched_logged AS (
      SELECT l.*
      FROM logged l
      WHERE NOT EXISTS (
        SELECT 1 FROM cells c
        WHERE {COVERS} AND ({EXACT} OR NOT IFNULL(c.has_output, FALSE))
      )
    )
    SELECT
      @date AS analysis_date,
      experiment,
      analysis_period,
      window_index,
      analysis_basis,
      segment,
      metric,
      status,
      error_category,
      exception_type,
      exception,
      IF(experiment IN (SELECT experiment FROM configs), COALESCE(is_expected, FALSE), NULL)
        AS expected
    FROM cell_rows
    UNION ALL
    SELECT
      @date AS analysis_date,
      experiment,
      analysis_period,
      window_index,
      analysis_basis,
      segment,
      metric,
      status,
      error_category,
      exception_type,
      exception,
      IF(experiment IN (SELECT experiment FROM configs) AND {EXACT}, FALSE, NULL) AS expected
    FROM unmatched_logged l
    """


def partition_is_complete(client: bigquery.Client, args, run_date: str) -> bool:
    """Whether run_date's partition was last built after its window closed."""
    window_end = datetime.combine(
        date.fromisoformat(run_date) + timedelta(days=2),
        time(JETSTREAM_RUN_HOUR_UTC),
        tzinfo=UTC,
    )
    rows = client.query(
        f"""
        SELECT last_modified_time
        FROM `{args.destination_project}.{args.destination_dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = @table AND partition_id = @partition
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "table", "STRING", args.destination_table
                ),
                bigquery.ScalarQueryParameter(
                    "partition", "STRING", run_date.replace("-", "")
                ),
            ]
        ),
    ).result()
    return any(row.last_modified_time >= window_end for row in rows)


def run_for_date(client: bigquery.Client, args, run_date: str) -> None:
    """Build and write one analysis_date partition."""
    backfill_age_days = (
        datetime.now(tz=UTC).date() - date.fromisoformat(run_date)
    ).days
    if backfill_age_days > MAX_BACKFILL_AGE_DAYS:
        raise RuntimeError(
            f"--date={run_date} is {backfill_age_days} days old, past "
            f"JOBS_BY_PROJECT's {MAX_BACKFILL_AGE_DAYS}-day retention -- "
            "table discovery would silently find nothing."
        )

    tables = discover_statistics_tables(client, run_date)
    print(f"Discovered {len(tables)} statistics tables for {run_date}")
    if len(tables) > MAX_DISCOVERED_TABLES:
        raise RuntimeError(
            f"{len(tables)} tables discovered for {run_date}, over the "
            f"{MAX_DISCOVERED_TABLES} safety margin -- query would likely hit "
            "BigQuery's 1,000-referenced-resource limit. Suggested fix: "
            "implement chunking."
        )
    final_sql = build_final_sql(tables)

    destination = (
        f"{args.destination_project}.{args.destination_dataset}.{args.destination_table}"
        f"${run_date.replace('-', '')}"
    )
    job = client.query(
        final_sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", run_date)],
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="analysis_date"
            ),
            dry_run=args.dry_run,
        ),
    )
    if not args.dry_run:
        result = job.result()
        print(f"Wrote {result.total_rows} rows to {destination}")


def main():
    """Run."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        default=PROJECT,
        help="Billing project (no effect on query source/destination).",
    )
    parser.add_argument("--destination_project", default=PROJECT)
    parser.add_argument("--destination_dataset", default=DATASET)
    parser.add_argument("--destination_table", default=DESTINATION_TABLE)
    parser.add_argument(
        "--date", dest="date", required=True, help="Date to roll up (YYYY-MM-DD)"
    )
    parser.add_argument("--dry_run", "--dry-run", action="store_true")
    parser.add_argument(
        "--rebuild-previous-day",
        "--rebuild_previous_day",
        action="store_true",
        help=(
            "Also rebuild date - 1 to pick up reruns that landed after its last "
            "build; skipped if that partition was already built after its window closed."
        ),
    )
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    dates = [args.date]
    if args.rebuild_previous_day:
        previous = (date.fromisoformat(args.date) - timedelta(days=1)).isoformat()
        if partition_is_complete(client, args, previous):
            print(
                f"Skipping {previous}: partition already built after its window closed"
            )
        else:
            dates.insert(0, previous)
    for run_date in dates:
        run_for_date(client, args, run_date)


if __name__ == "__main__":
    main()
