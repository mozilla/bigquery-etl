#!/usr/bin/env python3

"""Build the daily jetstream analysis success/failure rollup.

For a given date, records one row per (experiment, analysis_period,
analysis_basis, segment, metric) cell that jetstream either successfully
computed (found in that experiment's `statistics_*` tables) or failed to
compute (found in `monitoring.jetstream_analysis_errors`).

Table names under `mozanalysis` don't map 1:1 to a single experiment slug
without normalizing hyphens to underscores (`jetstream.bq_normalize_name`),
so the set of tables written on the target date is discovered via
`INFORMATION_SCHEMA.TABLE_STORAGE_TIMELINE`, matched back to experiment
slugs via each table's description, and unioned into a single query
alongside the failure view.

`--date` is jetstream's analysis date, matching its own `--date={{ds}}`
argument. Both it and this task actually run a day later, so table/log
timestamps are filtered on `date + 1`, not `date` itself.

Scheduled to run after telemetry-airflow's `jetstream` DAG completes for the
same analysis date (see metadata.yaml's `depends_on`).

Backfillable, with one caveat: TABLE_STORAGE_TIMELINE is historical, so
discovery is accurate for old dates, but if a table's content changes after
the target date (e.g. an in-progress "overall" table, or an explicit rerun),
a backfill reads today's content, not the target date's -- BigQuery has no
way to recover the old rows past its few-day time-travel window. Tables
written once and never touched again (the common case) aren't affected.
"""

from argparse import ArgumentParser

from google.cloud import bigquery

PROJECT = "moz-fx-data-experiments"
DATASET = "monitoring"
STATS_DATASET = "mozanalysis"
DESTINATION_TABLE = "analysis_results_monitoring_v1"

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

DISCOVER_TABLES_SQL = f"""
WITH discovered_tables AS (
  -- `timestamp` is a periodic snapshot time (matches nearly every table
  -- ever created); `creation_time` only changes when jetstream rewrites a
  -- table, so it's the right filter for "written on this date". Views
  -- aren't excluded automatically here, unlike TABLE_STORAGE.
  SELECT DISTINCT table_name
  FROM `{PROJECT}.region-us.INFORMATION_SCHEMA.TABLE_STORAGE_TIMELINE`
  WHERE table_schema = '{STATS_DATASET}'
    AND table_type = 'BASE TABLE'
    AND STARTS_WITH(table_name, 'statistics_')
    AND DATE(creation_time) = DATE_ADD(@date, INTERVAL 1 DAY)
),
-- jetstream sets each table's description to the exact experiment slug.
table_experiments AS (
  SELECT table_name, TRIM(option_value, '"') AS experiment
  FROM `{PROJECT}.{STATS_DATASET}.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE option_name = 'description'
),
with_period_window AS (
  SELECT
    d.table_name,
    e.experiment,
    SUBSTR(
      d.table_name,
      LENGTH('statistics_' || REGEXP_REPLACE(e.experiment, r'[^a-zA-Z0-9_]', '_') || '_') + 1
    ) AS period_window
  FROM discovered_tables d
  JOIN table_experiments e USING (table_name)
)
SELECT
  table_name,
  experiment,
  REGEXP_REPLACE(period_window, r'_\\d+$', '') AS analysis_period
FROM with_period_window
WHERE REGEXP_CONTAINS(period_window, r'^({"|".join(PERIOD_TOKENS)})_\\d+$')
"""

# `analysis_period` on failed rows is sometimes bare (e.g. "day", logged by
# statistics.py) and sometimes period+window-count (e.g. "day_7", logged by
# calculate_metrics/calculate_metric_for_ds). Window granularity isn't tracked
# by this rollup (see plan-error-categorization.md), so both are normalized to
# the bare period token to match the analysis_period produced by table
# discovery above.
FAILED_SQL = f"""
SELECT
  * EXCEPT (timestamp)
FROM (
  SELECT
    @date AS analysis_date,
    experiment,
    REGEXP_REPLACE(analysis_period, r'_\\d+$', '') AS analysis_period,
    analysis_basis,
    segment,
    metric,
    'failed' AS status,
    error_category,
    timestamp
  FROM `{PROJECT}.{DATASET}.jetstream_analysis_errors`
  WHERE source = 'jetstream'
    AND experiment IS NOT NULL
    AND DATE(timestamp) = DATE_ADD(@date, INTERVAL 1 DAY)
    AND error_category IN ({", ".join(f"'{c}'" for c in FAILURE_CATEGORIES)})
)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY experiment, analysis_period, analysis_basis, segment, metric
  ORDER BY timestamp DESC
) = 1
"""


def _escape(value: str) -> str:
    return value.replace("'", "''")


CURRENTLY_EXISTS_SQL = f"""
SELECT table_name
FROM `{PROJECT}.{STATS_DATASET}.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE' AND table_name IN UNNEST(@table_names)
"""


def discover_statistics_tables(
    client: bigquery.Client, date: str
) -> list[bigquery.Row]:
    """Find the statistics_* tables written on `date`, matched to experiments.

    A table found via TABLE_STORAGE_TIMELINE may have since been deleted (it's
    a historical record, so this is expected for old-enough backfills, not a
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
    """Build a UNION ALL over the discovered tables' (metric, segment, basis)."""
    if not tables:
        return """
        SELECT
          CAST(NULL AS STRING) AS experiment,
          CAST(NULL AS STRING) AS analysis_period,
          CAST(NULL AS STRING) AS analysis_basis,
          CAST(NULL AS STRING) AS segment,
          CAST(NULL AS STRING) AS metric
        WHERE FALSE
        """
    return "\nUNION ALL\n".join(f"""
        SELECT DISTINCT
          '{_escape(row.experiment)}' AS experiment,
          '{_escape(row.analysis_period)}' AS analysis_period,
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
      COALESCE(f.analysis_basis, s.analysis_basis) AS analysis_basis,
      COALESCE(f.segment, s.segment) AS segment,
      COALESCE(f.metric, s.metric) AS metric,
      CASE
        WHEN f.error_category IS NOT NULL AND s.status IS NOT NULL THEN 'partial'
        WHEN f.error_category IS NOT NULL THEN 'failed'
        ELSE 'succeeded'
      END AS status,
      f.error_category
    FROM failed f
    FULL OUTER JOIN succeeded s
      ON f.experiment = s.experiment
      AND f.analysis_period = s.analysis_period
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

    client = bigquery.Client(project=args.project)

    tables = discover_statistics_tables(client, args.date)
    print(f"Discovered {len(tables)} statistics tables")
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
