#!/usr/bin/env python3

"""Build the daily jetstream analysis success/failure rollup.

For a given date, records one row per (experiment, analysis_period,
analysis_basis, segment, metric) cell that jetstream either successfully
computed (found in that experiment's `statistics_*` tables) or failed to
compute (found in `monitoring.jetstream_analysis_errors`).

Table names under `mozanalysis` don't encode analysis_basis, and don't map
1:1 to a single experiment slug without normalizing hyphens to underscores
(`jetstream.bq_normalize_name`), so the set of tables written on the target
date is discovered via `INFORMATION_SCHEMA.TABLE_STORAGE`, matched back to
experiment slugs, and unioned into a single query alongside the failure view.

Scheduled to run after telemetry-airflow's `jetstream` DAG completes for the
same date (see metadata.yaml's `depends_on`), since jetstream's own run isn't
part of this DAG and this rollup would otherwise see a partial day.
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

# ordered longest-first so a shorter token (e.g. "day") can't partially shadow
# a longer one (e.g. "days28") in the REGEXP_CONTAINS alternation below.
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
  -- TABLE_STORAGE can lag behind deletions, so cross-check against TABLES.
  SELECT ts.table_name
  FROM `{PROJECT}.region-us.INFORMATION_SCHEMA.TABLE_STORAGE` ts
  JOIN `{PROJECT}.{STATS_DATASET}.INFORMATION_SCHEMA.TABLES` t
    ON ts.table_name = t.table_name
  WHERE ts.table_schema = '{STATS_DATASET}'
    AND ts.table_type = 'BASE TABLE'
    AND t.table_type = 'BASE TABLE'
    AND STARTS_WITH(ts.table_name, 'statistics_')
    AND DATE(ts.storage_last_modified_time) = @date
),
normalized_experiments AS (
  SELECT
    normandy_slug AS experiment,
    REGEXP_REPLACE(normandy_slug, r'[^a-zA-Z0-9_]', '_') AS normalized_slug
  FROM `{PROJECT}.{DATASET}.experimenter_experiments_v1`
  WHERE normandy_slug IS NOT NULL
),
matched AS (
  -- a table can prefix-match more than one experiment's normalized slug
  -- (e.g. "ios-worldcup-feature" is a prefix of "ios-worldcup-feature-1512");
  -- resolved by preferring the longest (most specific) slug below.
  SELECT
    t.table_name,
    e.experiment,
    e.normalized_slug,
    SUBSTR(t.table_name, LENGTH('statistics_' || e.normalized_slug || '_') + 1) AS period_window
  FROM discovered_tables t
  JOIN normalized_experiments e
    ON STARTS_WITH(t.table_name, CONCAT('statistics_', e.normalized_slug, '_'))
),
ranked AS (
  SELECT
    table_name,
    experiment,
    period_window,
    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY LENGTH(normalized_slug) DESC) AS rn
  FROM matched
)
SELECT
  table_name,
  experiment,
  REGEXP_REPLACE(period_window, r'_\\d+$', '') AS analysis_period
FROM ranked
WHERE rn = 1
  AND REGEXP_CONTAINS(period_window, r'^({"|".join(PERIOD_TOKENS)})_\\d+$')
"""

# `analysis_period` on failed rows is sometimes bare (e.g. "day", logged by
# statistics.py) and sometimes period+window-count (e.g. "day_7", logged by
# calculate_metrics/calculate_metric_for_ds). Window granularity isn't tracked
# by this rollup (see plan-error-categorization.md), so both are normalized to
# the bare period token to match the analysis_period produced by table
# discovery above.
FAILED_SQL = f"""
SELECT
  @date AS analysis_date,
  experiment,
  REGEXP_REPLACE(analysis_period, r'_\\d+$', '') AS analysis_period,
  analysis_basis,
  segment,
  metric,
  'failed' AS status,
  error_category
FROM `{PROJECT}.{DATASET}.jetstream_analysis_errors`
WHERE source = 'jetstream'
  AND experiment IS NOT NULL
  AND DATE(timestamp) = @date
  AND error_category IN ({", ".join(f"'{c}'" for c in FAILURE_CATEGORIES)})
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY experiment, analysis_period, analysis_basis, segment, metric
  ORDER BY timestamp DESC
) = 1
"""


def _escape(value: str) -> str:
    return value.replace("'", "''")


def discover_statistics_tables(
    client: bigquery.Client, date: str
) -> list[bigquery.Row]:
    """Find the statistics_* tables written on `date`, matched to experiments."""
    job = client.query(
        DISCOVER_TABLES_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", date)]
        ),
    )
    return list(job.result())


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
    """Combine failed and succeeded into one table."""
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
        'succeeded' AS status,
        CAST(NULL AS STRING) AS error_category
      FROM (
        {succeeded_sql}
      )
    )
    SELECT * FROM failed
    UNION ALL
    SELECT * FROM succeeded
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
    job.result()
    print(
        f"Wrote {job.output_rows} rows to {destination} ({len(tables)} tables discovered)"
    )


if __name__ == "__main__":
    main()
