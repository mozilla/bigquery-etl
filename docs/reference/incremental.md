# Incremental Queries

## Benefits

- BigQuery billing discounts for destination table partitions not modified in
  the last 90 days
- May use [dags.utils.gcp.bigquery_etl_query] to simplify airflow configuration
  e.g. see [dags.main_summary.exact_mau28_by_dimensions]
- May use [script/generate_incremental_table] to automate backfilling
- Should use `WRITE_TRUNCATE` mode or `bq query --replace` to replace
  partitions atomically to prevent duplicate data
- Will have tooling to generate an optimized _mostly materialized view_ that
  only calculates the most recent partition

[script/generate_incremental_table]: https://github.com/mozilla/bigquery-etl/blob/main/script/generate_incremental_table
[dags.utils.gcp.bigquery_etl_query]: https://github.com/mozilla/telemetry-airflow/blob/89a6dc3/dags/utils/gcp.py#L364

## Properties

- Must accept a date via `@submission_date` query parameter
  - Must output a column named `submission_date` matching the query parameter
- Must produce similar results when run multiple times
  - Should produce identical results when run multiple times
- May depend on the previous partition
  - If using previous partition, must include an `init.sql` query to initialize the
    table, e.g. `sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_v1/init.sql`
  - Should be impacted by values from a finite number of preceding partitions
    - This allows for backfilling in chunks instead of serially for all time
      and limiting backfills to a certain number of days following updated data
    - For example `sql/moz-fx-data-shared-prod/clients_last_seen_v1.sql` can be run serially on any 28 day
      period and the last day will be the same whether or not the partition
      preceding the first day was missing because values are only impacted by
      27 preceding days

[dags.main_summary.exact_mau28_by_dimensions]: https://github.com/mozilla/telemetry-airflow/blob/89a6dc3/dags/main_summary.py#L385-L390
