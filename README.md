[![CircleCI](https://img.shields.io/circleci/project/github/mozilla/bigquery-etl/master.svg)](https://circleci.com/gh/mozilla/bigquery-etl)

BigQuery ETL
===

Bigquery UDFs and SQL queries for building derived datasets.

Recommended practices
---

### Queries

- Should be defined in files named as `sql/table_version.sql` e.g.
  `sql/clients_daily_v6.sql`
- Should not specify a project or dataset in table names to simplify testing
- Should be [incremental](#incremental-queries)
- Should filter input tables on partition and clustering columns
- Should use `_` prefix in generated column names not meant for output
- Should not use `DATETIME` type, due to incompatiblity with
  [spark-bigquery-connector](GoogleCloudPlatform/spark-bigquery-connector#5)
- Should not use jinja templating on the query file in Airflow

### UDFs

- Should be used to avoid code duplication
- Should use lower snake case names with `udf_` prefix e.g. `udf_mode_last`
- Should be defined in files named as `udfs/function.{sql,js}` e.g. `udfs/udf_mode_last.sql`
- Should use `SQL` over `js` for performance
- Must not be used for incremental queries with a _mostly materialized view_ (defined below)

### Backfills

- Should be avoided on large datasets
  - Backfills may double storage cost for a dataset for 90 days by moving
    data from long-term storage to short-term storage
    - For example regenerating `clients_last_seen_v1` from scratch would cost
      about $1600 for the query and about $6800 for data moved to short-term
      storage
  - Should combine multiple backfills happening around the same time
  - Should delay column deletes until the next other backfill
    - Should use `NULL` for new data and `EXCEPT` to exclude from views until
      dropped
- Should use copy operations in append mode to change column order
  - Copy operations do not allow changing partitioning, changing clustering, or
    column deletes, but using the Storage API to do these things for less than
    on-demand query pricing, $5 per TiB as of 2019-04-01, is being investigated
    in [#66](https://github.com/mozilla/bigquery-etl/issues/66)
- Should split backfilling into queries that finish in minutes not hours
  - May output to multiple tables that are combined using copy-append
    operations
- May generate incremental tables from scratch
  - Tooling is being added to automate this process in
    [#65](https://github.com/mozilla/bigquery-etl/issues/65)
  - May use `UPDATE` instead of equivalent `SELECT` queries to improve speed
    - In one case adding a new column calculated from existing columns with
      `DATE_DIFF` using `UPDATE` was twice as fast as using `SELECT`

Incremental Queries
---

### Benefits

- BigQuery billing discounts for destination table partitions not modified in
  the last 90 days
- Requires less airflow configuration
- Will have tooling to automate backfilling
- Will have tooling to replace partitions atomically to prevent duplicate data
- Will have tooling to generate an optimized _mostly materialized view_ that
  only calculates the most recent partition
  - Note: incompatible with UDFs, which are not allowed in views

### Properties

- Must accept a date via `@submission_date` query parameter
  - Must output a column named `submission_date` matching the query parameter
- Must produce similar results when run multiple times
  - Should produce identical results when run multiple times
- May depend on the previous partition
  - If using previous partition, must include a `.init.sql` query to init the
    table
  - Should be impacted by values from a finite number of preceding partitions
    - This allows for backfilling in chunks instead of serially for all time
      and limiting backfills to a certain number of days following updated data
    - For example `sql/clients_last_seen_v1.sql` can be run serially on any 28 day
      period and the last day will be the same whether or not the partition
      preceding the first day was missing because values are only impacted by
      27 preceding days

Tests
---

[See the documentation in tests/](tests/README.md)
