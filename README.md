BigQuery ETL
===

Bigquery UDFs and SQL queries for building derived datasets.

Recommended practices
===

- Should name sql files like `sql/destination_table_with_version.sql` e.g.
  `sql/clients_daily_v6.sql`
- Should not specify a project or dataset in table names to simplify testing
- Should use incremental queries
- Should filter input tables on partition and clustering columns
- Should use UDF language `SQL` over `js` for performance
- Should use UDFs for reusability
- Should use query parameters over jinja templating
  - Temporary issue: Airflow 1.10+ is required in order to use query parameters

Incremental Queries
===

Incremental queries have these benefits:

- BigQuery billing discounts for destination table partitions not modified in
  the last 90 days
- Requires less airflow configuration
- Will have tooling to automate backfilling
- Will have tooling to replace partitions atomically to prevent duplicate data
- Will have tooling to generate an optimized "destination plus" view that
  calculates the most recent partition

Incremental queries have these properties:

- Must accept a date via `@submission_date` query parameter
  - Must output a column named `submission_date` matching the query parameter
- Must produce similar results when run multiple times
  - Should produce identical results when run multiple times
- May depend on the previous partition
  - If using previous partition, must include a `.init.sql` query to init the
    first partition
