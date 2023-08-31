#fail

ASSERT(
  (
    SELECT
      COUNT(*)
    FROM
      `{{project_id}}.{{dataset_id}}.{{table_name}}`
  ) > 50000
)
AS
  'ETL Data Check Failed: Table {{project_id}}.{{dataset_id}}.{{table_name}} contains less than 50,000 rows for date: {{ download_date }}.';

ASSERT(
  (
    SELECT
      COUNT(*)
    FROM
      `{{project_id}}.{{dataset_id}}.{{table_name}}`
  ) > 60000
)
AS
  'ETL Data Check Failed: Table {{project_id}}.{{dataset_id}}.{{table_name}} contains less than 60,000 rows for date: {{ download_date }}.'

#warn

ASSERT(
  (
    SELECT
      COUNT(*)
    FROM
      `{{project_id}}.{{dataset_id}}.{{table_name}}`
  ) > 20000
)
AS
  'ETL Data Check Failed: Table {{project_id}}.{{dataset_id}}.{{table_name}} contains less than 20,000 rows for date: {{ download_date }}.'

