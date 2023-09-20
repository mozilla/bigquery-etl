#fail

ASSERT(
  (
    SELECT
      COUNT(*)
    FROM
      `{{project_id}}.{{dataset_id}}.{{table_name}}`
    WHERE
      download_date = @download_date
  ) > 50000
)
AS
  'ETL Data Check Failed: Table {{project_id}}.{{dataset_id}}.{{table_name}} contains less than 50,000 rows for date: {{ download_date }}.'