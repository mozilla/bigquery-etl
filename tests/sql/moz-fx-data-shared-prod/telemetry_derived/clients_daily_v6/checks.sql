ASSERT(
  (
    SELECT
      COUNT(*)
    FROM
      `{{project_id}}.{{dataset_id}}.{{table_name}}`
    WHERE
      submission_date = @submission_date
  ) > 0
)
AS
  'ETL Data Check Failed: Table {{project_id}}.{{dataset_id}}.{{table_name}} contains 0 rows for date: {{ submission_date }}.'
