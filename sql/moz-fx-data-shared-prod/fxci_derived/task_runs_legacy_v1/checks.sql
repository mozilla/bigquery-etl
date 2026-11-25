#fail
{{ is_unique(["task_id", "run_id"], where="submission_date = @submission_date") }}

#warn
{{ not_null([
  "task_id",
  "run_id",
  "resolved",
  "scheduled",
  "state",
  "submission_date"
  ], where="submission_date = @submission_date") }}

#warn
{{ min_row_count(1, where="submission_date = @submission_date") }}

#warn
SELECT
  IF(
    COUNTIF(state NOT IN ("completed", "failed", "exception")) > 0,
    ERROR("Unexpected values for field state detected."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;

#warn
SELECT
  IF(
    COUNTIF(started > resolved) > 0,
    ERROR("Runs that started after they were resolved detected."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;
