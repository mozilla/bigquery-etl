#fail
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
#fail
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 13,
    ERROR(
      "Day difference between submission_date and first_seen_date is not equal to 13 as expected"
    ),
    NULL
  )
FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE submission_date = @submission_date;
