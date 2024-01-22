#fail
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(1, "`date` = @submission_date") }}
#fail
SELECT
  IF(
    COUNTIF(is_new_profile) <> COUNT(*),
    ERROR("Number of is_new_profile TRUE values should be the same as the row count."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  `date` = @submission_date;

#fail
SELECT
  IF(
    DATE_DIFF(`date`, first_seen_date, DAY) <> 6,
    ERROR("Day difference between values inside `date` and submission_date fields should be 6."),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  `date` = @submission_date;
