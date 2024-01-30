#fail
{{ is_unique(["client_id"]) }}

#fail
{{ min_row_count(1, "`submission_date` = @submission_date") }}

#fail
WITH upstream_clients_count AS (
  SELECT
    COUNT(*)
  FROM
    `{{ project_id }}.firefox_ios.firefox_ios_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
),
activations_clients_count AS (
  SELECT
    COUNT(*)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM upstream_clients_count) <> (SELECT * FROM activations_clients_count),
    ERROR("Number of client records should match for the same first_seen_date."),
    NULL
  );

#fail
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 6,
    ERROR(
      "Day difference between values inside first_seen_date and submission_date fields should be 6."
    ),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  `submission_date` = @submission_date;
