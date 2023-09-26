#fail
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(1, "first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)") }}
#fail
WITH retention_week_2 AS (
  SELECT
    COUNTIF(retained_week_2)
  FROM
    `{{ project_id }}.{{ dataset_id }}.app_store_retention_week_2_v1`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
retention_week_4 AS (
  SELECT
    COUNTIF(retained_week_2)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  IF(
    (SELECT * FROM retention_week_2) <> (SELECT * FROM retention_week_4),
    ERROR(
      CONCAT(
        "Retention reported for week 2 by week_2 (",
        (SELECT * FROM retention_week_2),
        ") and week_4 (",
        (SELECT * FROM retention_week_4),
        ") tables does not match."
      )
    ),
    NULL
  )
