#fail
{{ is_unique(["client_id"]) }}
#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
#fail
-- Here we're checking that the retention_week_2 generated inside funnel_retention_week_2_v1
-- matches that reported by this table (generated 2 weeks later).
WITH retention_week_2 AS (
  SELECT
    COUNTIF(retained_week_2)
  FROM
    `{{ project_id }}.{{ dataset_id }}.funnel_retention_week_2_v1`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
retention_week_2_week_4_generated AS (
  SELECT
    COUNTIF(retained_week_2)
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  IF(
    (SELECT * FROM retention_week_2) <> (SELECT * FROM retention_week_2_week_4_generated),
    ERROR(
      CONCAT(
        "Retention reported for week 2 by week_2 (",
        (SELECT * FROM retention_week_2),
        ") and week_4 (",
        (SELECT * FROM retention_week_2_week_4_generated),
        ") tables does not match."
      )
    ),
    NULL
  )
