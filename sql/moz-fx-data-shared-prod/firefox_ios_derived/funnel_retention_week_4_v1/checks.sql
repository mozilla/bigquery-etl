#fail
{{ is_unique([
  "first_seen_date", "first_reported_country", "first_reported_isp",
  "adjust_ad_group", "adjust_campaign", "adjust_creative", "adjust_network"
]) }}
#fail
{{ not_null(["first_seen_date", "adjust_network"], "submission_date = @submission_date") }}
#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
#fail
WITH new_profile_count AS (
  SELECT SUM(new_profiles) FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` WHERE submission_date = @submission_date
),
new_profile_upstream_count AS (
  SELECT COUNT(*) FROM `{{ project_id }}.{{ dataset_id }}.funnel_retention_clients_week_4_v1` WHERE submission_date = @submission_date
)
SELECT IF(
  (SELECT * FROM new_profile_count) <> (SELECT * FROM new_profile_upstream_count),
  ERROR(
    CONCAT(
      "New profile count mismatch between this (",
      (SELECT * FROM new_profile_count),
      ") and upstream (",
      (SELECT * FROM new_profile_upstream_count),
      ") tables"
    )
  ),
  NULL
);
#fail
WITH repeat_user_count AS (
  SELECT SUM(repeat_user) FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` WHERE submission_date = @submission_date
),
repeat_user_upstream_count AS (
  SELECT COUNTIF(repeat_first_month_user) FROM `{{ project_id }}.{{ dataset_id }}.funnel_retention_clients_week_4_v1` WHERE submission_date = @submission_date
)
SELECT IF(
  (SELECT * FROM repeat_user_count) <> (SELECT * FROM repeat_user_upstream_count),
  ERROR(
    CONCAT(
      "New profile count mismatch between this (",
      (SELECT * FROM repeat_user_count),
      ") and upstream (",
      (SELECT * FROM repeat_user_upstream_count),
      ") tables"
    )
  ),
  NULL
);
#fail
WITH retained_week_4_count AS (
  SELECT SUM(retained_week_4) FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}` WHERE submission_date = @submission_date
),
retained_week_4_upstream_count AS (
  SELECT COUNTIF(retained_week_4) FROM `{{ project_id }}.{{ dataset_id }}.funnel_retention_clients_week_4_v1` WHERE submission_date = @submission_date
)
SELECT IF(
  (SELECT * FROM retained_week_4_count) <> (SELECT * FROM retained_week_4_upstream_count),
  ERROR(
    CONCAT(
      "New profile count mismatch between this (",
      (SELECT * FROM retained_week_4_count),
      ") and upstream (",
      (SELECT * FROM retained_week_4_upstream_count),
      ") tables"
    )
  ),
  NULL
);
#fail
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 27,
    ERROR("Day difference between submission_date and first_seen_date is not equal to 27 as expected"),
    NULL
  )
FROM `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE submission_date = @submission_date;
