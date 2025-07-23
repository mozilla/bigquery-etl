-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.engagement`
AS
SELECT
  *,
  CASE
    WHEN first_seen_date = submission_date
      THEN 'new_profile'
    WHEN DATE_DIFF(submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
  "Organic" AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.focus_android_derived.engagement_v1`
