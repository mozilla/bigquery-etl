CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_engagement_clients`
AS
SELECT
  a.*,
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
  END AS lifecycle_stage
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_engagement_clients_v1` a
