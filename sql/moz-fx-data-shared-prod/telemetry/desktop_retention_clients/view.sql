CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_retention_clients`
AS
SELECT
  *,
  CASE
    WHEN first_seen_date = metric_date
      THEN 'new_profile'
    WHEN DATE_DIFF(metric_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(metric_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_retention_clients_v2`
