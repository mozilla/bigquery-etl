CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.smoot_usage_day_0`
AS WITH
  unioned AS (
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_desktop_v2`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_nondesktop_v2`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_fxa_v2` )
  --
SELECT
  submission_date AS `date`,
  usage,
  metrics.day_0.*,
  * EXCEPT (submission_date, usage, metrics)
FROM
  unioned
UNION ALL
SELECT
  DATE_SUB(submission_date, INTERVAL 6 DAY) AS `date`,
  usage,
  new_profiles AS dau,
  NULL AS wau,
  NULL AS mau,
  NULL AS active_days_in_week,
  * EXCEPT (submission_date, usage, new_profiles)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_new_profiles_v2`
