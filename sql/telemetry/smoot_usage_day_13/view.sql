CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.smoot_usage_day_13`
AS WITH
  unioned AS (
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_desktop_v2`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_nondesktop_v2`
  UNION ALL
  SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.smoot_usage_fxa_v2` )
  --
SELECT
  DATE_SUB(submission_date, INTERVAL 13 DAY) AS `date`,
  usage,
  metrics.day_13.*,
  * EXCEPT (submission_date, usage, metrics)
FROM
  unioned
