CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.suggest_impression_rate_live`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.suggest_impression_rate_live_v1`
WHERE
  DATE(submission_minute) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.suggest_impression_rate_v1`
WHERE
  DATE(submission_minute) < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
