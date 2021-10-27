CREATE MATERIALIZED VIEW `moz-fx-data-shared-prod.monitoring_derived.suggest_click_rate_live_v1`
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNT(*) AS n,
FROM
  `moz-fx-data-shared-prod.contextual_services_live.quicksuggest_click_v1`
WHERE
  DATE(submission_timestamp) > '2010-01-01'
GROUP BY
  1
