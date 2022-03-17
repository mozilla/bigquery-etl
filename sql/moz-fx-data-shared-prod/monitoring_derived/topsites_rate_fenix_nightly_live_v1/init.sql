CREATE MATERIALIZED VIEW `moz-fx-data-shared-prod.monitoring_derived.topsites_rate_fenix_nightly_live_v1`
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNTIF(events[SAFE_OFFSET(0)].name = 'contile_impression') AS n_impression,
  COUNTIF(events[SAFE_OFFSET(0)].name = 'contile_click') AS n_click,
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_live.topsites_impression_v1`
WHERE
  DATE(submission_timestamp) >= '2010-01-01'
GROUP BY
  submission_minute
