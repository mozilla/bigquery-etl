CREATE MATERIALIZED VIEW `moz-fx-data-shared-prod.monitoring_derived.topsites_click_rate_live_v1`
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNT(*) AS n,
  COUNT(reporting_url) AS n_contile,
  -- Yandex pings are not included in this monitor
  COUNTIF(reporting_url IS NULL AND advertiser NOT IN ('O=45:A', 'yandex')) AS n_remotesettings,
FROM
  `moz-fx-data-shared-prod.contextual_services_live.topsites_click_v1`
WHERE
  DATE(submission_timestamp) > '2010-01-01'
GROUP BY
  1
