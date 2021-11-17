CREATE MATERIALIZED VIEW `moz-fx-data-shared-prod.monitoring_derived.topsites_impression_rate_live_v1`
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNT(*) AS n,
  COUNT(reporting_url) AS n_contile,
  COUNTIF(reporting_url IS NULL) AS n_remotesettings,
FROM
  `moz-fx-data-shared-prod.contextual_services_live.topsites_impression_v1`
WHERE
  DATE(submission_timestamp) > '2010-01-01'
AND
  -- filter out the non-adM advertisers: Yandex
  advertiser NOT IN ('O=45:A', 'yandex')
GROUP BY
  1
