CREATE MATERIALIZED VIEW `moz-fx-data-shared-prod.monitoring_derived.suggest_click_rate_live_v1`
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNT(*) AS n,
  COUNTIF(release_channel = "release") AS n_release,
  COUNTIF(release_channel = "beta") AS n_beta,
  COUNTIF(release_channel = "nightly") AS n_nightly,
  COUNT(request_id) AS n_merino,
  COUNTIF(request_id IS NOT NULL AND release_channel = "release") AS n_merino_release,
  COUNTIF(request_id IS NOT NULL AND release_channel = "beta") AS n_merino_beta,
  COUNTIF(request_id IS NOT NULL AND release_channel = "nightly") AS n_merino_nightly,
FROM
  `moz-fx-data-shared-prod.contextual_services_live.quicksuggest_click_v1`
WHERE
  DATE(submission_timestamp) > '2010-01-01'
GROUP BY
  1
