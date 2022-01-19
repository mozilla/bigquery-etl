-- The IF NOT EXISTS below is to make this dry-runnable; actually recreating
-- this table will require that you modify this to CREATE OR REPLACE.
CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.suggest_impression_rate_v1`
PARTITION BY
  DATE(submission_minute)
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
  -- For this initialization query, we select from the sanitized table, which contains
  -- 15 days of history rather than just 2.
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v1`
WHERE
  DATE(submission_timestamp) >= '2021-10-27'
GROUP BY
  1
