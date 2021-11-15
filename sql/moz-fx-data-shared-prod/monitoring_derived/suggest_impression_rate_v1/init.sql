CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.suggest_impression_rate_v1`
PARTITION BY
  DATE(submission_minute)
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNT(*) AS n,
FROM
  -- For this initialization query, we select from the sanitized table, which contains
  -- 15 days of history rather than just 2.
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v1`
WHERE
  DATE(submission_timestamp) >= '2021-10-27'
GROUP BY
  1
