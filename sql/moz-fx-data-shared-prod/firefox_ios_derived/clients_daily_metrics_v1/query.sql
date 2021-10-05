WITH ios_unioned AS (
  SELECT
    *
  FROM
    org_mozilla_ios_firefox.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_firefoxbeta.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
    -- no URI count on fx ios - there is cumulative tab count.
  SUM(CAST(NULL AS int64)) AS uri_count,
    -- https://dictionary.telemetry.mozilla.org/apps/firefox_ios/metrics/app_opened_as_default_browser
  LOGICAL_OR(metrics.counter.app_opened_as_default_browser > 0) AS is_default_browser
FROM
  ios_unioned
GROUP BY
  submission_date,
  client_id,
  sample_id
