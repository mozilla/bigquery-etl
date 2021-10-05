WITH fenix_unioned AS (
  SELECT
    *
  FROM
    org_mozilla_firefox.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_firefox_beta.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_fennec_aurora.metrics
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser
FROM
  fenix_unioned
GROUP BY
  submission_date,
  client_id,
  sample_id
