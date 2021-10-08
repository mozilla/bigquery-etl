SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(CAST(NULL AS int64)) AS uri_count,
  LOGICAL_OR(metrics.counter.app_opened_as_default_browser > 0) AS is_default_browser,
FROM
  `org_mozilla_ios_firefox.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "beta" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(CAST(NULL AS int64)) AS uri_count,
  LOGICAL_OR(metrics.counter.app_opened_as_default_browser > 0) AS is_default_browser,
FROM
  `org_mozilla_ios_firefoxbeta.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "nightly" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(CAST(NULL AS int64)) AS uri_count,
  LOGICAL_OR(metrics.counter.app_opened_as_default_browser > 0) AS is_default_browser,
FROM
  `org_mozilla_ios_fennec.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
