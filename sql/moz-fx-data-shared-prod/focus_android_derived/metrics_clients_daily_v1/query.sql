SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.browser_total_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.browser_is_default) AS is_default_browser,
FROM
  `org_mozilla_focus.metrics` AS m
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
  SUM(metrics.counter.browser_total_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.browser_is_default) AS is_default_browser,
FROM
  `org_mozilla_focus_beta.metrics` AS m
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
  SUM(metrics.counter.browser_total_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.browser_is_default) AS is_default_browser,
FROM
  `org_mozilla_focus_nightly.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
