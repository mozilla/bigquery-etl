SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_firefox.metrics` m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_firefox_beta.metrics` m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_fenix.metrics` m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_fenix_nightly.metrics` m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id
UNION ALL
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_fennec_aurora.metrics` m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id
