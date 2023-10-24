SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_firefox.metrics` AS m
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
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_firefox_beta.metrics` AS m
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
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(metrics.counter.events_normal_and_private_uri_count) AS uri_count,
  LOGICAL_OR(metrics.boolean.metrics_default_browser) AS is_default_browser,
FROM
  `org_mozilla_fenix.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
