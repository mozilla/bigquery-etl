SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
FROM
  `mozillavpn.metrics` AS m
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
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
FROM
  `org_mozilla_firefox_vpn.metrics` AS m
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
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
FROM
  `org_mozilla_ios_firefoxvpn.metrics` AS m
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
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
FROM
  `org_mozilla_ios_firefoxvpn_network_extension.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
