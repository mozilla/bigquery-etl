SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  sample_id,
  "release" AS normalized_channel,
  COUNT(*) AS n_metrics_ping,
  1 AS days_sent_metrics_ping_bits,
  SUM(CAST(NULL AS int64)) AS uri_count,
  LOGICAL_OR(CAST(NULL AS boolean)) AS is_default_browser,
FROM
  `org_mozilla_ios_focus.metrics` AS m
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  sample_id,
  normalized_channel
