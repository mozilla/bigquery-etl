SELECT
  submission_date,
  normalized_channel,
  distribution_id,
  enterprise_classification,
  is_dau,
  COUNT(*) AS client_count,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.enterprise_metrics_clients`
GROUP BY
  ALL
