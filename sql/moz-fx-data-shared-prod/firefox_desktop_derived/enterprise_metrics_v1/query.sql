SELECT
  submission_date,
  normalized_channel,
  enterprise_classification,
  is_dau,
  COUNT(*) AS client_count,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.enterprise_metrics_clients`
WHERE
  submission_date = @submission_date
GROUP BY
  ALL
