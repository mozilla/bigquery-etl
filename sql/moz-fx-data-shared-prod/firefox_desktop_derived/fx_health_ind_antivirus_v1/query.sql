SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_os,
  metrics.string_list.windows_security_antivirus AS antivirus,
  COUNT(DISTINCT client_info.client_id) AS unique_clients
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  normalized_os,
  metrics.string_list.windows_security_antivirus
