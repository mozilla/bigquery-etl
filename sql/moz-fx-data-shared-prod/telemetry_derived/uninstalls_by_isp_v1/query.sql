SELECT
  DATE(submission_timestamp) AS submission_date,
  metadata.isp.name AS isp_name,
  COUNT(DISTINCT client_id) AS nbr_clients_uninstalling,
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  isp_name
