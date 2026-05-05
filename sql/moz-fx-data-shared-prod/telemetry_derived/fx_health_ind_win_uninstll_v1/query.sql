SELECT
  DATE(submission_timestamp) AS submission_date,
  COUNT(DISTINCT(client_id)) AS uninstall_client_count
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
  AND application.channel = 'release'
GROUP BY
  DATE(submission_timestamp)
