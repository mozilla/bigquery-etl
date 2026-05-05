SELECT
  DATE(submission_timestamp) AS submission_date,
  environment.system.cpu.cores AS nbr_cpu_cores,
  COUNT(DISTINCT client_id) AS uninstalls_count
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  nbr_cpu_cores
