SELECT
  DATE(submission_timestamp) AS submission_date,
  payload.other_installs AS nbr_other_installs,
  COUNT(DISTINCT client_id) AS nbr_unique_clients
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  nbr_other_installs
