SELECT
  DATE(submission_timestamp) AS submission_date,
  environment.settings.attribution.dlsource AS attribution_dlsource,
  COUNT(DISTINCT client_id) AS uninstalls_count
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  attribution_dlsource
