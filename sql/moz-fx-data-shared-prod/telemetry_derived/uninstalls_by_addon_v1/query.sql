SELECT
  DATE(submission_timestamp) AS submission_date,
  environment.addons.`active_addons`[SAFE_OFFSET(0)].value.name AS active_addon_name,
  COUNT(DISTINCT client_id) AS uninstalls_count
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  active_addon_name
