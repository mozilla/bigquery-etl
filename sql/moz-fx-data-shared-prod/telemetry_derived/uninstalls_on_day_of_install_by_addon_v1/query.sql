SELECT
  DATE(submission_timestamp) AS submission_date,
  IF(
    DATE(creation_date) = DATE(submission_timestamp),
    TRUE,
    FALSE
  ) AS uninstall_same_day_as_install,
  environment.addons.`active_addons`[SAFE_OFFSET(0)].value.name AS active_addon_name,
  COUNT(DISTINCT client_id) AS uninstalls_count
FROM
  `moz-fx-data-shared-prod.telemetry.uninstall`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND application.name = 'Firefox'
GROUP BY
  submission_date,
  uninstall_same_day_as_install,
  active_addon_name
