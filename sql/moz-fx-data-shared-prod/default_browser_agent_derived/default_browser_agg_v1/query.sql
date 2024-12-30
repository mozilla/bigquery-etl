SELECT
  DATE(submission_timestamp) AS submission_date,
  mozfun.norm.truncate_version(build_version, 'major') AS build_version_major,
  mozfun.norm.get_windows_info(`os_version`).name AS windows_name,
  normalized_country_code,
  build_channel,
  default_browser,
  previous_default_browser,
  notification_shown,
  notification_type,
  notification_action,
  COUNT(*) AS row_count
FROM
  `moz-fx-data-shared-prod.default_browser_agent.default_browser`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  DATE(submission_timestamp),
  mozfun.norm.truncate_version(build_version, 'major'),
  mozfun.norm.get_windows_info(`os_version`).name,
  normalized_country_code,
  build_channel,
  default_browser,
  previous_default_browser,
  notification_shown,
  notification_type,
  notification_action
