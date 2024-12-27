SELECT
  DATE(submission_timestamp) AS submission_date,
  os_version,
  mozfun.norm.get_windows_info(`os_version`).name AS windows_name,
  default_browser,
  previous_default_browser,
  mozfun.norm.truncate_version(build_version, 'major') AS build_version_major,
  COUNT(*) AS row_count
FROM
  `moz-fx-data-shared-prod.default_browser_agent.default_browser`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  DATE(submission_timestamp),
  os_version,
  mozfun.norm.get_windows_info(`os_version`).name,
  default_browser,
  previous_default_browser,
  mozfun.norm.truncate_version(build_version, 'major')
