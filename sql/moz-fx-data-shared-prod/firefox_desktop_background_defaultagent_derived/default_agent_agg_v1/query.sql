SELECT
  DATE(submission_timestamp) AS submission_date,
  mozfun.norm.truncate_version(client_info.app_display_version, 'major') AS major_build_version,
  normalized_channel,
  metrics.string.system_default_browser,
  metrics.string.system_default_previous_browser,
  metrics.boolean.notification_show_success,
  metrics.string.notification_action,
  COUNT(*) AS row_count
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent.default_agent`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  DATE(submission_timestamp),
  mozfun.norm.truncate_version(client_info.app_display_version, 'major'),
  normalized_channel,
  metrics.string.system_default_browser,
  metrics.string.system_default_previous_browser,
  metrics.boolean.notification_show_success,
  metrics.string.notification_action
