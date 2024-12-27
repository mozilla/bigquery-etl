SELECT
  submission_date,
  is_default_browser,
  days_since_first_seen < 28 new_profile,
  days_since_first_seen >= 28 old_profile,
  normalized_os_version,
  os,
  normalized_channel,
  COUNT(*) AS row_count,
FROM
  `moz-fx-data-shared-prod.telemetry.clients_last_seen`
WHERE
  days_since_seen = 0
  AND submission_date = @submission_date
GROUP BY
  submission_date,
  is_default_browser,
  new_profile,
  old_profile,
  normalized_os_version,
  os,
  normalized_channel
