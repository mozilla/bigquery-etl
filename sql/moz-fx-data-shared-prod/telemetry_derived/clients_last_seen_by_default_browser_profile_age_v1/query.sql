SELECT
  submission_date,
  is_default_browser,
  CASE
    WHEN days_since_first_seen < 28 THEN TRUE
    ELSE FALSE
END
  AS is_new_profile,
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
  is_new_profile,
  normalized_os_version,
  os,
  normalized_channel
