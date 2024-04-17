SELECT
  metric_date,
  channel,
  country,
  COUNTIF(new_profile) AS new_profiles,
  COUNTIF(new_profile_retained_week_4) AS new_profiles_retained_week_4,
  COUNTIF(day_ping) AS day_ping,
  COUNTIF(sent_ping_week_4) AS sent_ping_week_4,
  COUNTIF(day_active) AS day_active,
  COUNTIF(retained_week_4) AS retained_week_4,
FROM
  firefox_ios.retention_clients
WHERE
  submission_date = @submission_date
  AND NOT is_suspicious_device_client
  AND app_name = "Firefox iOS"
GROUP BY
  metric_date,
  channel,
  country
