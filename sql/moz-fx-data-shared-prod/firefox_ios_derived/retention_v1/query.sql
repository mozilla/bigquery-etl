SELECT
  metric_date,
  channel,
  country,
  COUNTIF(new_client) AS new_clients,
  COUNTIF(new_client_retained_week_4) AS new_clients_retained_week_4,
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
