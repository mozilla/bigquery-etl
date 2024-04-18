SELECT
  metric_date,
  first_seen_date,
  app_name,
  channel,
  country,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  COUNTIF(ping_sent_metric_date) AS ping_sent_metric_date,
  COUNTIF(ping_sent_week_4) AS ping_sent_week_4,
  COUNTIF(active_metric_date) AS active_metric_date,
  COUNTIF(retained_week_4) AS retained_week_4,
  COUNTIF(new_client_metric_date) AS new_clients_metric_date,
  COUNTIF(retained_week_4_new_client) AS retained_week_4_new_clients,
  COUNTIF(repeat_client) AS repeat_clients,
FROM
  tmp.retention_clients
  -- firefox_ios.retention_clients
WHERE
  metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
  AND submission_date = @submission_date
  AND NOT is_suspicious_device_client
GROUP BY
  metric_date,
  first_seen_date,
  app_name,
  channel,
  country,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network
