-- Query generated via `kpi_support_metrics` SQL generator.
SELECT
  metric_date,
  first_seen_date,
  app_name,
  normalized_channel,
  country,
  app_version,
  locale,
  is_mobile,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  is_suspicious_device_client,
  COUNTIF(ping_sent_metric_date) AS ping_sent_metric_date,
  COUNTIF(ping_sent_week_4) AS ping_sent_week_4,
  COUNTIF(active_metric_date) AS active_metric_date,
  COUNTIF(retained_week_4) AS retained_week_4,
  COUNTIF(retained_week_4_new_profile) AS retained_week_4_new_profiles,
  COUNTIF(new_profile_metric_date) AS new_profiles_metric_date,
  COUNTIF(repeat_profile) AS repeat_profiles,
FROM
  `moz-fx-data-shared-prod.firefox_ios.retention_clients`
WHERE
  metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
  AND submission_date = @submission_date
GROUP BY
  metric_date,
  first_seen_date,
  app_name,
  normalized_channel,
  country,
  app_version,
  locale,
  is_mobile,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  is_suspicious_device_client
