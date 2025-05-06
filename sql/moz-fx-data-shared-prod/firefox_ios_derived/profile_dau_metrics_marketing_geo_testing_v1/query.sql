SELECT
  active_users.submission_date,
  active_users.normalized_channel,
  active_users.app_name,
  active_users.app_display_version AS app_version,
  COALESCE(active_users.country, '??') AS country,
  active_users.city,
  active_users.geo_subdivision,
  active_users.locale,
  active_users.device_type,
  active_users.is_mobile,
  profile_attribution.is_suspicious_device_client,
  profile_attribution.adjust_ad_group,
  profile_attribution.adjust_campaign,
  profile_attribution.adjust_creative,
  profile_attribution.adjust_network,
  COUNT(*) AS dau,
FROM
  `moz-fx-data-shared-prod.firefox_ios.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients` AS profile_attribution
  USING (client_id, channel)
WHERE
  active_users.submission_date = @submission_date
  AND is_dau
GROUP BY
  submission_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  city,
  geo_subdivision,
  locale,
  device_type,
  is_mobile,
  is_suspicious_device_client,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network
