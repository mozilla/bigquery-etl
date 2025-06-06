SELECT
  @submission_date AS first_seen_date,
  client_id,
  normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  city,
  geo_subdivision,
  locale,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_manufacturer,
  device_model,
  device_type,
  is_mobile,
FROM
  `moz-fx-data-shared-prod.focus_ios.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.focus_ios.attribution_clients` AS attribution
  USING (client_id, normalized_channel)
WHERE
  active_users.submission_date = @submission_date
  AND active_users.first_seen_date = @submission_date
  AND is_new_profile
  AND is_daily_user
