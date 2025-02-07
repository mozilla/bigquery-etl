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
  profile_attribution.install_source,
  profile_attribution.distribution_id,
  profile_attribution.play_store_attribution_campaign,
  profile_attribution.play_store_attribution_medium,
  profile_attribution.play_store_attribution_source,
  profile_attribution.play_store_attribution_content,
  profile_attribution.play_store_attribution_term,
  profile_attribution.meta_attribution_app,
  profile_attribution.adjust_ad_group,
  profile_attribution.adjust_campaign,
  profile_attribution.adjust_creative,
  profile_attribution.adjust_network,
  COUNT(*) AS dau,
FROM
  `moz-fx-data-shared-prod.fenix.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.attribution_clients` AS profile_attribution
  USING (client_id)
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
  install_source,
  distribution_id,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_content,
  play_store_attribution_term,
  meta_attribution_app,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network
