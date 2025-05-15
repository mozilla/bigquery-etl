-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.new_profile_clients`
AS
SELECT
  client_id,
  active_users.submission_date AS first_seen_date,
  normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  city,
  geo_subdivision,
  locale,
  isp,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  attribution.play_store_attribution_campaign,
  attribution.play_store_attribution_medium,
  attribution.play_store_attribution_source,
  attribution.play_store_attribution_timestamp,
  attribution.play_store_attribution_content,
  attribution.play_store_attribution_term,
  attribution.play_store_attribution_install_referrer_response,
  attribution.meta_attribution_app,
  attribution.meta_attribution_timestamp,
  attribution.install_source,
  attribution.adjust_ad_group,
  attribution.adjust_campaign,
  attribution.adjust_creative,
  attribution.adjust_network,
  attribution.adjust_attribution_timestamp,
  attribution.distribution_id,
  attribution.paid_vs_organic,
  device_type,
FROM
  `moz-fx-data-shared-prod.fenix.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.attribution_clients` AS attribution
  USING (client_id, normalized_channel)
WHERE
  active_users.submission_date < CURRENT_DATE
  AND is_new_profile
  AND is_daily_user
  AND active_users.submission_date = active_users.first_seen_date
