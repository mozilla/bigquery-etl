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
  attribution.play_store_attribution_content,
  attribution.play_store_attribution_term,
  attribution.play_store_attribution_install_referrer_response,
  attribution.meta_attribution_app,
  attribution.install_source,
  attribution.adjust_ad_group,
  attribution.adjust_campaign,
  attribution.adjust_creative,
  attribution.adjust_network,
  attribution.distribution_id,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.fenix.active_users` AS active_users
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.attribution_clients` AS attribution
  USING (client_id)
WHERE
  active_users.submission_date < CURRENT_DATE
  AND is_new_profile
  AND is_daily_user
  AND active_users.submission_date = active_users.first_seen_date
