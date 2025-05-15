-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.engagement_clients`
AS
WITH active_users AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    first_seen_date,
    normalized_channel,
    app_name,
    app_display_version,
    country,
    city,
    geo_subdivision,
    locale,
    isp,
    normalized_os,
    normalized_os_version,
    device_model,
    device_manufacturer,
    device_type,
    is_dau,
    is_wau,
    is_mau,
    is_mobile,
  FROM
    `moz-fx-data-shared-prod.fenix.active_users`
),
attribution AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_timestamp,
    play_store_attribution_content,
    play_store_attribution_term,
    play_store_attribution_install_referrer_response,
    meta_attribution_app,
    meta_attribution_timestamp,
    install_source,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    adjust_attribution_timestamp,
    distribution_id,
    paid_vs_organic,
  FROM
    `moz-fx-data-shared-prod.fenix.attribution_clients`
)
SELECT
  submission_date,
  client_id,
  sample_id,
  first_seen_date,
  app_name,
  normalized_channel,
  app_display_version AS app_version,
  locale,
  country,
  city,
  geo_subdivision,
  isp,
  is_dau,
  is_wau,
  is_mau,
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
  CASE
    WHEN active_users.submission_date = first_seen_date
      THEN 'new_profile'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
  device_type,
  device_manufacturer,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_model,
FROM
  active_users
LEFT JOIN
  attribution
  USING (client_id, sample_id, normalized_channel)
