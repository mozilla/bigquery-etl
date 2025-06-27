CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.firefox_android_clients`
AS
SELECT
  new_profile_clients.client_id,
  new_profile_clients.first_seen_date,
  new_profile_clients.normalized_channel AS channel,
  new_profile_clients.app_version,
  new_profile_clients.country AS first_reported_country,
  new_profile_clients.locale,
  new_profile_clients.os_version,
  new_profile_clients.device_model,
  new_profile_clients.device_manufacturer,
  new_profile_clients.play_store_attribution_campaign,
  new_profile_clients.play_store_attribution_medium,
  new_profile_clients.play_store_attribution_source,
  new_profile_clients.play_store_attribution_content,
  new_profile_clients.play_store_attribution_term,
  new_profile_clients.play_store_attribution_install_referrer_response,
  new_profile_clients.meta_attribution_app,
  CASE
    WHEN new_profile_clients.install_source IS NULL
      OR new_profile_clients.install_source = ''
      THEN 'Unknown'
    WHEN new_profile_clients.install_source NOT IN ('com.android.vending')
      THEN 'Other'
    ELSE new_profile_clients.install_source
  END AS install_source,
  new_profile_clients.adjust_ad_group,
  new_profile_clients.adjust_campaign,
  new_profile_clients.adjust_creative,
  CASE
    WHEN new_profile_clients.adjust_network IS NULL
      OR new_profile_clients.adjust_network = ''
      THEN 'Unknown'
    WHEN new_profile_clients.adjust_network NOT IN (
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      THEN 'Other'
    ELSE new_profile_clients.adjust_network
  END AS adjust_network,
  new_profile_clients.distribution_id,
  CAST(REGEXP_EXTRACT(new_profile_clients.adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
  CAST(REGEXP_EXTRACT(new_profile_clients.adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
  activation_clients.is_activated AS activated,
FROM
  `moz-fx-data-shared-prod.fenix.new_profile_clients` AS new_profile_clients
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.new_profile_activation_clients` AS activation_clients
  USING (client_id, first_seen_date)
WHERE
  first_seen_date >= "2023-06-01"
-- Pull older profiles older than 2024-06-01 from the old profiles table
-- due to the retention policy set on the source of the new tables
UNION ALL
SELECT
  client_id,
  first_seen_date,
  channel,
  app_version,
  first_reported_country,
  locale,
  os_version,
  device_model,
  device_manufacturer,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_content,
  play_store_attribution_term,
  play_store_attribution_install_referrer_response,
  meta_attribution_app,
  CASE
    WHEN install_source IS NULL
      OR install_source = ''
      THEN 'Unknown'
    WHEN install_source NOT IN ('com.android.vending')
      THEN 'Other'
    ELSE install_source
  END AS install_source,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  CASE
    WHEN adjust_network IS NULL
      OR adjust_network = ''
      THEN 'Unknown'
    WHEN adjust_network NOT IN (
        'Organic',
        'Google Organic Search',
        'Untrusted Devices',
        'Product Marketing (Owned media)',
        'Google Ads ACI'
      )
      THEN 'Other'
    ELSE adjust_network
  END AS adjust_network,
  distribution_id,
  CAST(REGEXP_EXTRACT(adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
  CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
  activated,
FROM
  `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
WHERE
  first_seen_date < "2023-06-01"
