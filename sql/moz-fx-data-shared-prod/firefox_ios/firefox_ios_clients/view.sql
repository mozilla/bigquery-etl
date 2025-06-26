CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
AS
SELECT
  client_id,
  first_seen_date,
  country AS first_reported_country,
  normalized_channel AS channel,
  device_manufacturer,
  device_model,
  os_version,
  app_version,
  adjust_attribution_timestamp AS submission_timestamp,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  CASE
    WHEN adjust_network IS NULL
      THEN 'Unknown'
    WHEN adjust_network NOT IN (
        'Apple Search Ads',
        'Product Marketing (Owned media)',
        'product-owned'
      )
      THEN 'Other'
    ELSE adjust_network
  END AS adjust_network,
  is_suspicious_device_client,
FROM
  `moz-fx-data-shared-prod.firefox_ios.new_profile_clients`
WHERE
  first_seen_date >= "2023-06-01"
UNION ALL
SELECT
  client_id,
  first_seen_date,
  first_reported_country,
  channel,
  device_manufacturer,
  device_model,
  os_version,
  app_version,
  submission_timestamp,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  is_suspicious_device_client,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
WHERE
  first_seen_date < "2023-06-01"
