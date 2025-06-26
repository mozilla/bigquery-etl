CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.firefox_android_clients`
AS
SELECT
  new_profile_clients.* REPLACE (
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
    CASE
      WHEN new_profile_clients.install_source IS NULL
        OR new_profile_clients.install_source = ''
        THEN 'Unknown'
      WHEN new_profile_clients.install_source NOT IN ('com.android.vending')
        THEN 'Other'
      ELSE new_profile_clients.install_source
    END AS install_source
  ),
  CAST(REGEXP_EXTRACT(new_profile_clients.adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
  CAST(REGEXP_EXTRACT(new_profile_clients.adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
  activation_clients.is_activated AS activated,
FROM
  `moz-fx-data-shared-prod.fenix.new_profile_clients` AS new_profile_clients
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.new_profile_activation_clients` AS activation_clients
  USING (client_id, first_seen_date)
