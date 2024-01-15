SELECT
  clients.* EXCEPT(is_activated) REPLACE (
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
    END AS adjust_network
  ),
  -- We need to pull is_activated from clients_activation which correctly
  -- determines if a specific client is activated or not.
  -- see: DENG-2083 for more info.
  activation.is_activated,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1` AS clients
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_ios.clients_activation` AS activation
USING(client_id, first_seen_date)
