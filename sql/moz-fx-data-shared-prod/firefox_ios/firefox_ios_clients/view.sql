CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
AS
SELECT
  * EXCEPT (is_suspicious_device_client) REPLACE (
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
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
WHERE
  -- filtering out suspicious devices on iOS, for more info see: bug-1846554
  NOT is_suspicious_device_client
