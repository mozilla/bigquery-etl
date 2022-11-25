CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_android_clients`
AS
SELECT
  * EXCEPT (adjust_network, install_source),
  CASE
  WHEN
    adjust_network NOT IN (
      '',
      'Organic',
      'Google Organic Search',
      'Untrusted Devices',
      'Product Marketing (Owned media)',
      'Google Ads ACI'
    )
    AND adjust_network IS NOT NULL
  THEN
    'Other'
  ELSE
    adjust_network
  END
  AS adjust_network,
  CASE
  WHEN
    install_source NOT IN ('com.android.vending')
    AND install_source IS NOT NULL
  THEN
    'Other'
  ELSE
    install_source
  END
  AS install_source
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_android_clients_v1`
