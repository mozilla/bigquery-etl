CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.exchange_rates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.exchange_rates_v1
