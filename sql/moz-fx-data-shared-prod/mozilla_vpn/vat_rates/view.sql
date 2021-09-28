CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.vat_rates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.vat_rates_v1
