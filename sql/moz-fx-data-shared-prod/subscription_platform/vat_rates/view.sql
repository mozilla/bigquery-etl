CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.vat_rates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.vat_rates_v1`
