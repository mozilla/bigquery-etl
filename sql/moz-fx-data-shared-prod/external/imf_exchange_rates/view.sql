CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.imf_exchange_rates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.external_derived.imf_exchange_rates_v1`
