CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.exchange_rates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.external_derived.exchange_rates_v1`
