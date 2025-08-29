CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.fred_daily_exchange_rates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.external_derived.fred_daily_exchange_rates_v1`
