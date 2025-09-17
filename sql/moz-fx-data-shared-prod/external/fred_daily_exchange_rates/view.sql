CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.fred_daily_exchange_rates`
AS
SELECT
  submission_date,
  conversion_type,
  1 / exchange_rate AS exchange_rate
FROM
  `moz-fx-data-shared-prod.external_derived.fred_daily_exchange_rates_v1`
