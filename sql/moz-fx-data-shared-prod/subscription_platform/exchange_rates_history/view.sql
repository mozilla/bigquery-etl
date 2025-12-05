CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.exchange_rates_history`
AS
SELECT
  base_currency,
  quote_currency,
  price_type AS exchange_rate_type,
  price AS exchange_rate,
  `date` AS valid_from,
  COALESCE(
    (LEAD(`date`) OVER (PARTITION BY base_currency, quote_currency ORDER BY `date`) - 1),
    '9999-12-31'
  ) AS valid_to
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.exchange_rates_v1`
