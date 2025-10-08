CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.vat_rates_history`
AS
SELECT
  country_code,
  country AS country_name,
  vat AS vat_rate,
  effective_date AS valid_from,
  COALESCE(
    (LEAD(effective_date) OVER (PARTITION BY country_code ORDER BY effective_date) - 1),
    '9999-12-31'
  ) AS valid_to
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.vat_rates_v1`
