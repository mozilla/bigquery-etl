CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.vat_rates`
AS
SELECT
  *,
  LAG(effective_date) OVER (PARTITION BY country ORDER BY effective_date) AS prev_effective_date,
  LEAD(effective_date) OVER (PARTITION BY country ORDER BY effective_date) AS next_effective_date,
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.vat_rates_v1
