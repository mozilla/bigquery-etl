CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
AS
SELECT
  raw.*,
  COALESCE(cn.code, raw.country) AS country_code
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_accounts_exact_mau28_by_dimensions_v1` AS raw
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` cn
  ON (raw.country = cn.name)
