CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
AS
SELECT
  raw.* EXCEPT (mau_tier1_inclusive),
  -- We rename this column here to match the new standard of prefixing _mau
  -- with the usage criterion; we can refactor to have the correct name in
  -- the raw table the next time we need to make a change and backfill.
  mau_tier1_inclusive AS seen_in_tier1_country_mau,
  COALESCE(cn.code, raw.country) AS country_code
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.exact_mau28_v1` AS raw
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_names_v1` cn
  ON (raw.country = cn.name)
