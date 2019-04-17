CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
AS
SELECT
  raw.* EXCEPT (generated_time, mau_tier1_inclusive),
    -- We rename this column here to match the new standard of prefixing _mau
    -- with the usage criterion; we can refactor to have the correct name in
    -- the raw table the next time we need to make a change and backfill.
    mau_tier1_inclusive AS seen_in_tier1_country_mau
FROM
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_raw_v1` AS raw
