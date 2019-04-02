CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
AS
SELECT
  * EXCEPT (generated_time, country, mau_tier1_inclusive),
    -- We rename this column here to match the new standard of prefixing _mau
    -- with the usage criterion; we can refactor to have the correct name in
    -- the raw table the next time we need to make a change and backfill.
    mau_tier1_inclusive AS seen_in_tier1_country_mau,
  -- We normalize country to match the two-digit country codes that appear in
  -- telemetry data, so that this view is compatible with the exact_mau28 views
  -- for desktop and nondesktop.
  CASE country
    WHEN 'United States' THEN 'US'
    WHEN 'France' THEN 'FR'
    WHEN 'Germany' THEN 'DE'
    WHEN 'United Kingdom' THEN 'UK'
    WHEN 'Canada' THEN 'CA'
    ELSE 'Other'
  END AS country
FROM
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_raw_v1`
