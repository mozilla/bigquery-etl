CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
AS
SELECT
  raw.* EXCEPT (generated_time, country, mau_tier1_inclusive),
    -- We rename this column here to match the new standard of prefixing _mau
    -- with the usage criterion; we can refactor to have the correct name in
    -- the raw table the next time we need to make a change and backfill.
    mau_tier1_inclusive AS seen_in_tier1_country_mau,
  -- Telemetry data includes a "country" field that is the 2-letter ISO 3166-1
  -- country code while FxA data contains long-form country names;
  -- we join with a public dataset to retrieve the country code here to have
  -- a consistent interface with desktop and nondesktop exact_mau28 views;
  -- this value will be null for some countries with multiple names
  -- (for example, FxA data contains "South Korea" vs. "Republic of Korea").
  census_data.country_code AS country,
  raw.country AS country_name
FROM
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_raw_v1` AS raw
LEFT JOIN
  `bigquery-public-data.census_bureau_international.country_names_area` AS census_data
ON
  raw.country = census_data.country_name
