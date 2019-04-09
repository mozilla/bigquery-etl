CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
AS
SELECT
  raw.* EXCEPT (generated_time),
  census_data.country_name
FROM
  `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_raw_v1` AS raw
LEFT JOIN
  `bigquery-public-data.census_bureau_international.country_names_area` AS census_data
ON
  raw.country = census_data.country_code
