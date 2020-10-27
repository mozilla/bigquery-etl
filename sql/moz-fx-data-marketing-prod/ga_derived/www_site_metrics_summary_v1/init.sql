CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.www_site_metrics_summary_v1`(
    date DATE,
    device_category STRING,
    operating_system STRING,
    browser STRING,
    `language` STRING,
    country STRING,
    standardized_country_name STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    ad_content STRING,
    sessions INT64,
    non_fx_sessions INT64,
    downloads INT64,
    non_fx_downloads INT64
  )
PARTITION BY
  date
CLUSTER BY
  country,
  browser,
  source,
  medium
