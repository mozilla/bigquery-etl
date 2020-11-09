CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.www_site_landing_page_metrics_v1`(
    date DATE,
    site STRING,
    device_category STRING,
    operating_system STRING,
    `language` STRING,
    landing_page STRING,
    locale STRING,
    page_name STRING,
    page_level_1 STRING,
    page_level_2 STRING,
    page_level_3 STRING,
    page_level_4 STRING,
    page_level_5 STRING,
    country STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    ad_content STRING,
    browser STRING,
    sessions INT64,
    non_fx_sessions INT64,
    downloads INT64,
    non_fx_downloads INT64,
    pageviews INT64,
    unique_pageviews INT64,
    single_page_sessions INT64,
    bounces INT64,
    exits INT64
  )
PARTITION BY
  date
CLUSTER BY
  page_name,
  country,
  locale,
  medium
