CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.www_site_page_metrics_v1`(
    date DATE,
    page STRING,
    locale STRING,
    page_level_1 STRING,
    page_level_2 STRING,
    page_level_3 STRING,
    page_level_4 STRING,
    page_level_5 STRING,
    device_category STRING,
    operating_system STRING,
    `language` STRING,
    browser STRING,
    browser_version STRING,
    country STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    ad_content STRING,
    pageviews INT64,
    unique_pageviews INT64,
    entrances INT64,
    exits INT64,
    non_exit_pageviews INT64,
    total_time_on_page FLOAT64,
    total_events INT64,
    unique_events INT64,
    single_page_sessions INT64,
    bounces INT64,
    page_name STRING
  )
PARTITION BY
  date
CLUSTER BY
  page_name,
  country,
  locale,
  medium
