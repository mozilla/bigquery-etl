CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.www_site_events_metrics_v1`(
    date DATE,
    event_category STRING,
    event_action STRING,
    event_label STRING,
    page_path STRING,
    locale STRING,
    page_level_1 STRING,
    page_level_2 STRING,
    page_level_3 STRING,
    page_level_4 STRING,
    page_level_5 STRING,
    page_name STRING,
    device_category STRING,
    operating_system STRING,
    `language` STRING,
    browser STRING,
    country STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    ad_content STRING,
    total_events INT64,
    unique_events INT64
  )
PARTITION BY
  date
CLUSTER BY
  page_name,
  event_category,
  event_action,
  event_label
