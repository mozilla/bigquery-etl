CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.www_site_downloads_v1`(
    date DATE,
    visit_identifier STRING,
    device_category STRING,
    operating_system STRING,
    `language` STRING,
    country STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    ad_content STRING,
    browser STRING,
    download_events INT64,
    downloads INT64,
    non_fx_downloads INT64
  )
PARTITION BY
  date
