CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.blogs_sessions_v1`(
    date DATE,
    visit_identifier STRING,
    device_category STRING,
    operating_system STRING,
    browser STRING,
    `language` STRING,
    country STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    content STRING,
    blog STRING,
    subblog STRING,
    sessions INT64
  )
PARTITION BY
  date
