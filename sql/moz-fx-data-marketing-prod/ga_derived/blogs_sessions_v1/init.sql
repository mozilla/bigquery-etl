CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.blogs_sessions_v1`(
    date DATE,
    visitIdentifier STRING,
    deviceCategory STRING,
    operatingSystem STRING,
    browser STRING,
    -- format: off
    language STRING,
    -- format: on
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
