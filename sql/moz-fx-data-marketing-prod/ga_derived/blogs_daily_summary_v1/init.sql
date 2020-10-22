CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.blogs_daily_summary_v1`(
    date DATE,
    deviceCategory STRING,
    operatingSystem STRING,
    browser STRING,
    -- format: off
    language STRING,
    -- format: on
    country STRING,
    standardized_country_name STRING,
    source STRING,
    medium STRING,
    campaign STRING,
    content STRING,
    blog STRING,
    subblog STRING,
    sessions INT64,
    downloads INT64,
    socialShare INT64,
    newsletterSubscription INT64
  )
PARTITION BY
  date
CLUSTER BY
  country,
  browser,
  blog,
  subblog
