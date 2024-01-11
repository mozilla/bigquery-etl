WITH sessions_table AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    device.deviceCategory AS device_category,
    device.operatingSystem AS operating_system,
    device.language,
    device.browser,
    geoNetwork.country,
    trafficSource.source,
    trafficSource.medium,
    trafficSource.campaign,
    trafficSource.adContent AS ad_content,
    COUNT(*) AS sessions,
    COUNTIF(
      NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.browser)
    ) AS non_fx_sessions,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND totals.visits = 1
  GROUP BY
    date,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content
),
download_aggregates AS (
  SELECT
    date,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content,
    SUM(downloads) AS downloads,
    SUM(non_fx_downloads) AS non_fx_downloads,
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_downloads_v1`
  GROUP BY
    date,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content
),
site_data AS (
  SELECT
    sessions_table.date,
    sessions_table.device_category,
    sessions_table.operating_system,
    sessions_table.browser,
    sessions_table.language,
    sessions_table.country,
    sessions_table.source,
    sessions_table.medium,
    sessions_table.campaign,
    sessions_table.ad_content,
    SUM(sessions_table.sessions) AS sessions,
    SUM(sessions_table.non_fx_sessions) AS non_fx_sessions,
    SUM(download_aggregates.downloads) AS downloads,
    SUM(download_aggregates.non_fx_downloads) AS non_fx_downloads
  FROM
    sessions_table
  LEFT JOIN
    download_aggregates
    ON sessions_table.date = download_aggregates.date
    AND sessions_table.device_category = download_aggregates.device_category
    AND sessions_table.operating_system = download_aggregates.operating_system
    AND sessions_table.browser = download_aggregates.browser
    AND sessions_table.language = download_aggregates.language
    AND sessions_table.country = download_aggregates.country
    AND sessions_table.source = download_aggregates.source
    AND sessions_table.medium = download_aggregates.medium
    AND sessions_table.campaign = download_aggregates.campaign
    AND sessions_table.ad_content = download_aggregates.ad_content
  GROUP BY
    date,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content
)
SELECT
  date,
  device_category,
  operating_system,
  browser,
  `language`,
  country,
  standardized_country_list.standardized_country AS standardized_country_name,
  source,
  medium,
  campaign,
  ad_content,
  sessions,
  non_fx_sessions,
  downloads,
  non_fx_downloads
FROM
  site_data
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS standardized_country_list
  ON site_data.country = standardized_country_list.raw_country
