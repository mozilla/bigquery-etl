WITH firefox_desktop_downloads_stg AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS `language`,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    event_name,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'product'
      LIMIT
        1
    ).string_value AS product_type,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'platform'
      LIMIT
        1
    ).string_value AS platform_type
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND _TABLE_SUFFIX <= '20240216'
    AND event_name = 'product_download'
),
firefox_desktop_downloads AS (
  --use this logic on or before 2024-02-16
  SELECT
    `date`,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content,
    COUNTIF(NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser)) AS non_fx_downloads,
    COUNT(1) AS downloads
  FROM
    firefox_desktop_downloads_stg
  WHERE
    product_type = 'firefox'
    AND platform_type IN (
      'win',
      'win64',
      'macos',
      'linux64',
      'win64-msi',
      'linux',
      'win-msi',
      'win64-aarch64'
    )
  GROUP BY
    `date`,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content
  UNION ALL
    --use this logic on & after 2024-02-17
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS `language`,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    COUNTIF(
      NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser)
    ) AS non_fx_downloads,
    COUNT(1) AS downloads
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'firefox_download'
    AND _TABLE_SUFFIX >= '20240217'
  GROUP BY
    event_date,
    device.category,
    device.operating_system,
    device.web_info.browser,
    device.language,
    geo.country,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    collected_traffic_source.manual_campaign_name,
    collected_traffic_source.manual_content
),
sessions_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS `language`,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    COUNTIF(event_name = 'session_start') AS sessions,
    COUNTIF(
      event_name = 'session_start'
      AND NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser)
    ) AS non_fx_sessions
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    event_date,
    device.category,
    device.operating_system,
    device.web_info.browser,
    device.language,
    geo.country,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    collected_traffic_source.manual_campaign_name,
    collected_traffic_source.manual_content
),
sessions_and_downloads_combined AS (
  SELECT
    COALESCE(sess.date, dl.date) AS date,
    COALESCE(sess.device_category, dl.device_category) AS device_category,
    COALESCE(sess.operating_system, dl.operating_system) AS operating_system,
    COALESCE(sess.browser, dl.browser) AS browser,
    COALESCE(sess.language, dl.language) AS language,
    COALESCE(sess.country, dl.country) AS country,
    COALESCE(sess.source, dl.source) AS source,
    COALESCE(sess.medium, dl.medium) AS medium,
    COALESCE(sess.campaign, dl.campaign) AS campaign,
    COALESCE(sess.ad_content, dl.ad_content) AS ad_content,
    COALESCE(sess.sessions, 0) AS sessions,
    COALESCE(sess.non_fx_sessions, 0) AS non_fx_sessions,
    COALESCE(dl.downloads, 0) AS downloads,
    COALESCE(dl.non_fx_downloads, 0) AS non_fx_downloads
  FROM
    sessions_data sess
  FULL OUTER JOIN
    firefox_desktop_downloads dl
    ON sess.date = dl.date
    AND COALESCE(sess.device_category, '') = COALESCE(dl.device_category, '')
    AND COALESCE(sess.operating_system, '') = COALESCE(dl.operating_system, '')
    AND COALESCE(sess.browser, '') = COALESCE(dl.browser, '')
    AND COALESCE(sess.language, '') = COALESCE(dl.language, '')
    AND COALESCE(sess.country, '') = COALESCE(dl.country, '')
    AND COALESCE(sess.source, 'NA') = COALESCE(dl.source, 'NA')
    AND COALESCE(sess.medium, 'NA') = COALESCE(dl.medium, 'NA')
    AND COALESCE(sess.campaign, 'NA') = COALESCE(dl.campaign, 'NA')
    AND COALESCE(sess.ad_content, 'NA') = COALESCE(dl.ad_content, 'NA')
)
SELECT
  s.date,
  s.device_category,
  s.operating_system,
  s.browser,
  s.language,
  s.country,
  std_cntry_nms.standardized_country AS standardized_country_name,
  s.source,
  s.medium,
  s.campaign,
  s.ad_content,
  s.sessions,
  s.non_fx_sessions,
  s.downloads,
  s.non_fx_downloads
FROM
  sessions_and_downloads_combined AS s
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS std_cntry_nms
  ON s.country = std_cntry_nms.raw_country
