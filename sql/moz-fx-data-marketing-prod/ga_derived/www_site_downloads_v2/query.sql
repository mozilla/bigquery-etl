WITH staging AS (
  SELECT
    `date`,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content,
    browser,
  --note: the 2 columns are the same because in GA4, there is no logic saying you can only count 1 download per session, unlike GA3
    COUNTIF(
    --prior to and including 2/16/24
      (
        `date` <= '2024-02-16'
        AND event_name = 'product_download'
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
        AND product_type = 'firefox'
      )
      OR
    --on and after 2/17/24
      (`date` >= '2024-02-17' AND event_name = 'firefox_download')
    ) AS download_events
  FROM
    (
      SELECT
        PARSE_DATE('%Y%m%d', event_date) AS `date`,
        user_pseudo_id || '-' || CAST(e.value.int_value AS string) AS visit_identifier,
        device.category AS device_category,
        device.operating_system AS operating_system,
        device.language AS `language`,
        geo.country AS country,
        collected_traffic_source.manual_source AS source,
        collected_traffic_source.manual_medium AS medium,
        collected_traffic_source.manual_campaign_name AS campaign,
        collected_traffic_source.manual_content AS ad_content,
        device.web_info.browser AS browser,
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
      JOIN
        UNNEST(event_params) AS e
      WHERE
        _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
        AND e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
    ) stg
  GROUP BY
    `date`,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    country,
    source,
    medium,
    campaign,
    ad_content,
    browser
)
SELECT
  `date`,
  visit_identifier,
  device_category,
  operating_system,
  `language`,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser,
  download_events,
  download_events AS downloads,
  IF(
    NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser),
    download_events,
    0
  ) AS non_fx_downloads
FROM
  staging
