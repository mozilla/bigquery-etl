WITH sessions_stg AS (
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
    collected_traffic_source.manual_content AS content,
    'mozilla.org' AS `site`,
    COUNT(
      DISTINCT(
        user_pseudo_id || '-' || CAST(
          (
            SELECT
              `value`
            FROM
              UNNEST(event_params)
            WHERE
              key = 'ga_session_id'
            LIMIT
              1
          ).int_value AS STRING
        )
      )
    ) AS sessions,
    IF(
      NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser),
      COUNT(
        DISTINCT(
          user_pseudo_id || '-' || CAST(
            (
              SELECT
                `value`
              FROM
                UNNEST(event_params)
              WHERE
                key = 'ga_session_id'
              LIMIT
                1
            ).int_value AS STRING
          )
        )
      ),
      0
    ) AS non_fx_sessions
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    --filter to only pages relative to VPN
    AND (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ).string_value LIKE "%/products/vpn/%"
  GROUP BY
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
    device.category,
    device.operating_system,
    device.web_info.browser,
    device.language,
    geo.country,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    collected_traffic_source.manual_campaign_name,
    collected_traffic_source.manual_content,
    `site`
),
goals_stg AS (
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
    collected_traffic_source.manual_content AS content,
    'mozilla.org' AS `site`,
    COUNT(
      DISTINCT(
        user_pseudo_id || '-' || CAST(
          (
            SELECT
              `value`
            FROM
              UNNEST(event_params)
            WHERE
              key = 'ga_session_id'
            LIMIT
              1
          ).int_value AS STRING
        )
      )
    ) AS subscribe_intent_goal,
    IF(
      NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(device.web_info.browser),
      COUNT(
        DISTINCT(
          user_pseudo_id || '-' || CAST(
            (
              SELECT
                `value`
              FROM
                UNNEST(event_params)
              WHERE
                key = 'ga_session_id'
              LIMIT
                1
            ).int_value AS STRING
          )
        )
      ),
      0
    ) AS non_fx_subscribe_intent_goal,
    --fix below here
    join_waitlist_intent_goal,
    join_waitlist_success_goal,
    sign_in_intent_goal,
    download_intent_goal,
    download_installer_intent_goal
    --fix above here
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  LEFT JOIN
    UNNEST(items) i
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'begin_checkout'
    AND i.item_name = 'vpn'
)
SELECT
  ssns.`date`,
  ssns.device_category,
  ssns.operating_system,
  ssns.browser,
  ssns.`language`,
  ssns.country,
  ssns.source,
  ssns.medium,
  ssns.campaign,
  ssns.content,
  ssns.`site`,
  stg.sessions,
  ssns.non_fx_sessions,
  gls.subscribe_intent_goal,
  gls.non_fx_subscribe_intent_goal,
  gls.join_waitlist_intent_goal,
  gls.join_waitlist_success_goal,
  gls.sign_in_intent_goal,
  gls.download_intent_goal,
  gls.download_installer_intent_goal,
  std_cntry.standardized_country AS standardized_country_name
FROM
  sessions_stg ssns
LEFT JOIN
  goals_stg gls
  USING (
    `date`,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    content,
    `site`
  )
LEFT OUTER JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` std_cntry
  ON stg.country = std_cntry.raw_country
