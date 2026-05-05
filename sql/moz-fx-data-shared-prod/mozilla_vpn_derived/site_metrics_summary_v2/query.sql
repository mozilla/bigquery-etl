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
    PARSE_DATE('%Y%m%d', event_date),
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
vpn_subscribe_goals_stg AS (
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
    ) AS non_fx_subscribe_intent_goal
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  LEFT JOIN
    UNNEST(items) i
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'begin_checkout'
    AND i.item_name = 'vpn'
  GROUP BY
    PARSE_DATE('%Y%m%d', event_date),
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
vpn_dl_goals AS (
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
    ) AS download_intent_goal,
    COUNT(
      DISTINCT(
        CASE
          WHEN (
              SELECT
                `value`
              FROM
                UNNEST(event_params)
              WHERE
                key = 'page_location'
              LIMIT
                1
            ).string_value LIKE "%/products/vpn/download/windows/thanks%"
            THEN user_pseudo_id || '-' || CAST(
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
          ELSE NULL
        END
      )
    ) AS download_installer_intent_goal
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    --page is the VPN download page
    AND (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ).string_value LIKE "%/products/vpn/download/%"
  GROUP BY
    PARSE_DATE('%Y%m%d', event_date),
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
)
SELECT
  COALESCE(COALESCE(ssns.`date`, vpn_sub_gls.`date`), vpn_dl_gls.`date`) AS `date`,
  COALESCE(
    COALESCE(ssns.device_category, vpn_sub_gls.device_category),
    vpn_dl_gls.device_category
  ) AS device_category,
  COALESCE(
    COALESCE(ssns.operating_system, vpn_sub_gls.operating_system),
    vpn_dl_gls.operating_system
  ) AS operating_system,
  COALESCE(COALESCE(ssns.browser, vpn_sub_gls.browser), vpn_dl_gls.browser) AS browser,
  COALESCE(COALESCE(ssns.`language`, vpn_sub_gls.`language`), vpn_dl_gls.`language`) AS `language`,
  COALESCE(COALESCE(ssns.country, vpn_sub_gls.country), vpn_dl_gls.country) AS country,
  COALESCE(COALESCE(ssns.source, vpn_sub_gls.source), vpn_dl_gls.source) AS source,
  COALESCE(COALESCE(ssns.medium, vpn_sub_gls.medium), vpn_dl_gls.medium) AS medium,
  COALESCE(COALESCE(ssns.campaign, vpn_sub_gls.campaign), vpn_dl_gls.campaign) AS campaign,
  COALESCE(COALESCE(ssns.content, vpn_sub_gls.content), vpn_dl_gls.content) AS content,
  COALESCE(COALESCE(ssns.`site`, vpn_sub_gls.`site`), vpn_dl_gls.`site`) AS `site`,
  ssns.sessions,
  ssns.non_fx_sessions,
  vpn_sub_gls.subscribe_intent_goal,
  vpn_sub_gls.non_fx_subscribe_intent_goal,
  NULL AS join_waitlist_intent_goal, --don't see any waitlist option in GA4 for VPN
  NULL AS join_waitlist_success_goal,  --don't see any waitlist option in GA4 for VPN
  NULL AS sign_in_intent_goal, --not sure how to add this yet
  vpn_dl_gls.download_intent_goal,
  vpn_dl_gls.download_installer_intent_goal,
  std_cntry.standardized_country AS standardized_country_name
FROM
  sessions_stg AS ssns
FULL OUTER JOIN
  vpn_subscribe_goals_stg AS vpn_sub_gls
  ON ssns.`date` = vpn_sub_gls.`date`
  AND COALESCE(ssns.device_category, 'Unknown') = COALESCE(vpn_sub_gls.device_category, 'Unknown')
  AND COALESCE(ssns.operating_system, 'Unknown') = COALESCE(vpn_sub_gls.operating_system, 'Unknown')
  AND COALESCE(ssns.browser, 'Unknown') = COALESCE(vpn_sub_gls.browser, 'Unknown')
  AND COALESCE(ssns.`language`, 'Unknown') = COALESCE(vpn_sub_gls.`language`, 'Unknown')
  AND COALESCE(ssns.country, 'Unknown') = COALESCE(vpn_sub_gls.country, 'Unknown')
  AND COALESCE(ssns.source, '') = COALESCE(vpn_sub_gls.source, '')
  AND COALESCE(ssns.medium, '') = COALESCE(vpn_sub_gls.medium, '')
  AND COALESCE(ssns.campaign, '') = COALESCE(vpn_sub_gls.campaign, '')
  AND COALESCE(ssns.content, '') = COALESCE(vpn_sub_gls.content, '')
  AND COALESCE(ssns.`site`, '') = COALESCE(vpn_sub_gls.`site`, '')
FULL OUTER JOIN
  vpn_dl_goals AS vpn_dl_gls
  ON COALESCE(ssns.`date`, vpn_sub_gls.`date`) = vpn_dl_gls.`date`
  AND COALESCE(COALESCE(ssns.device_category, vpn_sub_gls.device_category), 'Unknown') = COALESCE(
    vpn_dl_gls.device_category,
    'Unknown'
  )
  AND COALESCE(COALESCE(ssns.operating_system, vpn_sub_gls.operating_system), 'Unknown') = COALESCE(
    vpn_dl_gls.operating_system,
    'Unknown'
  )
  AND COALESCE(COALESCE(ssns.browser, vpn_sub_gls.browser), 'Unknown') = COALESCE(
    vpn_dl_gls.browser,
    'Unknown'
  )
  AND COALESCE(COALESCE(ssns.`language`, vpn_sub_gls.`language`), 'Unknown') = COALESCE(
    vpn_dl_gls.`language`,
    'Unknown'
  )
  AND COALESCE(COALESCE(ssns.country, vpn_sub_gls.country), 'Unknown') = COALESCE(
    vpn_dl_gls.country,
    'Unknown'
  )
  AND COALESCE(COALESCE(ssns.source, vpn_sub_gls.source), '') = COALESCE(vpn_dl_gls.source, '')
  AND COALESCE(COALESCE(ssns.medium, vpn_sub_gls.medium), '') = COALESCE(vpn_dl_gls.medium, '')
  AND COALESCE(COALESCE(ssns.campaign, vpn_sub_gls.campaign), '') = COALESCE(
    vpn_dl_gls.campaign,
    ''
  )
  AND COALESCE(COALESCE(ssns.content, vpn_sub_gls.content), '') = COALESCE(vpn_dl_gls.content, '')
  AND COALESCE(COALESCE(ssns.`site`, vpn_sub_gls.`site`), '') = COALESCE(vpn_dl_gls.`site`, '')
LEFT OUTER JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS std_cntry
  ON COALESCE(
    COALESCE(ssns.country, vpn_sub_gls.country),
    vpn_dl_gls.country
  ) = std_cntry.raw_country
