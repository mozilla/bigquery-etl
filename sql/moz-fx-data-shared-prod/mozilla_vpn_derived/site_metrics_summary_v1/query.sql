WITH ga_sessions_union AS (
  SELECT
    ga_sessions.* REPLACE (SAFE.PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS `date`),
    "mozilla.org" AS site,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*` AS ga_sessions
  UNION ALL
  SELECT
    ga_sessions.* REPLACE (SAFE.PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS `date`),
    "vpn.mozilla.org" AS site,
  FROM
    `moz-fx-data-marketing-prod.220432379.ga_sessions_*` AS ga_sessions
),
base_table AS (
  SELECT
    `date`,
    CONCAT(fullVisitorId, visitId) AS visit_identifier,
    device.deviceCategory AS device_category,
    device.operatingSystem AS operating_system,
    device.browser,
    device.language,
    geoNetwork.country,
    trafficSource.source,
    trafficSource.medium,
    trafficSource.campaign,
    trafficSource.adcontent AS content,
    site,
    totals.visits,
    hits.type AS hit_type,
    hits.eventInfo.eventCategory AS hit_event_category,
    hits.eventInfo.eventAction AS hit_event_action,
    hits.eventInfo.eventLabel AS hit_event_label,
    IF(
      site = "vpn.mozilla.org",
      STRUCT(
        hits.eventInfo.eventLabel LIKE "%Subscribe%" AS subscribe_intent,
        hits.eventInfo.eventLabel LIKE "%Join%" AS join_waitlist_intent,
        hits.eventInfo.eventLabel LIKE "%Submit waitlist form%" AS join_waitlist_success,
        hits.eventInfo.eventLabel LIKE "%Sign in%" AS sign_in_intent,
        hits.eventInfo.eventLabel LIKE "%Download%" AS download_intent,
        hits.eventInfo.eventLabel LIKE "%Download installer%" AS download_installer_intent
      ),
      STRUCT(
        hits.eventInfo.eventAction = "cta: fxa-vpn"
        AND (
          hits.eventInfo.eventLabel = "Try Mozilla VPN"
          OR hits.eventInfo.eventLabel LIKE "Get Mozilla VPN %"
        ) AS subscribe_intent,
        hits.eventInfo.eventAction = "cta: button"
        AND hits.eventInfo.eventLabel = "Join the VPN Waitlist" AS join_waitlist_intent,
        hits.eventInfo.eventAction = "newsletter subscription"
        AND hits.eventInfo.eventLabel = "guardian-vpn-waitlist" AS join_waitlist_success,
        hits.eventInfo.eventAction = "cta: fxa-vpn"
        AND hits.eventInfo.eventLabel = "VPN Sign In" AS sign_in_intent,
        -- Downloading clients does not happen on mozilla.org
        FALSE AS download_intent,
        FALSE AS download_installer_intent
      )
    ).*
  FROM
    ga_sessions_union
  CROSS JOIN
    UNNEST(hits) AS hits
  WHERE
    `date` IS NOT NULL
    AND `date` = @date
    AND (
      site = "vpn.mozilla.org"
      OR (site = "mozilla.org" AND hits.page.pagePath LIKE "%/products/vpn/%")
    )
),
sessions_table AS (
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
    content,
    site,
    COUNT(DISTINCT visit_identifier) AS sessions,
    COUNT(DISTINCT IF(browser != "Firefox", visit_identifier, NULL)) AS non_fx_sessions,
  FROM
    base_table
  WHERE
    visits = 1
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
    content,
    site
),
goals_table AS (
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
    content,
    site,
    COUNT(DISTINCT IF(subscribe_intent, visit_identifier, NULL)) AS subscribe_intent_goal,
    COUNT(
      DISTINCT IF(subscribe_intent AND browser != "Firefox", visit_identifier, NULL)
    ) AS non_fx_subscribe_intent_goal,
    COUNT(DISTINCT IF(join_waitlist_intent, visit_identifier, NULL)) AS join_waitlist_intent_goal,
    COUNT(DISTINCT IF(join_waitlist_success, visit_identifier, NULL)) AS join_waitlist_success_goal,
    COUNT(DISTINCT IF(sign_in_intent, visit_identifier, NULL)) AS sign_in_intent_goal,
    COUNT(DISTINCT IF(download_intent, visit_identifier, NULL)) AS download_intent_goal,
    COUNT(
      DISTINCT IF(download_installer_intent, visit_identifier, NULL)
    ) AS download_installer_intent_goal,
  FROM
    base_table
  WHERE
    hit_type = "EVENT"
    AND hit_event_category IS NOT NULL
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
    content,
    site
)
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
  content,
  site,
  sessions,
  IF(browser != "Firefox", sessions, 0) AS non_fx_sessions,
  subscribe_intent_goal,
  IF(browser != 'Firefox', subscribe_intent_goal, 0) AS non_fx_subscribe_intent_goal,
  join_waitlist_intent_goal,
  join_waitlist_success_goal,
  sign_in_intent_goal,
  download_intent_goal,
  download_installer_intent_goal,
  third_party_standardized_country_names.standardized_country AS standardized_country_name,
FROM
  sessions_table
LEFT JOIN
  goals_table
USING
  (
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
    site
  )
LEFT JOIN
  `moz-fx-data-shared-prod`.static.third_party_standardized_country_names
ON
  sessions_table.country = third_party_standardized_country_names.raw_country
