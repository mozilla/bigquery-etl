CREATE TEMP FUNCTION udf_ga_sessions(ga_sessions ANY TYPE, site STRING) AS (
  STRUCT(
    CONCAT(ga_sessions.fullVisitorId, ga_sessions.visitId) AS visit_identifier,
    ga_sessions.device.deviceCategory AS device_category,
    ga_sessions.device.operatingSystem AS operating_system,
    ga_sessions.device.browser,
    ga_sessions.device.language,
    ga_sessions.geoNetwork.country,
    ga_sessions.trafficSource.source,
    ga_sessions.trafficSource.medium,
    ga_sessions.trafficSource.campaign,
    ga_sessions.trafficSource.adcontent AS content,
    site,
    ga_sessions.totals.visits,
    ARRAY(
      SELECT AS STRUCT
        type AS hit_type,
        eventInfo.eventCategory AS hit_event_category,
        eventInfo.eventAction AS hit_event_action,
        eventInfo.eventLabel AS hit_event_label,
        IF(
          site = "vpn.mozilla.org",
          STRUCT(
            CAST(NULL AS STRING) AS source_param,
            eventInfo.eventLabel LIKE "%Subscribe%" AS subscribe_intent,
            eventInfo.eventLabel LIKE "%Join%" AS join_waitlist_intent,
            eventInfo.eventLabel LIKE "%Submit waitlist form%" AS join_waitlist_success,
            eventInfo.eventLabel LIKE "%Sign in%" AS sign_in_intent,
            eventInfo.eventLabel LIKE "%Download%" AS download_intent,
            eventInfo.eventLabel LIKE "%Download installer%" AS download_installer_intent
          ),
          STRUCT(
            REGEXP_EXTRACT(page.pagePath, "[?&]source=((?:whatsnew|welcome)[^&]+)") AS source_param,
            eventInfo.eventAction = "cta: fxa-vpn"
            AND (
              eventInfo.eventLabel = "Try Mozilla VPN"
              OR eventInfo.eventLabel LIKE "Get Mozilla VPN %"
            ) AS subscribe_intent,
            eventInfo.eventAction = "cta: button"
            AND eventInfo.eventLabel = "Join the VPN Waitlist" AS join_waitlist_intent,
            eventInfo.eventAction = "newsletter subscription"
            AND eventInfo.eventLabel = "guardian-vpn-waitlist" AS join_waitlist_success,
            eventInfo.eventAction = "cta: fxa-vpn"
            AND eventInfo.eventLabel = "VPN Sign In" AS sign_in_intent,
                -- Downloading clients does not happen on mozilla.org
            FALSE AS download_intent,
            FALSE AS download_installer_intent
          )
        ).*
      FROM
        UNNEST(ga_sessions.hits)
      WHERE
        site = "vpn.mozilla.org"
        OR (site = "mozilla.org" AND page.pagePath LIKE "%/products/vpn/%")
    ) AS hits
  )
);

WITH ga_sessions AS (
  SELECT
    -- keep date out of UDF to preserve suffix pruning
    SAFE.PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS `date`,
    udf_ga_sessions(ga_sessions, "mozilla.org").*,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*` AS ga_sessions
  UNION ALL
  SELECT
    -- keep date out of UDF to preserve suffix pruning
    SAFE.PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS `date`,
    udf_ga_sessions(ga_sessions, "vpn.mozilla.org").*,
  FROM
    `moz-fx-data-marketing-prod.220432379.ga_sessions_*` AS ga_sessions
),
base_table AS (
  SELECT
    * EXCEPT (source_param, campaign, content, medium, source, hits),
    IF(
      source_param IS NOT NULL,
      STRUCT(
        source_param AS campaign,
        "(not set)" AS content,
        "(none)" AS medium,
        CONCAT("www.mozilla.org-", REGEXP_EXTRACT(source_param, "^[^0-9]+")) AS source
      ),
      STRUCT(campaign, content, medium, source)
    ).*,
  FROM
    ga_sessions
  CROSS JOIN
    UNNEST(hits)
  WHERE
    `date` IS NOT NULL
    AND `date` = @date
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
    site
  )
LEFT JOIN
  `moz-fx-data-shared-prod`.static.third_party_standardized_country_names
  ON sessions_table.country = third_party_standardized_country_names.raw_country
