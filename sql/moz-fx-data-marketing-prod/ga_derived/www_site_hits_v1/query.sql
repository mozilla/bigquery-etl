WITH hits AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(CAST(fullVisitorId AS STRING), CAST(visitId AS STRING)) AS visit_identifier,
    fullVisitorId AS full_visitor_id,
    visitStartTime AS visit_start_time,
    hit.page.pagePath AS page_path,
    hit.page.pagePathLevel1 AS page_path_level1,
    -- splitting the pagePath to make it easier to filter on pages in dashboards
    SPLIT(split(hit.page.pagePath, '?')[offset(0)], '/') AS split_page_path,
    hit.type AS hit_type,
    hit.isExit AS is_exit,
    hit.isEntrance AS is_entrance,
    hit.hitNumber AS hit_number,
    hit.eventInfo.eventCategory AS event_category,
    hit.eventInfo.eventLabel AS event_label,
    hit.eventInfo.eventAction AS event_action,
    device.deviceCategory AS device_category,
    device.operatingSystem AS operating_system,
    device.language,
    device.browser,
    SPLIT(device.browserVersion, '.')[offset(0)] AS browser_version,
    geoNetwork.country,
    trafficSource.source,
    trafficSource.medium,
    trafficSource.campaign,
    trafficSource.adContent AS ad_content,
    totals.visits,
    totals.bounces,
    hit.time / 1000 AS hit_time,
    MIN(IF(hit.isInteraction IS NOT NULL, hit.hitNumber, 0)) OVER (
      PARTITION BY
        fullVisitorId,
        visitStartTime
    ) AS first_interaction,
    MAX(IF(hit.isInteraction IS NOT NULL, hit.time / 1000, 0)) OVER (
      PARTITION BY
        fullVisitorId,
        visitStartTime
    ) AS last_interaction,
    IF(hit.isEntrance IS NOT NULL, 1, 0) AS entrances,
    IF(hit.isExit IS NOT NULL, 1, 0) AS exits,
    CONCAT(
      hit.eventInfo.eventCategory,
      COALESCE(hit.eventInfo.eventaction, ''),
      COALESCE(hit.eventInfo.eventLabel, '')
    ) AS event_id,
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
  CROSS JOIN
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
),
page_levels AS (
  SELECT
    * EXCEPT (split_page_path),
    split_page_path[SAFE_OFFSET(2)] AS page_level_1,
    split_page_path[SAFE_OFFSET(3)] AS page_level_2,
    split_page_path[SAFE_OFFSET(4)] AS page_level_3,
    split_page_path[SAFE_OFFSET(5)] AS page_level_4,
    split_page_path[SAFE_OFFSET(6)] AS page_level_5,
  FROM
    hits
)
SELECT
  *,
  -- Page name without locale and query string
  IF(
    page_level_2 IS NULL,
    CONCAT('/', page_level_1, '/'),
    ARRAY_TO_STRING(['', page_level_1, page_level_2, page_level_3, page_level_4, page_level_5], '/')
  ) AS page_name,
FROM
  page_levels
