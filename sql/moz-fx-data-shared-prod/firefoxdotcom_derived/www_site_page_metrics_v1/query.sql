--first, calculate the next page view's start time relative to when the session started in seconds using lead
WITH page_view_staging AS (
  SELECT
    *,
    LEAD(hit_time) OVER (
      PARTITION BY
        full_visitor_id,
        visit_start_time
      ORDER BY
        hit_time
    ) AS next_pageview,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND hit_type = 'PAGE'
),
--now, subtract current page view start time from next page view start time to get time on current page
page_views_only AS (
  SELECT
    pvs.date,
    pvs.page_path AS page,
    pvs.page_path_level1 AS locale,
    pvs.page_name,
    pvs.page_level_1,
    pvs.page_level_2,
    pvs.page_level_3,
    pvs.page_level_4,
    pvs.page_level_5,
    pvs.device_category,
    pvs.operating_system,
    pvs.language,
    pvs.browser,
    pvs.browser_version,
    pvs.country,
    pvs.source,
    pvs.medium,
    pvs.campaign,
    pvs.ad_content,
    SUM(pvs.next_pageview - pvs.hit_time) AS total_time_on_page
  FROM
    page_view_staging AS pvs
  GROUP BY
    pvs.date,
    pvs.page_path,
    pvs.page_path_level1,
    pvs.page_name,
    pvs.page_level_1,
    pvs.page_level_2,
    pvs.page_level_3,
    pvs.page_level_4,
    pvs.page_level_5,
    pvs.device_category,
    pvs.operating_system,
    pvs.language,
    pvs.browser,
    pvs.browser_version,
    pvs.country,
    pvs.source,
    pvs.medium,
    pvs.campaign,
    pvs.ad_content
),
all_events_staging AS (
  SELECT
    site_hits.date,
    site_hits.page_path AS page,
    site_hits.page_path_level1 AS locale,
    site_hits.page_name,
    site_hits.page_level_1,
    site_hits.page_level_2,
    site_hits.page_level_3,
    site_hits.page_level_4,
    site_hits.page_level_5,
    site_hits.device_category,
    site_hits.operating_system,
    site_hits.language,
    site_hits.browser,
    site_hits.browser_version,
    site_hits.country,
    site_hits.source,
    site_hits.medium,
    site_hits.campaign,
    site_hits.ad_content,
    COUNTIF(site_hits.event_name = 'page_view') AS pageviews,
    COUNT(
      DISTINCT(
        CASE
          WHEN site_hits.event_name = 'page_view'
            THEN site_hits.visit_identifier
          ELSE NULL
        END
      )
    ) AS unique_pageviews,
    SUM(site_hits.entrances) AS entrances,
    SUM(site_hits.exits) AS exits,
    COUNTIF(event_name = 'page_view' AND is_exit IS FALSE) AS non_exit_pageviews,
    COUNTIF(hit_type = 'EVENT') AS total_events,
    COUNT(
      DISTINCT(CASE WHEN hit_type = 'EVENT' THEN visit_identifier ELSE NULL END)
    ) AS unique_events,
    COUNT(
      DISTINCT(CASE WHEN single_page_session IS TRUE THEN visit_identifier ELSE NULL END)
    ) AS single_page_sessions,
    COUNT(
      DISTINCT(
        CASE
          WHEN bounces = 1
            AND event_name = 'page_view'
            THEN visit_identifier
          ELSE NULL
        END
      )
    ) AS bounces
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.www_site_hits_v1` AS site_hits
  WHERE
    date = @submission_date
  GROUP BY
    site_hits.date,
    site_hits.page_path,
    site_hits.page_path_level1,
    site_hits.page_name,
    site_hits.page_level_1,
    site_hits.page_level_2,
    site_hits.page_level_3,
    site_hits.page_level_4,
    site_hits.page_level_5,
    site_hits.device_category,
    site_hits.operating_system,
    site_hits.language,
    site_hits.browser,
    site_hits.browser_version,
    site_hits.country,
    site_hits.source,
    site_hits.medium,
    site_hits.campaign,
    site_hits.ad_content
)
--join it all together to get everything plus total time on each page
SELECT
  aes.date,
  aes.page,
  aes.locale,
  aes.page_name,
  aes.page_level_1,
  aes.page_level_2,
  aes.page_level_3,
  aes.page_level_4,
  aes.page_level_5,
  aes.device_category,
  aes.operating_system,
  aes.language,
  aes.browser,
  aes.browser_version,
  aes.country,
  aes.source,
  aes.medium,
  aes.campaign,
  aes.ad_content,
  aes.pageviews,
  aes.unique_pageviews,
  aes.entrances,
  aes.exits,
  aes.non_exit_pageviews,
  pvo.total_time_on_page,
  aes.total_events,
  aes.unique_events,
  aes.single_page_sessions,
  aes.bounces,
FROM
  all_events_staging AS aes
FULL OUTER JOIN
  page_views_only AS pvo
  ON aes.date = pvo.date
  AND COALESCE(aes.page, '') = COALESCE(pvo.page, '')
  AND COALESCE(aes.locale, '') = COALESCE(pvo.locale, '')
  AND COALESCE(aes.page_name, '') = COALESCE(pvo.page_name, '')
  AND COALESCE(aes.page_level_1, '') = COALESCE(pvo.page_level_1, '')
  AND COALESCE(aes.page_level_2, '') = COALESCE(pvo.page_level_2, '')
  AND COALESCE(aes.page_level_3, '') = COALESCE(pvo.page_level_3, '')
  AND COALESCE(aes.page_level_4, '') = COALESCE(pvo.page_level_4, '')
  AND COALESCE(aes.page_level_5, '') = COALESCE(pvo.page_level_5, '')
  AND COALESCE(aes.device_category, '') = COALESCE(pvo.device_category, '')
  AND COALESCE(aes.operating_system, '') = COALESCE(pvo.operating_system, '')
  AND COALESCE(aes.language, '') = COALESCE(pvo.language, '')
  AND COALESCE(aes.browser, '') = COALESCE(pvo.browser, '')
  AND COALESCE(aes.browser_version, '') = COALESCE(pvo.browser_version, '')
  AND COALESCE(aes.country, '') = COALESCE(pvo.country, '')
  AND COALESCE(aes.source, '') = COALESCE(pvo.source, '')
  AND COALESCE(aes.medium, '') = COALESCE(pvo.medium, '')
  AND COALESCE(aes.campaign, '') = COALESCE(pvo.campaign, '')
  AND COALESCE(aes.ad_content, '') = COALESCE(pvo.ad_content, '')
