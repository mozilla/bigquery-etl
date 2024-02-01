--first, calculate the next page view's start time relative to when the session started in seconds using lead
with page_view_staging AS (

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
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v2`
  WHERE
    date = @submission_date
    AND hit_type = 'PAGE'
),

--now, subtract current page view start time from next page view start time to get time on current page
page_views_only  AS (
  SELECT 
  a.date,
  a.page_path AS page,
  a.page_path_level1 AS locale,
  a.page_name,
  a.page_level_1,
  a.page_level_2,
  a.page_level_3,
  a.page_level_4,
  a.page_level_5,
  a.device_category,
  a.operating_system,
  a.language,
  a.browser,
  a.browser_version,
  a.country,
  a.source,
  a.medium,
  a.campaign,
  a.ad_content,
  sum(a.next_pageview - a.hit_time) AS total_time_on_page
  FROM page_view_staging
  GROUP BY 
  a.date,
  a.page_path,
  a.page_path_level1,
  a.page_name,
  a.page_level_1,
  a.page_level_2,
  a.page_level_3,
  a.page_level_4,
  a.page_level_5,
  a.device_category,
  a.operating_system,
  a.language,
  a.browser,
  a.browser_version,
  a.country,
  a.source,
  a.medium,
  a.campaign,
  a.ad_content
)



all_events_staging AS (
SELECT
  a.date,
  a.page_path AS page,
  a.page_path_level1 AS locale,
  a.page_name,
  a.page_level_1,
  a.page_level_2,
  a.page_level_3,
  a.page_level_4,
  a.page_level_5,
  a.device_category,
  a.operating_system,
  a.language,
  a.browser,
  a.browser_version,
  a.country,
  a.source,
  a.medium,
  a.campaign,
  a.ad_content,
  COUNTIF(a.event_name = 'page_view') AS pageviews,
  COUNT(
    DISTINCT(CASE WHEN a.event_name = 'page_view' THEN a.visit_identifier ELSE NULL END)
  ) AS unique_pageviews,
  SUM(a.entrances) AS entrances,
  SUM(a.exits) AS exits,
  COUNTIF(event_name = 'page_view' AND is_exit IS FALSE) AS non_exit_pageviews,
  COUNTIF(hit_type = 'EVENT') AS total_events,
  COUNT(
    DISTINCT(CASE WHEN hit_type = 'EVENT' THEN visit_identifier ELSE NULL END)
  ) AS unique_events,
  COUNT(
    DISTINCT(CASE WHEN single_page_session IS TRUE THEN visit_identifier ELSE NULL END)
  ) AS single_page_sessions,
  COUNT(
    DISTINCT(CASE WHEN bounces = 1 AND event_name = 'page_view' THEN visit_identifier ELSE NULL END)
  ) AS bounces
FROM
  `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v2` AS a
WHERE
  date = @submission_date
GROUP BY
  a.date,
  a.page_path,
  a.page_path_level1,
  a.page_name,
  a.page_level_1,
  a.page_level_2,
  a.page_level_3,
  a.page_level_4,
  a.page_level_5,
  a.device_category,
  a.operating_system,
  a.language,
  a.browser,
  a.browser_version,
  a.country,
  a.source,
  a.medium,
  a.campaign,
  a.ad_content
) 

--join it all together to get everything plus total time on each page 

SELECT 
FROM 
all_events_staging a 
FULL OUTER JOIN 
page_views_only b 
ON a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?
AND a.? = b.?