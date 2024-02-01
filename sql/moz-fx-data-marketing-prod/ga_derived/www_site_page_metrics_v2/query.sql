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
  ? AS total_time_on_page,
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
