





SELECT 
a.date,
a.page_path AS page,
a.page_path_level1 AS locale,
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
countif(a.event_name = 'page_view') AS pageviews, 
count(distinct(CASE WHEN a.event_name = 'page_view' THEN a.visit_identifier ELSE NULL END)) AS unique_pageviews, 
sum(a.entrances) AS entrances, 
sum(a.exits) AS exits, 
? AS non_exit_pageviews,
? AS total_time_on_page, 
? AS total_events, 
? AS unique_events,
? AS single_page_sessions,
? AS bounces,
? AS page_name 
FROM `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v2` AS a
WHERE date = @submission_date 
GROUP BY 
a.date,
a.page_path,
a.page_path_level1,
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
