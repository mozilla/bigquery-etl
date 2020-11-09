SELECT
  date,
  event_category,
  event_action,
  event_label,
  page_path,
  page_path_level1 AS locale,
  page_level_1,
  page_level_2,
  page_level_3,
  page_level_4,
  page_level_5,
  page_name,
  device_category,
  operating_system,
  `language`,
  browser,
  country,
  source,
  medium,
  campaign,
  ad_content,
  COUNT(*) AS total_events,
  COUNT(DISTINCT visit_identifier) AS unique_events,
FROM
  `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
WHERE
  date = @submission_date
  AND hit_type = 'EVENT'
  AND visits = 1
  AND event_category IS NOT NULL
GROUP BY
  date,
  event_category,
  event_action,
  event_label,
  page_path,
  locale,
  page_level_1,
  page_level_2,
  page_level_3,
  page_level_4,
  page_level_5,
  device_category,
  operating_system,
  `language`,
  browser,
  country,
  source,
  medium,
  campaign,
  ad_content,
  page_name
