WITH page_hits AS (
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
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND hit_type = 'PAGE'
),
page_hits_with_metrics AS (
  SELECT
    * EXCEPT (bounces),
    IF(hit_number = first_interaction, visits, 0) AS single_page_sessions,
    IF(hit_number = first_interaction, bounces, 0) AS bounces,
    IF(is_exit IS NOT NULL, last_interaction - hit_time, next_pageview - hit_time) AS time_on_page
  FROM
    page_hits
),
page_metrics_table AS (
  -- Calculate time on page
  SELECT
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content,
    COUNT(*) AS pageviews,
    COUNT(DISTINCT CONCAT(visit_identifier, page_path)) AS unique_pageviews,
    SUM(IF(is_entrance IS NOT NULL, 1, 0)) AS entrances,
    SUM(IF(is_exit IS NOT NULL, 1, 0)) AS exits,
    -- Single Page sessions used to calculate bounce rate
    SUM(single_page_sessions) AS single_page_sessions,
    SUM(bounces) AS bounces,
    SUM(time_on_page) AS total_time_on_page
  FROM
    page_hits_with_metrics
  GROUP BY
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content
),
bounce_aggregates AS (
  SELECT
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content,
    SUM(IF(hit_number = first_interaction, visits, 0)) AS single_page_sessions,
    SUM(IF(hit_number = first_interaction, bounces, 0)) AS bounces,
    SUM(exits) AS exits,
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
  GROUP BY
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content
),
events_table AS (
  SELECT
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content,
    COUNT(*) AS total_events,
    COUNT(DISTINCT CONCAT(visit_identifier, event_id)) AS unique_events
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND hit_type = 'EVENT'
    AND event_category IS NOT NULL
  GROUP BY
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content
),
joined_table AS (
  SELECT
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    page_metrics_table.ad_content,
    pageviews,
    unique_pageviews,
    entrances,
    exits,
    total_time_on_page,
    -- non_exit_pageviews -> Denominator for avg time on page calculation
    (page_metrics_table.pageviews - page_metrics_table.exits) AS non_exit_pageviews,
    -- Single Page sessions used to calculate bounce rate
    NULL AS single_page_sessions,
    NULL AS bounces,
    NULL AS total_events,
    NULL AS unique_events
  FROM
    page_metrics_table
  UNION ALL
  SELECT
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content,
    NULL AS pageviews,
    NULL AS unique_pageviews,
    NULL AS entrances,
    NULL AS exits,
    NULL AS total_time_on_page,
    -- non_exit_pageviews -> Denominator for avg time on page calculation
    NULL AS non_exit_pageviews,
    -- Single Page sessions used to calculate bounce rate
    single_page_sessions,
    bounces,
    NULL AS total_events,
    NULL AS unique_events,
  FROM
    bounce_aggregates
  UNION ALL
  SELECT
    date,
    page_path,
    page_path_level1,
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
    browser_version,
    country,
    source,
    medium,
    campaign,
    ad_content,
    NULL AS pageviews,
    NULL AS unique_pageviews,
    NULL AS entrances,
    NULL AS exits,
    NULL AS total_time_on_page,
    -- non_exit_pageviews -> Denominator for avg time on page calculation
    NULL AS non_exit_pageviews,
    -- Single Page sessions used to calculate bounce rate
    NULL AS single_page_sessions,
    NULL AS bounces,
    total_events,
    unique_events,
  FROM
    events_table
)
SELECT
  date,
  page_path AS page,
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
  browser_version,
  country,
  source,
  medium,
  campaign,
  ad_content,
  SUM(pageviews) AS pageviews,
  SUM(unique_pageviews) AS unique_pageviews,
  SUM(entrances) AS entrances,
  -- Divide exits by pageviews to get exit rate
  SUM(exits) AS exits,
  -- Non exit pageviews used to calculate avg time on page -> Total time on page / non exit pageviews
  SUM(non_exit_pageviews) AS non_exit_pageviews,
  SUM(total_time_on_page) AS total_time_on_page,
  SUM(total_events) AS total_events,
  SUM(unique_events) AS unique_events,
  -- Single Page sessions used to calculate bounce rate
  SUM(single_page_sessions) AS single_page_sessions,
  SUM(bounces) AS bounces
FROM
  joined_table
GROUP BY
  date,
  page,
  locale,
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
  browser_version,
  country,
  source,
  medium,
  campaign,
  ad_content
