WITH sessions_table AS (
  -- Pull sessions related activity by landing page
  SELECT
    date,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    page_path AS landing_page,
    page_path_level1 AS locale,
    page_level_1,
    page_level_2,
    page_level_3,
    page_level_4,
    page_level_5,
    page_name,
    country,
    source,
    medium,
    campaign,
    ad_content,
    browser,
    SUM(entrances) AS sessions,
    SUM(
      IF(NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(browser), entrances, 0)
    ) AS non_fx_sessions,
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
  GROUP BY
    date,
    device_category,
    operating_system,
    `language`,
    landing_page,
    locale,
    page_level_1,
    page_level_2,
    page_level_3,
    page_level_4,
    page_level_5,
    page_name,
    visit_identifier,
    country,
    source,
    medium,
    campaign,
    ad_content,
    browser
),
pageviews_table AS (
  -- Select pageview metrics by visit identifier
  SELECT
    date,
    visit_identifier,
    COUNT(DISTINCT(page_path)) AS unique_pageviews,
    COUNT(*) AS pageviews,
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND hit_type = 'PAGE'
  GROUP BY
    date,
    visit_identifier
),
bounces_table AS (
  SELECT
    date,
    visit_identifier,
    SUM(IF(hit_number = first_interaction, visits, 0)) AS single_page_sessions,
    SUM(IF(hit_number = first_interaction, bounces, 0)) AS bounces,
    SUM(exits) AS exits,
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
  GROUP BY
    date,
    visit_identifier
),
joined_table AS (
  -- Join tables based off of date and visitIdentifier
  SELECT
    sessions_table.date,
    sessions_table.visit_identifier,
    sessions_table.device_category,
    sessions_table.operating_system,
    sessions_table.language,
    sessions_table.landing_page,
    sessions_table.locale,
    sessions_table.page_level_1,
    sessions_table.page_level_2,
    sessions_table.page_level_3,
    sessions_table.page_level_4,
    sessions_table.page_level_5,
    sessions_table.page_name,
    sessions_table.country,
    sessions_table.source,
    sessions_table.medium,
    sessions_table.campaign,
    sessions_table.ad_content,
    sessions_table.browser,
    sessions_table.sessions,
    sessions_table.non_fx_sessions,
    pageviews_table.pageviews,
    downloads_table.downloads,
    downloads_table.non_fx_downloads,
    pageviews_table.unique_pageviews,
    bounces_table.single_page_sessions,
    bounces_table.bounces,
    bounces_table.exits,
  FROM
    sessions_table
  FULL JOIN
    `moz-fx-data-marketing-prod.ga_derived.www_site_downloads_v1` AS downloads_table
    USING (date, visit_identifier)
  FULL JOIN
    pageviews_table
    USING (date, visit_identifier)
  FULL JOIN
    bounces_table
    USING (date, visit_identifier)
  WHERE
    -- To minimize table size, filtering for sessions != 0
    sessions_table.sessions != 0
)
SELECT
  date,
  -- Adding a site field in case we want to append the blog traffic to this same table for YoY comparability
  'mozilla.org' AS site,
  device_category,
  operating_system,
  `language`,
  landing_page,
  locale,
  page_name,
  page_level_1,
  page_level_2,
  page_level_3,
  page_level_4,
  page_level_5,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser,
  SUM(sessions) AS sessions,
  SUM(non_fx_sessions) AS non_fx_sessions,
  SUM(downloads) AS downloads,
  SUM(non_fx_downloads) AS non_fx_downloads,
  SUM(pageviews) AS pageviews,
  SUM(unique_pageviews) AS unique_pageviews,
  SUM(single_page_sessions) AS single_page_sessions,
  SUM(bounces) AS bounces,
  SUM(exits) AS exits
FROM
  joined_table
GROUP BY
  date,
  site,
  device_category,
  operating_system,
  `language`,
  landing_page,
  locale,
  page_name,
  page_level_1,
  page_level_2,
  page_level_3,
  page_level_4,
  page_level_5,
  country,
  source,
  medium,
  campaign,
  ad_content,
  browser
