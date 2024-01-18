-- Some pages have a landing page = (not set). This makes it difficult to match the totals for the property to the landing page totals
-- By joining the landing page totals to the summary total, we can isolate the (not set) sessions
WITH landing_page_table AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visit_identifier,
    hits.page.pagePath AS landing_page,
    SPLIT(hits.page.pagePath, '?')[OFFSET(0)] AS cleaned_landing_page,
    SUM(IF(hits.isEntrance IS NOT NULL, 1, 0)) AS page_sessions,
  FROM
    `moz-fx-data-marketing-prod.66602784.ga_sessions_*`
  CROSS JOIN
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    date,
    visit_identifier,
    landing_page,
    cleaned_landing_page
  HAVING
    page_sessions != 0
)
SELECT
  date,
  device_category,
  operating_system,
  browser,
  `language`,
  country,
  standardized_country_list.standardized_country AS standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landing_page,
  cleaned_landing_page,
  SUM(sessions) AS sessions,
  SUM(downloads) AS downloads,
  SUM(social_share) AS social_share,
  SUM(newsletter_subscription) AS newsletter_subscription,
FROM
  `moz-fx-data-marketing-prod.ga_derived.blogs_sessions_v1` AS sessions_table
LEFT JOIN
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v1` AS goals_table
  USING (date, visit_identifier)
LEFT JOIN
  landing_page_table
  USING (date, visit_identifier)
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS standardized_country_list
  ON sessions_table.country = standardized_country_list.raw_country
WHERE
  date = @submission_date
GROUP BY
  date,
  device_category,
  operating_system,
  browser,
  `language`,
  country,
  standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landing_page,
  cleaned_landing_page
