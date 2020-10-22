-- Some pages have a landing page = (not set). This makes it difficult to match the totals for the property to the landing page totals
-- By joining the landing page totals to the summary total, we can isolate the (not set) sessions
WITH landing_page_table AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visitIdentifier,
    hits.page.pagePath AS landingPage,
    SPLIT(hits.page.pagePath, '?')[OFFSET(0)] AS cleanedLandingPage,
    SUM(IF(hits.isEntrance IS NOT NULL, 1, 0)) AS page_sessions,
  FROM
    `ga-mozilla-org-prod-001.66602784.ga_sessions_*`
  CROSS JOIN
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    date,
    visitIdentifier,
    landingPage,
    cleanedLandingPage
  HAVING
    page_sessions != 0
)
SELECT
  date,
  deviceCategory AS device_category,
  operatingSystem AS operating_system,
  browser,
  LANGUAGE AS language,
  country,
  standardized_country_list.standardized_country AS standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landingPage AS landing_page,
  cleanedLandingPage AS cleaned_landing_page,
  SUM(sessions) AS sessions,
  SUM(downloads) AS downloads,
  SUM(socialShare) AS socialShare,
  SUM(newsletterSubscription) AS newsletterSubscription,
FROM
  `moz-fx-data-marketing-prod.ga_derived.blogs_sessions_v1` AS sessions_table
LEFT JOIN
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v1`
USING
  (date, visitIdentifier)
LEFT JOIN
  landing_page_table
USING
  (date, visitIdentifier)
LEFT JOIN
  `moz-fx-data-marketing-prod.static.standardized_country_list` AS standardized_country_list
ON
  sessions_table.country = standardized_country_list.raw_country
WHERE
  date = @submission_date
GROUP BY
  date,
  deviceCategory,
  operatingSystem,
  browser,
  LANGUAGE,
  country,
  standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landingPage,
  cleanedLandingPage
