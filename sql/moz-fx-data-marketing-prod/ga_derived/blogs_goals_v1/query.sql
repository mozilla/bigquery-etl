WITH by_browser AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visitIdentifier,
    device.browser,
    SUM(IF(hits.eventInfo.eventAction = "Firefox Download", 1, 0)) AS downloads,
    SUM(IF(hits.eventInfo.eventAction = "share", 1, 0)) AS share,
    SUM(IF(hits.eventInfo.eventAction = "newsletter subscription", 1, 0)) AS newsletterSubscription
  FROM
    `ga-mozilla-org-prod-001.66602784.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    date,
    visitIdentifier,
    browser
)
SELECT
  date,
  visitIdentifier,
  SUM(IF(downloads > 0, 1, 0)) AS downloads,
  SUM(IF(share > 0, 1, 0)) AS socialShare,
  SUM(IF(newsletterSubscription > 0, 1, 0)) AS newsletterSubscription
FROM
  by_browser
GROUP BY
  date,
  visitIdentifier
