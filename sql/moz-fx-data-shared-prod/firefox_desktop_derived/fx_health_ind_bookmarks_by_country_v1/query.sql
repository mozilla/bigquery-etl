SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_country_code,
  SUM(metrics.counter.browser_engagement_bookmarks_toolbar_bookmark_opened) / COUNT(
    DISTINCT client_info.client_id
  ) AS bookmarks_added_per_dau,
  SUM(metrics.counter.browser_engagement_bookmarks_toolbar_bookmark_opened) / COUNT(
    DISTINCT client_info.client_id
  ) AS bookmarks_opened_per_dau
FROM
  `moz-fx-data-shared-prod.firefox_desktop.metrics`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_channel = 'release'
  AND app_version_major >= 84
GROUP BY
  submission_date,
  normalized_country_code
