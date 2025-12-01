SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_os,
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
  AND app_version_major >= 84
  AND normalized_channel = 'release'
GROUP BY
  DATE(submission_timestamp),
  normalized_os
