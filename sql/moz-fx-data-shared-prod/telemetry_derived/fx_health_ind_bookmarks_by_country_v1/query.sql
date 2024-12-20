SELECT
  DATE(submission_timestamp) AS submission_date,
  normalized_country_code,
  SUM(payload.processes.parent.scalars.browser_engagement_bookmarks_toolbar_bookmark_added) / COUNT(
    DISTINCT client_id
  ) AS bookmarks_added_per_dau,
  SUM(
    payload.processes.parent.scalars.browser_engagement_bookmarks_toolbar_bookmark_opened
  ) / COUNT(DISTINCT client_id) AS bookmarks_opened_per_dau
FROM
  `moz-fx-data-shared-prod.telemetry.main_1pct`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND SUBSTR(application.version, 0, 2) >= '84'
  AND normalized_channel = 'release'
GROUP BY
  submission_date,
  normalized_country_code
