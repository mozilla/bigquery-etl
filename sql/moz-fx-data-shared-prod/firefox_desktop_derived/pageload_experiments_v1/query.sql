SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop.pageload`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND ARRAY_LENGTH(ping_info.experiments) > 0
