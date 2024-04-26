SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop.pageload`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_channel = "nightly"
