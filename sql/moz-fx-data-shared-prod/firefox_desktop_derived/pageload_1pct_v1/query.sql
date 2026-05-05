SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop.pageload`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND MOD(ABS(FARM_FINGERPRINT(document_id)), 100) = 0
