SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context`
WHERE
  DATE(submission_timestamp)
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND sample_id < 10
  AND client_info.client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_info.client_id ORDER BY submission_timestamp DESC) = 1
