SELECT
  TIMESTAMP_TRUNC(submission_timestamp, minute) AS submission_minute,
  COUNT(*) AS n,
  COUNT(request_id) AS n_merino,
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  1
