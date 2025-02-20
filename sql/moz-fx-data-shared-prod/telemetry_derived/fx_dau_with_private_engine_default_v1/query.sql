SELECT
  submission_date_s3 AS submission_date,
  default_search_engine,
  COUNT(DISTINCT client_id) AS users
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date = @submission_date
  AND app_name = 'Firefox'
GROUP BY
  submission_date_s3,
  default_search_engine
