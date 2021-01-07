SELECT
  CAST(date_add(submission_date, INTERVAL 1 day) AS timestamp) AS time,
  experiment_id,
  SUM(active_clients) AS active_clients
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiments_daily_active_clients_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  1,
  2
ORDER BY
  1
