SELECT
  submission_date,
  e.key AS experiments,
  e.value AS branch,
  count(*) AS active_clients
FROM
  telemetry.clients_daily
CROSS JOIN
  UNNEST(experiments) AS e
WHERE
  submission_date = @submission_date
GROUP BY
  1,
  2,
  3
