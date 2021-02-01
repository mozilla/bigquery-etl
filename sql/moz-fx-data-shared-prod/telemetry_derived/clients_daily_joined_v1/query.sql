SELECT
  *
FROM
  telemetry_derived.clients_daily_v6 AS cd
LEFT JOIN
  telemetry_derived.clients_daily_event_v1 AS cde
USING
  (submission_date, sample_id, client_id)
WHERE
  cd.submission_date = @submission_date
  AND cde.submission_date = @submission_date
