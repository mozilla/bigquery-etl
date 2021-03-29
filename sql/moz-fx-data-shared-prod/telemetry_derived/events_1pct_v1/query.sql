SELECT
  *
FROM
  telemetry.events
WHERE
  sample_id = 0
  AND submission_date = @submission_date
