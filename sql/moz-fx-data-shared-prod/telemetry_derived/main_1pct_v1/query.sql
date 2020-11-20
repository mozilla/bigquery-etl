SELECT
  *
FROM
  telemetry_stable.main_v4
WHERE
  DATE(submission_timestamp) = @submission_date
