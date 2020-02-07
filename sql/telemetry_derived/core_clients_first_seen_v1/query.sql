WITH clients_today AS (
  SELECT
    DISTINCT client_id
  FROM
    telemetry.core
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  DISTINCT client_id,
  DATE @submission_date AS first_submission_date
FROM
  clients_today
LEFT JOIN
  telemetry_derived.core_clients_first_seen_v1 AS cfs
USING
  (client_id)
WHERE
  cfs.first_seen_date < @submission_date
  AND cfs.client_id IS NULL
