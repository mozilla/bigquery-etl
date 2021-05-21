WITH _current AS (
  SELECT DISTINCT
    client_id
  FROM
    telemetry.core
  WHERE
    DATE(submission_timestamp) = @submission_date
),
_previous AS (
  SELECT
    *
  FROM
    telemetry_derived.core_clients_first_seen_v1
  WHERE
    first_seen_date > "2010-01-01"
)
SELECT
  client_id,
  @submission_date AS first_seen_date
FROM
  _current
LEFT JOIN
  _previous
USING
  (client_id)
WHERE
  _previous.client_id IS NULL
