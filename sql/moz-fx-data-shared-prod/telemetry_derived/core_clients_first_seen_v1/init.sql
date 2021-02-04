CREATE TABLE
  `moz-fx-data-shared-prod.telemetry_derived.core_clients_first_seen_v1`
PARTITION BY
  (first_seen_date)
AS
SELECT
  client_id,
  DATE(MIN(submission_timestamp)) AS first_seen_date,
FROM
  telemetry.core
WHERE
  submission_timestamp > '2010-01-01'
GROUP BY
  client_id
