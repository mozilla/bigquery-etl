CREATE TABLE IF NOT EXISTS
  clients_daily_event_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  submission_date,
  sample_id,
  client_id,
FROM
  telemetry.events
WHERE
  -- Output empty table and read no input rows
  FALSE
