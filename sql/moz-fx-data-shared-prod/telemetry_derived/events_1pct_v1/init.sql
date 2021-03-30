CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.events_1pct_v1`
PARTITION BY
  submission_date
CLUSTER BY
  event_category,
  sample_id
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 180)
AS
SELECT
  *
FROM
  telemetry.events
WHERE
  FALSE
  AND sample_id = 0
