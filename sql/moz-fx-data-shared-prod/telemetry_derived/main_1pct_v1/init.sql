CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.main_1pct_v1`
PARTITION BY
  DATE(submission_timestamp)
CLUSTER BY
  normalized_channel,
  sample_id,
  subsample_id
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 180)
AS
SELECT
  CAST(NULL AS INT64) AS subsample_id,
  *
FROM
  telemetry_stable.main_v5
WHERE
  FALSE
  AND sample_id = 0
