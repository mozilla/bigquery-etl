CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.main_nightly_v1`
PARTITION BY
  DATE(submission_timestamp)
CLUSTER BY
  normalized_channel,
  sample_id
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 180)
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    `moz-fx-data-shared-prod.udf.normalize_main_payload`(payload) AS payload
  )
FROM
  telemetry_stable.main_v5
WHERE
  FALSE
  AND normalized_channel = 'nightly'
