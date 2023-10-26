SELECT
  -- We apply field cleaning at the table level rather than the view level because
  -- the logic here ends up becoming the bottleneck for simple queries on top of
  -- this table. The limited size and retention policy on this table makes it feasible
  -- to perform full backfills as needed if this logic changes.
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    `moz-fx-data-shared-prod.udf.normalize_main_payload`(payload) AS payload
  )
FROM
  telemetry_stable.main_v5
WHERE
  normalized_channel = 'nightly'
  AND DATE(submission_timestamp) = @submission_date
