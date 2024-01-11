SELECT
  -- subsample_id can allow you to efficiently filter down to a 0.01% sample.
  -- The choice of implementation here is not particularly well vetted;
  -- it's simply chosen to be a hash that's stable, has a reasonable
  -- avalanche effect, and is _different_ from sample_id. We use this same approach
  -- for choosing id_bucket in exact_mau28 tables.
  MOD(ABS(FARM_FINGERPRINT(client_id)), 100) AS subsample_id,
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
  sample_id = 0
  AND DATE(submission_timestamp) = @submission_date
