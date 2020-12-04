SELECT
  -- subsample_id is an especially experimental field; this table already represents
  -- a 1% sample, but this field can allow you to efficiently filter down to a 0.01%
  -- sample, which may or may not be useful in practice. We'll need to get user
  -- feedback about whether to keep this. The choice of implementation here is not
  -- yet vetted; it's simply chosen to be a hash that's stable, has a reasonable
  -- avalanche effect, and is _different_ from sample_id. We use this same approach
  -- for choosing id_bucket in exact_mau28 tables.
  MOD(ABS(FARM_FINGERPRINT(client_id)), 100) AS subsample_id,
  *
FROM
  telemetry_stable.main_v4
WHERE
  sample_id = 0
  AND DATE(submission_timestamp) = @submission_date
