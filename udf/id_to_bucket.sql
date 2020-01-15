/*

From an identifier and a number of buckets, deterministically
returns an integer between 0 and (n_buckets - 1).

See https://github.com/mozilla/bigquery-etl/issues/36

*/
CREATE TEMP FUNCTION udf_id_to_bucket(identifier STRING, n_buckets INT64)
RETURNS INT64 AS (
  MOD(ABS(FARM_FINGERPRINT(identifier)), n_buckets)
);
-- Tests.
SELECT
  assert_equals(11, udf_id_to_bucket('foo', 20)),
  assert_equals(1, udf_id_to_bucket('89c0d5aa-2dcb-4fa4-9dd2-7226bdb4e104', 20));
