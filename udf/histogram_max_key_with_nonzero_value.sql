/*

Find the largest numeric bucket that contains a value greater than zero.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L253-L266

*/
CREATE TEMP FUNCTION udf_histogram_max_key_with_nonzero_value(histogram STRING) AS (
  (SELECT MAX(key) FROM UNNEST(udf_json_extract_histogram(histogram).values) WHERE value > 0)
);

-- Tests
SELECT
  assert_equals(1, udf_histogram_max_key_with_nonzero_value('{"values":{"0":3,"1":2}}')),
  assert_null(udf_histogram_max_key_with_nonzero_value('{}')),
  assert_null(udf_histogram_max_key_with_nonzero_value('{"values":{"0":0}}')),
  assert_equals(5, udf_histogram_max_key_with_nonzero_value('{"values":{"5":1}}'))
