/*

Given histogram h, return TRUE if it has a value in the "true" bucket,
or FALSE if it has a value in the "false" bucket, or NULL otherwise.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L309-L317

*/

CREATE TEMP FUNCTION
  udf_boolean_histogram_to_boolean(histogram STRING) AS (
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(histogram,
          "$.values.1") AS INT64) > 0,
      NOT SAFE_CAST( JSON_EXTRACT_SCALAR(histogram,
          "$.values.0") AS INT64) > 0));

-- Tests

SELECT
  assert_equals(TRUE, udf_boolean_histogram_to_boolean('{"values":{"0":1,"1":1}}')),
  assert_null(udf_boolean_histogram_to_boolean('{}')),
  assert_equals(FALSE, udf_boolean_histogram_to_boolean('{"values":{"0":1}}')),
  assert_equals(TRUE, udf_boolean_histogram_to_boolean('{"values":{"1":1}}'))
