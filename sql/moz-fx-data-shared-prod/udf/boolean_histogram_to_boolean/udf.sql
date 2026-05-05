/*

Given histogram h, return TRUE if it has a value in the "true" bucket,
or FALSE if it has a value in the "false" bucket, or NULL otherwise.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L309-L317

*/
CREATE OR REPLACE FUNCTION udf.boolean_histogram_to_boolean(histogram STRING) AS (
  COALESCE(
    SAFE_CAST(JSON_EXTRACT_SCALAR(histogram, "$.values.1") AS INT64) > 0,
    NOT SAFE_CAST(JSON_EXTRACT_SCALAR(histogram, "$.values.0") AS INT64) > 0,
    -- If the above don't work, the histogram may be in compact boolean encoding like "0,5"
    SAFE_CAST(SPLIT(histogram, ',')[SAFE_OFFSET(1)] AS INT64) > 0,
    NOT SAFE_CAST(SPLIT(histogram, ',')[SAFE_OFFSET(0)] AS INT64) > 0
  )
);

-- Tests
SELECT
  mozfun.assert.equals(TRUE, udf.boolean_histogram_to_boolean('{"values":{"0":1,"1":1}}')),
  mozfun.assert.null(udf.boolean_histogram_to_boolean('{}')),
  mozfun.assert.equals(FALSE, udf.boolean_histogram_to_boolean('{"values":{"0":1}}')),
  mozfun.assert.equals(TRUE, udf.boolean_histogram_to_boolean('0,1')),
  mozfun.assert.equals(TRUE, udf.boolean_histogram_to_boolean('1,1')),
  mozfun.assert.equals(FALSE, udf.boolean_histogram_to_boolean('1,0'))
