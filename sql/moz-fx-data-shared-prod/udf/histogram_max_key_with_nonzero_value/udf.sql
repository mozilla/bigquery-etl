/*

Find the largest numeric bucket that contains a value greater than zero.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L253-L266

*/
CREATE OR REPLACE FUNCTION udf.histogram_max_key_with_nonzero_value(histogram STRING) AS (
  (SELECT MAX(key) FROM UNNEST(mozfun.hist.extract(histogram).values) WHERE value > 0)
);

-- Tests
SELECT
  mozfun.assert.equals(1, udf.histogram_max_key_with_nonzero_value('{"values":{"0":3,"1":2}}')),
  mozfun.assert.null(udf.histogram_max_key_with_nonzero_value('{}')),
  mozfun.assert.null(udf.histogram_max_key_with_nonzero_value('{"values":{"0":0}}')),
  mozfun.assert.equals(5, udf.histogram_max_key_with_nonzero_value('{"values":{"5":1}}'))
